"""Bounded public-document reader for research, with pinned public DNS targets."""
from __future__ import annotations

import ipaddress
import json
import math
import os
import socket
import re
import subprocess
import sys
from contextlib import redirect_stdout
from pathlib import Path
from time import monotonic
from urllib.parse import urljoin, urlsplit

import certifi
import urllib3
from bs4 import BeautifulSoup

DEFAULT_USER_AGENT = "QuantDinger Research support@quantdinger.com"


def public_target(url: str) -> tuple[str, str]:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Only public HTTPS documents are permitted")
    if parsed.port not in (None, 443):
        raise ValueError("Nonstandard document port")
    host = parsed.hostname.encode("idna").decode("ascii")
    addresses = {item[4][0] for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise ValueError("Document host must resolve only to public addresses")
    return host, sorted(addresses)[0]


def document_excerpt(text: str, query: str, limit: int = 14000) -> str:
    if len(text) <= limit or not query:
        return text[:limit]
    terms = list(dict.fromkeys(re.findall(r"[a-zA-Z]{3,}", query.lower())))[:12]
    chunks = [(index, text[index:index + 1800]) for index in range(0, len(text), 1500)]
    ranked = sorted(chunks, key=lambda item: sum(len(re.findall(r"\b" + re.escape(term) + r"\b", item[1], re.I)) for term in terms), reverse=True)
    chosen = sorted(ranked[:7])
    return "\n[... excerpt ...]\n".join(chunk for _, chunk in chosen)[:limit]


def read_public_document(url: str, *, timeout: float = 8.0, max_bytes: int = 5_000_000,
                         query: str = "", user_agent: str = DEFAULT_USER_AGENT,
                         hard_deadline: bool = False) -> dict:
    """Read public text; optionally isolate DNS, decoding and parsing in a timed child.

    A subprocess, rather than multiprocessing, also works in Celery's daemon
    prefork children. Existing callers retain the in-process implementation.
    """
    if not math.isfinite(timeout) or timeout <= 0 or max_bytes <= 0:
        raise ValueError("Invalid document limits")
    if not user_agent or len(user_agent) > 512 or any(ord(char) < 32 or ord(char) == 127 for char in user_agent):
        raise ValueError("Invalid document user agent")
    if hard_deadline:
        if len(url) > 8192 or len(query) > 8192 or max_bytes > 5_000_000:
            raise ValueError("Document request exceeds limits")
        return _read_with_deadline({"url": url, "timeout": timeout, "max_bytes": max_bytes,
                                    "query": query, "user_agent": user_agent}, timeout)
    return _read_document(url, timeout=timeout, max_bytes=max_bytes, query=query, user_agent=user_agent)


def _read_with_deadline(request: dict, timeout: float) -> dict:
    deadline = monotonic() + timeout
    child = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--bounded-document-child"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        text=True, encoding="utf-8", start_new_session=True,
    )
    try:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("Document deadline exceeded")
        output, _ = child.communicate(json.dumps(request), timeout=remaining)
        if child.returncode != 0 or len(output) > 262144:
            raise ValueError("Document reader failed")
        try:
            result = json.loads(output)
        except (ValueError, TypeError):
            raise ValueError("Document reader failed") from None
        if result.get("error") == "timeout":
            raise TimeoutError("Document deadline exceeded")
        if "error" in result:
            raise ValueError(result.get("message", "Document reader failed"))
        return result["document"]
    except subprocess.TimeoutExpired:
        raise TimeoutError("Document deadline exceeded") from None
    finally:
        # Reap on success, cancellation and timeout; no blocked DNS/body thread
        # remains in the worker after this operation has finished.
        if child.poll() is None:
            child.kill()
        child.wait()
        for stream in (child.stdin, child.stdout):
            if stream is not None:
                stream.close()


def _document_child() -> None:
    """Private JSON protocol: never forward library stdout or exception secrets."""
    try:
        request = json.loads(sys.stdin.read(32768))
        with open(os.devnull, "w", encoding="utf-8") as sink, redirect_stdout(sink):
            document = read_public_document(**request)
        result = {"document": document}
    except TimeoutError:
        result = {"error": "timeout"}
    except ValueError as exc:
        # Only fixed reader errors can cross the subprocess boundary. DNS,
        # TLS and parser exceptions may contain URLs or credential material.
        message = str(exc)
        permitted = ("Only public HTTPS documents are permitted", "Nonstandard document port",
                     "Document host must resolve only to public addresses", "Unsupported document content type",
                     "Document exceeds size limit", "Document redirect limit exceeded")
        result = {"error": "invalid", "message": message if message in permitted or re.fullmatch(r"Document HTTP \d{3}", message) else "Document reader failed"}
    except Exception:
        result = {"error": "failed", "message": "Document reader failed"}
    encoded = json.dumps(result)
    if len(encoded) > 262144:
        encoded = json.dumps({"error": "invalid", "message": "Document response exceeds limits"})
    sys.stdout.write(encoded)


def _read_document(url: str, *, timeout: float, max_bytes: int, query: str, user_agent: str) -> dict:
    deadline = monotonic() + timeout
    for _ in range(3):
        host, address = public_target(url)
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("Document deadline exceeded")
        parsed = urlsplit(url)
        pool = urllib3.HTTPSConnectionPool(
            address, port=443, server_hostname=host, assert_hostname=host,
            cert_reqs="CERT_REQUIRED", ca_certs=certifi.where(),
            timeout=urllib3.Timeout(connect=min(3, remaining), read=remaining),
        )
        response = None
        try:
            path = parsed.path or "/"
            if parsed.query:
                path += "?" + parsed.query
            response = pool.request("GET", path, headers={"Host": host,
                "User-Agent": user_agent},
                redirect=False, retries=False, preload_content=False)
            if response.status in {301, 302, 303, 307, 308}:
                url = urljoin(url, response.headers.get("Location", ""))
                continue
            if response.status != 200:
                raise ValueError(f"Document HTTP {response.status}")
            content_type = response.headers.get("Content-Type", "").lower()
            if not any(kind in content_type for kind in ("text/", "json", "xml")):
                raise ValueError("Unsupported document content type")
            chunks = []
            size = 0
            while True:
                if monotonic() >= deadline:
                    raise TimeoutError("Document deadline exceeded")
                chunk = response.read(min(65536, max_bytes + 1 - size), decode_content=True)
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    raise ValueError("Document exceeds size limit")
                chunks.append(chunk)
            body = b"".join(chunks)
            if monotonic() >= deadline:
                raise TimeoutError("Document deadline exceeded")
            soup = BeautifulSoup(body, "html.parser")
            title = soup.title.get_text(" ", strip=True) if soup.title else host
            for node in soup.select('[hidden], [aria-hidden="true"], [style]'):
                if node.attrs is not None and (node.has_attr("hidden") or node.get("aria-hidden") == "true"
                        or re.search(r"display\s*:\s*none", node.get("style", ""), re.I)):
                    node.decompose()
            for node in soup.find_all(lambda tag: tag.name in {"ix:header", "ix:hidden", "xbrli:context", "xbrli:unit"}):
                node.decompose()
            for node in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                node.decompose()
            links = []
            for anchor in soup.select("a[href]"):
                target = urljoin(url, anchor.get("href", ""))
                if target.startswith("https://") and not target.startswith(url + "#"):
                    links.append({"title": anchor.get_text(" ", strip=True)[:100], "url": target})
                if len(links) >= 30:
                    break
            text = soup.get_text(" ", strip=True)
            if monotonic() >= deadline:
                raise TimeoutError("Document deadline exceeded")
            return {"url": url, "title": title[:200], "text": document_excerpt(text, query), "links": links,
                    "truncated": len(text) > 14000, "evidence_kind": "document_excerpt"}
        finally:
            if response is not None:
                response.close()
            pool.close()
    raise ValueError("Document redirect limit exceeded")


if __name__ == "__main__" and sys.argv[1:] == ["--bounded-document-child"]:
    _document_child()
