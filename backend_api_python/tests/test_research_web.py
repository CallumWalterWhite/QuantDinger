"""Public-document safety and hard deadline tests; no external requests."""
import io
import multiprocessing
import os
import subprocess
import sys
import time

import pytest

from app.services import research_web as web


class Response:
    def __init__(self, body=b"<title>Report</title><p>Revenue rose.</p>", *, status=200, headers=None):
        self.status = status
        self.headers = headers or {"Content-Type": "text/html"}
        self.body = io.BytesIO(body)
        self.closed = False
        self.reads = []

    def read(self, amount, decode_content=False):
        self.reads.append((amount, decode_content))
        return self.body.read(amount)

    def close(self):
        self.closed = True


def fixture_pools(monkeypatch, responses):
    pools = []

    class Pool:
        def __init__(self, address, **kwargs):
            self.address, self.kwargs, self.closed = address, kwargs, False
            pools.append(self)

        def request(self, method, path, **kwargs):
            self.request_args = method, path, kwargs
            return responses.pop(0)

        def close(self):
            self.closed = True

    monkeypatch.setattr(web.urllib3, "HTTPSConnectionPool", Pool)
    monkeypatch.setattr(web.socket, "getaddrinfo", lambda *a, **k: [(None, None, None, None, ("93.184.216.34", 443))])
    return pools


def test_default_contract_pins_dns_and_cleans_hidden_text(monkeypatch):
    response = Response(b'<title>Report</title><script>secret</script><i hidden>hidden</i>'
                        b'<ix:hidden>hidden XBRL</ix:hidden><p>Revenue rose.</p>'
                        b'<a href="/accounts">Accounts</a>')
    pools = fixture_pools(monkeypatch, [response])
    result = web.read_public_document("https://example.com/report?a=1")
    assert result == {"url": "https://example.com/report?a=1", "title": "Report",
                      "text": "Report Revenue rose. Accounts", "links": [{"title": "Accounts", "url": "https://example.com/accounts"}],
                      "truncated": False, "evidence_kind": "document_excerpt"}
    assert pools[0].address == "93.184.216.34"
    assert pools[0].kwargs["assert_hostname"] == "example.com"
    assert pools[0].kwargs["server_hostname"] == "example.com"
    assert pools[0].request_args[1] == "/report?a=1"
    assert pools[0].request_args[2]["headers"]["User-Agent"] == web.DEFAULT_USER_AGENT
    assert pools[0].request_args[2]["redirect"] is False
    assert response.closed and pools[0].closed


def test_owner_user_agent_and_incremental_decoded_size_limit(monkeypatch):
    response = Response(b"a" * 200000)
    pools = fixture_pools(monkeypatch, [response])
    with pytest.raises(ValueError, match="size limit"):
        web.read_public_document("https://example.com/report", max_bytes=100000, user_agent="Owner owner@example.com")
    assert pools[0].request_args[2]["headers"]["User-Agent"] == "Owner owner@example.com"
    assert len(response.reads) == 2
    assert all(amount <= 65536 and decoded for amount, decoded in response.reads)
    assert response.closed and pools[0].closed


@pytest.mark.parametrize("url", ["http://example.com", "https://user:password@example.com", "https://example.com:8443"])
def test_invalid_urls_do_not_resolve(monkeypatch, url):
    monkeypatch.setattr(web.socket, "getaddrinfo", lambda *a, **k: pytest.fail("Must not resolve"))
    with pytest.raises(ValueError):
        web.read_public_document(url)


@pytest.mark.parametrize("address", ["127.0.0.1", "192.168.1.250", "::1", "169.254.169.254"])
def test_private_or_mixed_dns_rejected(monkeypatch, address):
    monkeypatch.setattr(web.socket, "getaddrinfo", lambda *a, **k: [(None, None, None, None, (ip, 443)) for ip in ["93.184.216.34", address]])
    with pytest.raises(ValueError, match="public addresses"):
        web.read_public_document("https://example.com")


def test_redirect_target_is_revalidated(monkeypatch):
    response = Response(status=302, headers={"Location": "https://private.example/report"})
    pools = fixture_pools(monkeypatch, [response])
    monkeypatch.setattr(web.socket, "getaddrinfo", lambda host, *a, **k: [(None, None, None, None, (("127.0.0.1" if host == "private.example" else "93.184.216.34"), 443))])
    with pytest.raises(ValueError, match="public addresses"):
        web.read_public_document("https://example.com/report")
    assert len(pools) == 1 and response.closed and pools[0].closed


@pytest.mark.parametrize("value", ["", "Owner\r\nAuthorization: secret", "Owner\x00secret", "x" * 513])
def test_user_agent_header_injection_rejected(value):
    with pytest.raises(ValueError, match="user agent"):
        web.read_public_document("https://example.com", user_agent=value)


def subprocess_fixture(monkeypatch, setup):
    """Run the real child protocol with isolated fake DNS/body, never a network."""
    original = subprocess.Popen
    children = []
    script = (
        "import importlib.util, socket, time, io\n"
        f"spec = importlib.util.spec_from_file_location('reader', {web.__file__!r})\n"
        "reader = importlib.util.module_from_spec(spec); spec.loader.exec_module(reader)\n"
        + setup + "\nreader._document_child()\n"
    )

    def launch(command, **kwargs):
        assert command[-1] == "--bounded-document-child"
        child = original([sys.executable, "-c", script], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(web.subprocess, "Popen", launch)
    return children


@pytest.mark.parametrize("phase", ["dns", "body", "parse"])
def test_hard_deadline_kills_and_reaps_hung_child(monkeypatch, tmp_path, phase):
    marker = tmp_path / "entered-phase"
    setup = "reader.socket.getaddrinfo = lambda *a, **k: time.sleep(60)"
    if phase != "dns":
        setup = """
reader.socket.getaddrinfo = lambda *a, **k: [(None, None, None, None, ('93.184.216.34', 443))]
class Response:
    status = 200
    headers = {'Content-Type': 'text/html'}
    body = io.BytesIO(b'<p>Report</p>')
    def read(self, *args, **kwargs):
        BODY
    def close(self): pass
class Pool:
    def __init__(self, *args, **kwargs): pass
    def request(self, *args, **kwargs): return Response()
    def close(self): pass
reader.urllib3.HTTPSConnectionPool = Pool
""".replace("BODY", "time.sleep(60)" if phase == "body" else "return self.body.read(*args[:1])")
        if phase == "parse":
            setup += "\nreader.BeautifulSoup = lambda *a, **k: time.sleep(60)\n"
    setup = ("from pathlib import Path\n"
             "def hang():\n"
             f"    Path({str(marker)!r}).write_text({phase!r})\n"
             "    time.sleep(60)\n" + setup.replace("time.sleep(60)", "hang()"))
    children = subprocess_fixture(monkeypatch, setup)
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="deadline exceeded"):
        web.read_public_document("https://example.com", hard_deadline=True, timeout=0.7)
    assert time.monotonic() - started < 2.5
    assert marker.read_text() == phase
    assert len(children) == 1 and children[0].returncode is not None
    with pytest.raises(ChildProcessError):
        os.waitpid(children[0].pid, os.WNOHANG)
    with pytest.raises(ProcessLookupError):
        os.kill(children[0].pid, 0)


def test_child_json_suppresses_library_stdout_and_exception_details(monkeypatch):
    children = subprocess_fixture(monkeypatch, """
def fail(*args, **kwargs):
    print('private credential must never reach protocol')
    raise RuntimeError('private credential')
reader._read_document = fail
""")
    with pytest.raises(ValueError, match="^Document reader failed$"):
        web.read_public_document("https://example.com", hard_deadline=True, timeout=3)
    assert children[0].returncode == 0


def test_child_success_passes_owner_header_without_multiprocessing(monkeypatch):
    # Celery prefork workers are daemonic: multiprocessing.Process is forbidden,
    # whereas this subprocess boundary must remain usable.
    monkeypatch.setitem(multiprocessing.current_process()._config, "daemon", True)
    children = subprocess_fixture(monkeypatch, """
def result(url, **kwargs):
    assert kwargs['user_agent'] == 'Owner owner@example.com'
    return {'url': url, 'title': 'Report', 'text': 'Revenue rose.', 'links': [], 'truncated': False, 'evidence_kind': 'document_excerpt'}
reader._read_document = result
""")
    result = web.read_public_document("https://example.com", hard_deadline=True, timeout=3, user_agent="Owner owner@example.com")
    assert result["text"] == "Revenue rose."
    assert children[0].returncode == 0


def test_hard_deadline_private_url_rejection_uses_real_protocol():
    # Syntax is rejected before DNS in the unpatched child.
    with pytest.raises(ValueError, match="Only public HTTPS"):
        web.read_public_document("http://user:password@example.com", hard_deadline=True, timeout=3)


def test_redirect_limit_closes_every_response(monkeypatch):
    responses = [Response(status=302, headers={"Location": "/next"}) for _ in range(3)]
    originals = list(responses)
    pools = fixture_pools(monkeypatch, responses)
    with pytest.raises(ValueError, match="redirect limit"):
        web.read_public_document("https://example.com/report")
    assert len(pools) == 3
    assert all(pool.closed for pool in pools)
    assert all(response.closed for response in originals)


def test_pdf_is_explicitly_unsupported(monkeypatch):
    response = Response(b"%PDF", headers={"Content-Type": "application/pdf"})
    fixture_pools(monkeypatch, [response])
    with pytest.raises(ValueError, match="Unsupported document"):
        web.read_public_document("https://example.com/report.pdf")
    assert not response.reads and response.closed


def test_excerpt_and_links_remain_bounded(monkeypatch):
    response = Response(("<p>revenue " + "a" * 30000 + "</p>" + '<a href="/report">Report</a>' * 40).encode())
    fixture_pools(monkeypatch, [response])
    result = web.read_public_document("https://example.com", query="revenue")
    assert len(result["text"]) <= 14000 and result["truncated"]
    assert len(result["links"]) == 30


def test_deadline_kills_and_reaps_child_on_cancellation(monkeypatch):
    class Cancelled(BaseException):
        pass

    class Child:
        stdin = io.StringIO()
        stdout = io.StringIO()
        killed = False
        reaped = False

        def communicate(self, *args, **kwargs):
            raise Cancelled()

        def poll(self):
            return None

        def kill(self):
            self.killed = True

        def wait(self):
            self.reaped = True

    child = Child()
    monkeypatch.setattr(web.subprocess, "Popen", lambda *a, **k: child)
    with pytest.raises(Cancelled):
        web.read_public_document("https://example.com", hard_deadline=True)
    assert child.killed and child.reaped and child.stdin.closed and child.stdout.closed
