# app/services/pre_event_digest.py
"""Pre-event digest: evidence gathering, LLM synthesis, delivery and orchestration."""

from __future__ import annotations

import json
import math
from datetime import date
from typing import Any, Callable

from app.data_providers.company_research import lookup_company
from app.data_providers.event_sources import fetch_sec_filings, fetch_yahoo_finance_events
from app.services.llm import LLMService
from app.services.event_digest_repository import DigestRepository
from app.services.signal_notifier import SignalNotifier
from app.services.upcoming_events import event_batch_lock
from app.services.user_preferences import get_notification_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

DISCLAIMER = "Research reference only. Not investment advice."
_STANCES = {"bullish", "bearish", "neutral", "unclear"}
_ACTIONS = {"consider_buying", "consider_trimming", "watch", "no_action"}
SUPPORTED_CHANNELS = {"browser", "telegram", "email"}

_SYSTEM_PROMPT = (
    "You are a cautious equity research assistant. Using ONLY the evidence provided, write a "
    "pre-earnings briefing. Do not invent numbers. If evidence is thin, say so and lower confidence. "
    "Reply with one JSON object with keys: headline (string), stance (bullish|bearish|neutral|unclear), "
    "confidence (0..1), bull_case (list of short strings), bear_case (list), what_to_watch (list), "
    "options_note (string, empty if no options data), suggested_action "
    "(consider_buying|consider_trimming|watch|no_action), risks (list)."
)


class DigestUnavailable(Exception):
    """The LLM could not produce a usable digest."""


def gather_evidence(
    event: dict[str, Any],
    *,
    news_fn: Callable[..., list] = fetch_yahoo_finance_events,
    filings_fn: Callable[..., list] = fetch_sec_filings,
    lookup_fn: Callable[..., dict] = lookup_company,
    sec_user_agent: str = "",
) -> dict[str, Any]:
    symbol = str(event["symbol"])
    errors: list[str] = []

    def attempt(label: str, call: Callable[[], Any], default: Any) -> Any:
        try:
            return call()
        except Exception as exc:
            errors.append(f"{label}:{str(exc)[:120]}")
            return default

    news = attempt("news", lambda: news_fn(symbol, days=7, limit=8), [])
    filings: list = []
    if sec_user_agent:
        filings = attempt(
            "filings", lambda: filings_fn(symbol, days=14, limit=5, user_agent=sec_user_agent), []
        )
    options = attempt("options", lambda: lookup_fn(symbol, "options"), {})
    analyst = attempt("analyst", lambda: lookup_fn(symbol, "analyst_expectations"), {})
    for label, result in (("options", options), ("analyst", analyst)):
        if isinstance(result, dict) and not result.get("data"):
            attempts = (result.get("provider_status") or {}).get("attempts") or []
            if attempts:
                errors.append(f"{label}:provider_unavailable")
    if not sec_user_agent:
        errors.append("filings:missing_user_agent")
    return {
        "symbol": symbol,
        "event_date": event["event_date"].isoformat(),
        "eps_estimate": event.get("eps_estimate"),
        "revenue_estimate": event.get("revenue_estimate"),
        "news": news,
        "filings": filings,
        "options": options,
        "analyst": analyst,
        "errors": errors,
    }


def _string_list(value: Any, limit: int = 5) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(v).strip()[:300] for v in value if str(v).strip()][:limit]


def _prompt_evidence(evidence: dict) -> dict:
    """Bound each source before encoding so later domains and errors survive."""
    def compact(value, text_limit, item_limit, depth=0):
        if depth > 6:
            return str(value)[:text_limit]
        if isinstance(value, dict):
            return {str(k)[:80]: compact(v, text_limit, item_limit, depth + 1)
                    for k, v in list(value.items())[:20]}
        if isinstance(value, list):
            return [compact(v, text_limit, item_limit, depth + 1) for v in value[:item_limit]]
        return value[:text_limit] if isinstance(value, str) else value

    result = {key: evidence.get(key) for key in ("symbol", "event_date", "eps_estimate", "revenue_estimate")}
    errors = list(evidence.get("errors") or [])
    for domain in ("news", "filings", "options", "analyst"):
        value = evidence.get(domain)
        for text_limit, item_limit in ((400, 8), (160, 4), (80, 1)):
            bounded = compact(value, text_limit, item_limit)
            if len(json.dumps(bounded, default=str)) <= 2300:
                break
        else:
            bounded = {"unavailable": "evidence_exceeds_prompt_limit"}
            errors.append(f"{domain}:evidence_exceeds_prompt_limit")
        if bounded != value:
            errors.append(f"{domain}:excerpted")
        result[domain] = bounded
    result["errors"] = [str(error)[:160] for error in errors[:12]]
    return result


def build_digest(evidence: dict[str, Any], *, llm: Any = None) -> dict[str, Any]:
    llm = llm or LLMService()
    if not llm.is_configured():
        raise DigestUnavailable("llm_not_configured")
    user_prompt = (
        f"Company: {evidence['symbol']}. Earnings date: {evidence['event_date']}.\n"
        f"Evidence JSON:\n{json.dumps(_prompt_evidence(evidence), default=str)}"
    )
    try:
        raw = llm.safe_call_llm(_SYSTEM_PROMPT, user_prompt, {})
    except Exception as exc:
        raise DigestUnavailable("llm_failed") from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("headline"), str):
        raise DigestUnavailable("llm_output_unusable")
    headline = raw["headline"].strip()
    if not headline:
        raise DigestUnavailable("llm_output_unusable")
    stance = str(raw.get("stance") or "").strip().lower()
    action = str(raw.get("suggested_action") or "").strip().lower()
    try:
        confidence = float(raw.get("confidence"))
        confidence = min(1.0, max(0.0, confidence)) if math.isfinite(confidence) else 0.0
    except (TypeError, ValueError):
        confidence = 0.0
    return {
        "headline": headline[:300],
        "stance": stance if stance in _STANCES else "unclear",
        "confidence": confidence,
        "bull_case": _string_list(raw.get("bull_case")),
        "bear_case": _string_list(raw.get("bear_case")),
        "what_to_watch": _string_list(raw.get("what_to_watch")),
        "options_note": str(raw.get("options_note") or "").strip()[:500],
        "suggested_action": action if action in _ACTIONS else "watch",
        "risks": _string_list(raw.get("risks")),
        "evidence": evidence,
    }


def format_digest(symbol: str, event_date: str, digest: dict[str, Any]) -> tuple[str, str]:
    title = f"{symbol} earnings {event_date}: {digest['headline']}"
    lines = [
        digest["headline"],
        f"Outlook: {digest['stance']} (confidence {digest['confidence']:.0%}). "
        f"Suggested action: {digest['suggested_action'].replace('_', ' ')}.",
    ]
    for label, key in (
        ("Bull case", "bull_case"),
        ("Bear case", "bear_case"),
        ("Watch", "what_to_watch"),
        ("Risks", "risks"),
    ):
        if digest.get(key):
            lines.append(f"{label}:")
            lines.extend(f"- {item}" for item in digest[key])
    if digest.get("options_note"):
        lines.append(f"Options: {digest['options_note']}")
    lines.append("")
    lines.append(DISCLAIMER)
    return title, "\n".join(lines)


def _delivery_state(ok, error):
    if ok:
        return "sent"
    # The existing notifier does not expose exception types. Only an explicit
    # refusal/configuration failure proves non-delivery; other failures are ambiguous.
    error = str(error or "")
    if error.startswith("missing_") or error.startswith("http_4"):
        return "failed"
    return "unknown"


def deliver_digest(user_id: int, *, symbol: str, title: str, text: str,
                   repo, record: dict, settings: dict) -> dict[str, str]:
    channels = [c for c in settings.get("default_channels", ["browser"]) if c in SUPPORTED_CHANNELS]
    results = dict(record["channels"])
    notifier = SignalNotifier()
    for channel in channels:
        state = results.get(channel)
        if state == "sending":
            # A worker died or lost the result. Browser work is transactional;
            # external providers cannot safely be called again.
            state = "failed" if channel == "browser" else "unknown"
            repo.state(record["id"], channel, state)
            results[channel] = state
        if state in ("sent", "unknown") or not repo.begin(record["id"], channel):
            continue
        try:
            if channel == "browser":
                repo.browser(record["id"], user_id, symbol, title, text)
                results[channel] = "sent"
                continue
            if channel == "telegram":
                # SignalNotifier caps Telegram text. Keep the mandatory disclaimer
                # inside that cap, even for a long generated digest.
                message = f"{title}\n\n{text}"
                if len(message) > 3900:
                    message = message[:3900 - len(DISCLAIMER) - 2] + "\n\n" + DISCLAIMER
                ok, error = notifier._notify_telegram(
                    chat_id=str(settings.get("telegram_chat_id") or ""), text=message,
                    token_override=str(settings.get("telegram_bot_token") or ""),
                )
            else:
                ok, error = notifier._notify_email(
                    to_email=str(settings.get("email") or ""), subject=title, body_text=text,
                )
            results[channel] = _delivery_state(ok, error)
        except Exception:
            results[channel] = "failed" if channel == "browser" else "unknown"
        repo.state(record["id"], channel, results[channel])
    return results


def run_pre_event_digests(*, today: date | None = None, lead_days: int = 3, repo=None,
                          gather=gather_evidence, build=build_digest, deliver=deliver_digest,
                          settings_fn=get_notification_settings, sec_user_agent: str = "") -> dict:
    today = today or date.today()
    repo = repo or DigestRepository()
    with event_batch_lock(2026100102) as acquired:
        if not acquired:
            return {"skipped": True, "reason": "already_running"}
        due = repo.due_events(today, lead_days)
        summary = {"due": len(due), "sent": 0, "skipped": 0, "failed": 0}
        cache = {}
        for item in due:
            try:
                settings = settings_fn(item["user_id"]) or {}
                channels = settings.get("default_channels") or ["browser"]
                if not any(channel in SUPPORTED_CHANNELS for channel in channels):
                    summary["skipped"] += 1
                    continue
                record = repo.get(item)
                if record is None:
                    key = (item["symbol"], item["event_date"])
                    if key not in cache:
                        try:
                            cache[key] = build(gather(item, sec_user_agent=sec_user_agent))
                        except DigestUnavailable:
                            cache[key] = None
                    if cache[key] is None:
                        summary["skipped"] += 1
                        continue
                    record = repo.prepare(item, cache[key])
                title, text = format_digest(item["symbol"], item["event_date"].isoformat(), record["digest"])
                before = dict(record["channels"])
                result = deliver(item["user_id"], symbol=item["symbol"], title=title, text=text,
                                 repo=repo, record=record, settings=settings)
                if any(v == "sent" and before.get(c) != "sent" for c, v in result.items()):
                    summary["sent"] += 1
                elif any(v == "failed" for v in result.values()):
                    summary["failed"] += 1
                else:
                    summary["skipped"] += 1
            except Exception:
                summary["failed"] += 1
                logger.warning("pre-event digest failed for user=%s symbol=%s",
                               item.get("user_id"), item.get("symbol"), exc_info=True)
        return summary
