# tests/test_pre_event_digest.py
from datetime import date
import json

import pytest

from app.services import pre_event_digest as module

EVENT = {
    "symbol": "GOOGL", "event_type": "earnings", "event_date": date(2026, 10, 27),
    "eps_estimate": 2.3, "revenue_estimate": 9.4e10,
}


def test_gather_evidence_collects_all_sources():
    ev = module.gather_evidence(
        EVENT,
        news_fn=lambda t, **kw: [{"title": "Google cloud grows"}],
        filings_fn=lambda t, **kw: [{"title": "8-K"}],
        lookup_fn=lambda s, d, **kw: {"domain": d},
        sec_user_agent="QuantDinger test@example.com",
    )
    assert ev["symbol"] == "GOOGL"
    assert ev["event_date"] == "2026-10-27"
    assert ev["news"] == [{"title": "Google cloud grows"}]
    assert ev["filings"] == [{"title": "8-K"}]
    assert ev["options"] == {"domain": "options"}
    assert ev["errors"] == []


def test_gather_evidence_records_source_failures_without_raising():
    def boom(*a, **kw):
        raise RuntimeError("down")

    ev = module.gather_evidence(EVENT, news_fn=boom, filings_fn=boom, lookup_fn=boom, sec_user_agent="x")
    assert ev["news"] == [] and ev["filings"] == []
    assert ev["options"] == {} and ev["analyst"] == {}
    assert len(ev["errors"]) == 4


def test_gather_evidence_skips_sec_without_user_agent():
    called = []
    ev = module.gather_evidence(
        EVENT, news_fn=lambda t, **kw: [], filings_fn=lambda *a, **kw: called.append(1) or [],
        lookup_fn=lambda *a, **kw: {}, sec_user_agent="",
    )
    assert called == []
    assert ev["filings"] == []


class FakeLLM:
    def __init__(self, reply=None, configured=True):
        self.reply = reply
        self.configured = configured
        self.prompts = []

    def is_configured(self):
        return self.configured

    def safe_call_llm(self, system_prompt, user_prompt, default_structure, **kw):
        self.prompts.append((system_prompt, user_prompt))
        return self.reply if self.reply is not None else default_structure


EVIDENCE = {
    "symbol": "GOOGL", "event_date": "2026-10-27", "eps_estimate": 2.3, "revenue_estimate": 9.4e10,
    "news": [], "filings": [], "options": {}, "analyst": {}, "errors": [],
}


def test_build_digest_returns_normalized_fields():
    llm = FakeLLM(reply={
        "headline": "Cloud growth is the swing factor", "stance": "BULLISH", "confidence": 1.7,
        "bull_case": ["cloud"], "bear_case": ["capex"], "what_to_watch": ["margins"],
        "options_note": "", "suggested_action": "consider_buying", "risks": ["macro"],
    })
    digest = module.build_digest(EVIDENCE, llm=llm)
    assert digest["headline"] == "Cloud growth is the swing factor"
    assert digest["stance"] == "bullish"
    assert digest["confidence"] == 1.0
    assert digest["suggested_action"] == "consider_buying"
    assert "GOOGL" in llm.prompts[0][1]


def test_build_digest_coerces_unknown_enums():
    llm = FakeLLM(reply={"headline": "x", "stance": "moon", "suggested_action": "YOLO"})
    digest = module.build_digest(EVIDENCE, llm=llm)
    assert digest["stance"] == "unclear"
    assert digest["suggested_action"] == "watch"
    assert digest["bull_case"] == []


@pytest.mark.parametrize("reply", [None, {"report": "parse failed"}, {"headline": "  "}])
def test_build_digest_raises_when_llm_output_unusable(reply):
    with pytest.raises(module.DigestUnavailable):
        module.build_digest(EVIDENCE, llm=FakeLLM(reply=reply))


def test_build_digest_raises_when_llm_not_configured():
    with pytest.raises(module.DigestUnavailable):
        module.build_digest(EVIDENCE, llm=FakeLLM(configured=False))


def test_format_digest_contains_disclaimer_and_sections():
    title, text = module.format_digest("GOOGL", "2026-10-27", {
        "headline": "H", "stance": "bullish", "confidence": 0.6, "bull_case": ["a"], "bear_case": ["b"],
        "what_to_watch": ["w"], "options_note": "IV elevated", "suggested_action": "watch", "risks": ["r"],
    })
    assert "GOOGL" in title and "2026-10-27" in title
    assert "IV elevated" in text
    assert text.rstrip().endswith("Research reference only. Not investment advice.")


def test_long_news_preserves_valid_json_other_domains_and_source_errors():
    evidence = {**EVIDENCE,
        "news": [{"title": "n" * 300, "summary": "s" * 600, "url": "u" * 700} for _ in range(8)],
        "options": {"data": {"nearest_atm_implied_volatility_pct": 40}},
        "analyst": {"data": {"consensus_target": 200}}, "errors": ["filings:down"]}
    llm = FakeLLM(reply={"headline": "H"})
    module.build_digest(evidence, llm=llm)
    payload = llm.prompts[0][1].split("Evidence JSON:\n", 1)[1]
    prompt = json.loads(payload)
    assert len(payload) < 12000
    assert prompt["news"] and prompt["options"] == evidence["options"]
    assert prompt["analyst"] == evidence["analyst"]
    assert "filings:down" in prompt["errors"]


@pytest.mark.parametrize("reply", ["invalid JSON", [], {"headline": 42}, {"headline": "H", "confidence": float("nan")}])
def test_invalid_llm_types_and_nonfinite_confidence(reply):
    if isinstance(reply, dict) and reply.get("headline") == "H":
        assert module.build_digest(EVIDENCE, llm=FakeLLM(reply=reply))["confidence"] == 0.0
    else:
        with pytest.raises(module.DigestUnavailable):
            module.build_digest(EVIDENCE, llm=FakeLLM(reply=reply))
