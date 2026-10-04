"""Retry and crash recovery must preserve successful or ambiguous delivery."""

from contextlib import nullcontext
from datetime import date

import pytest

from app.services import pre_event_digest as module


DIGEST = {"headline": "H", "stance": "neutral", "confidence": 0.5, "bull_case": [], "bear_case": [],
          "what_to_watch": [], "options_note": "", "suggested_action": "watch", "risks": []}
EVENT = {"user_id": 1, "symbol": "GOOGL", "event_type": "earnings", "event_date": date(2026, 10, 27)}


class Repo:
    def __init__(self, events=None):
        self.events = events if events is not None else [EVENT]
        self.records = {}
        self.notifications = []

    def due_events(self, *_):
        return self.events

    def get(self, item):
        return self.records.get(item["user_id"])

    def prepare(self, item, digest):
        self.records[item["user_id"]] = {"id": item["user_id"], "digest": digest, "channels": {}}
        return self.get(item)

    def begin(self, digest_id, channel):
        if self.records[digest_id]["channels"].get(channel) in ("sent", "sending", "unknown"):
            return False
        self.state(digest_id, channel, "sending")
        return True

    def state(self, digest_id, channel, state):
        self.records[digest_id]["channels"][channel] = state

    def browser(self, digest_id, user_id, symbol, title, text):
        self.notifications.append((user_id, symbol))
        self.state(digest_id, "browser", "sent")


@pytest.fixture(autouse=True)
def batch_lock(monkeypatch):
    monkeypatch.setattr(module, "event_batch_lock", lambda *_: nullcontext(True))


def run(repo, **kwargs):
    return module.run_pre_event_digests(today=date(2026, 10, 25), repo=repo,
        gather=lambda *_args, **_kwargs: {}, build=lambda *_: dict(DIGEST),
        settings_fn=lambda *_: {"default_channels": ["browser"]}, **kwargs)


def test_repeated_batches_deliver_browser_once():
    repo = Repo()
    assert run(repo)["sent"] == 1
    assert run(repo)["sent"] == 0
    assert repo.notifications == [(1, "GOOGL")]


def test_lock_contention_does_not_build(monkeypatch):
    monkeypatch.setattr(module, "event_batch_lock", lambda *_: nullcontext(False))
    repo = Repo()
    assert run(repo)["reason"] == "already_running"
    assert repo.records == {}


def test_digest_reused_across_users_and_persisted_for_retries():
    repo = Repo([EVENT, {**EVENT, "user_id": 2}])
    calls = []
    module.run_pre_event_digests(repo=repo, gather=lambda *_a, **_kw: {},
        build=lambda *_: calls.append(1) or dict(DIGEST), settings_fn=lambda *_: {})
    module.run_pre_event_digests(repo=repo, gather=lambda *_a, **_kw: {},
        build=lambda *_: calls.append(1) or dict(DIGEST), settings_fn=lambda *_: {})
    assert calls == [1]
    assert len(repo.notifications) == 2


def test_unusable_llm_output_is_retried_later():
    repo = Repo()
    def unavailable(_):
        raise module.DigestUnavailable()
    result = module.run_pre_event_digests(repo=repo, gather=lambda *_a, **_kw: {},
        build=unavailable, settings_fn=lambda *_: {})
    assert result["skipped"] == 1 and repo.records == {}
    assert run(repo)["sent"] == 1


def test_unsupported_only_channels_skip_generation():
    repo = Repo()
    def forbidden(_):
        pytest.fail("LLM should not be called")
    result = module.run_pre_event_digests(repo=repo, build=forbidden,
        settings_fn=lambda *_: {"default_channels": ["discord", "webhook"]})
    assert result["skipped"] == 1 and repo.records == {}


@pytest.mark.parametrize("error,expected_calls,expected_state", [
    ("missing_telegram_bot_token", 2, "failed"),
    ("http_429:rate limit", 2, "failed"),
    ("Read timed out", 1, "unknown"),
    ("http_500:upstream error", 1, "unknown"),
])
def test_external_failures_retry_only_when_delivery_is_known(monkeypatch, error, expected_calls, expected_state):
    calls = []
    class Notifier:
        def _notify_telegram(self, **_):
            calls.append(1)
            return False, error
    monkeypatch.setattr(module, "SignalNotifier", Notifier)
    repo = Repo()
    record = repo.prepare(EVENT, DIGEST)
    for _ in range(2):
        module.deliver_digest(1, symbol="GOOGL", title="t", text="b", repo=repo, record=record,
            settings={"default_channels": ["browser", "telegram"]})
    assert calls == [1] * expected_calls
    assert repo.notifications == [(1, "GOOGL")]
    assert record["channels"] == {"browser": "sent", "telegram": expected_state}


def test_interrupted_external_send_is_never_reissued(monkeypatch):
    repo = Repo()
    record = repo.prepare(EVENT, DIGEST)
    record["channels"]["telegram"] = "sending"
    result = module.deliver_digest(1, symbol="GOOGL", title="t", text="b", repo=repo, record=record,
        settings={"default_channels": ["telegram"]})
    assert result["telegram"] == "unknown"


def test_browser_crash_recovers_and_channel_exception_isolated(monkeypatch):
    class Notifier:
        def _notify_telegram(self, **_):
            raise RuntimeError("connection lost")
    monkeypatch.setattr(module, "SignalNotifier", Notifier)
    repo = Repo()
    record = repo.prepare(EVENT, DIGEST)
    record["channels"]["browser"] = "sending"
    result = module.deliver_digest(1, symbol="GOOGL", title="t", text="b", repo=repo, record=record,
        settings={"default_channels": ["telegram", "browser"]})
    assert result == {"browser": "sent", "telegram": "unknown"}


def test_telegram_disclaimer_survives_length_limit(monkeypatch):
    messages = []
    class Notifier:
        def _notify_telegram(self, **kwargs):
            messages.append(kwargs["text"])
            return True, ""
    monkeypatch.setattr(module, "SignalNotifier", Notifier)
    repo = Repo()
    module.deliver_digest(1, symbol="GOOGL", title="t", text="x" * 5000, repo=repo,
        record=repo.prepare(EVENT, DIGEST), settings={"default_channels": ["telegram"]})
    assert len(messages[0]) <= 3900 and messages[0].endswith(module.DISCLAIMER)


def test_global_disable_does_not_enter_service(monkeypatch):
    from app.tasks.event_digest import run_pre_event_digest
    monkeypatch.delenv("ENABLE_PRE_EVENT_DIGEST", raising=False)
    monkeypatch.setattr(module, "run_pre_event_digests", lambda **_: pytest.fail("Disabled task ran"))
    assert run_pre_event_digest.run() == {"skipped": True}


def test_celery_queue_and_schedule_boundaries():
    from app.celery_app import celery_app
    assert celery_app.conf.task_routes["quantdinger.tasks.pre_event_digest"]["queue"] == "ai"
    assert celery_app.conf.task_routes["quantdinger.tasks.earnings_calendar_sync"]["queue"] == "maintenance"
    assert celery_app.conf.beat_schedule["pre-event-digest"]["schedule"] == 3600
    assert celery_app.conf.beat_schedule["earnings-calendar-sync"]["schedule"] == 86400
