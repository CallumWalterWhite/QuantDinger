from contextlib import contextmanager
from datetime import date

from app.services import market_earnings as service


@contextmanager
def unlocked(_):
    yield True


def test_market_failure_does_not_publish_or_stop_other_market(monkeypatch):
    calls = []
    monkeypatch.setattr(service, "event_batch_lock", unlocked)
    monkeypatch.setattr(service, "start_run", lambda m, s, e: m)
    monkeypatch.setattr(service, "fail_run", lambda run: calls.append(("failed", run)))
    monkeypatch.setattr(service, "publish_snapshot", lambda run, m, s, e, d, c: calls.append(("published", m)) or {"status": "success"})
    def directory(m):
        if m == "US":
            raise RuntimeError("provider failed")
        return {"items": []}
    result = service.sync_market_earnings(today=date(2026, 10, 5), directory=directory,
                                         calendar=lambda *_: {"items": []})
    assert calls == [("failed", "US"), ("published", "UK")]
    assert result["US"]["status"] == "failed" and result["UK"]["status"] == "success"


def test_batch_lock_prevents_duplicate_provider_work(monkeypatch):
    @contextmanager
    def busy(_):
        yield False
    monkeypatch.setattr(service, "event_batch_lock", busy)
    assert service.sync_market_earnings(directory=lambda *_: (_ for _ in ()).throw(AssertionError())) == {"skipped": True}


def test_task_disabled_by_default(monkeypatch):
    from app.tasks.market_earnings import run_market_earnings_sync
    monkeypatch.delenv("ENABLE_MARKET_EARNINGS_SYNC", raising=False)
    monkeypatch.setattr(service, "sync_market_earnings", lambda: (_ for _ in ()).throw(AssertionError()))
    assert run_market_earnings_sync.run() == {"skipped": True}
