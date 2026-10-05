"""Input validation must fail before opening a database connection."""
import pytest
from app.services.earnings_research import repository as repo


@pytest.mark.parametrize('market,days,key', [('CN', 30, 'a'), ('all', True, 'a'), ('US', 91, 'a'), ('UK', 30, '../secret')])
def test_job_validation(market, days, key):
    with pytest.raises(ValueError):
        repo.start_job(1, market, days, key)


def test_disabled_worker_claims_nothing(monkeypatch):
    from app.services.earnings_research import worker
    monkeypatch.delenv('ENABLE_EARNINGS_RESEARCH_EVIDENCE', raising=False)
    monkeypatch.setattr(repo, 'claim_one', lambda: pytest.fail('disabled worker must not claim'))
    assert worker.run_one(fetch=lambda _: pytest.fail('disabled worker must not fetch')) is False


def test_worker_performs_one_source_and_no_model_call(monkeypatch):
    from app.services.earnings_research import worker
    from app.services.llm import LLMService
    monkeypatch.setenv('ENABLE_EARNINGS_RESEARCH_EVIDENCE', 'true')
    monkeypatch.setattr(LLMService, 'call_llm_api', lambda *a, **k: pytest.fail('evidence must not call a model'))
    monkeypatch.setattr(repo, 'claim_one', lambda: {'id': 1, 'kind': 'prices'})
    calls = []
    monkeypatch.setattr(repo, 'publish', lambda item, result: calls.append(('publish', item['id'], result['status'])))
    def fetch(item):
        calls.append(('fetch', item['kind']))
        raise RuntimeError('private provider detail must not be persisted')
    assert worker.run_one(fetch=fetch)
    assert calls == [('fetch', 'prices'), ('publish', 1, 'retry')]


def test_task_limits_and_default_off_without_beat_schedule(monkeypatch):
    from app.tasks.earnings_research import earnings_research_tick as task
    from app.celery_app import celery_app
    from app.services.earnings_research import worker
    monkeypatch.delenv('ENABLE_EARNINGS_RESEARCH_EVIDENCE', raising=False)
    assert task.run() == {'processed': False, 'disabled': True}
    monkeypatch.setenv('ENABLE_EARNINGS_RESEARCH_EVIDENCE', 'true')
    monkeypatch.setattr(worker, 'run_one', lambda: True)
    monkeypatch.setattr(worker, 'pending', lambda: True)
    calls = []
    monkeypatch.setattr(task, 'apply_async', lambda **kwargs: calls.append(kwargs))
    assert task.run() == {'processed': True}
    assert calls == [{'countdown': 2}]
    assert 20 < task.soft_time_limit < task.time_limit < 180
    assert celery_app.conf.task_routes[task.name]['queue'] == 'maintenance'
    assert not any(entry['task'] == task.name for entry in celery_app.conf.beat_schedule.values())
