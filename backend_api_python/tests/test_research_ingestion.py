from datetime import date, datetime, timedelta, timezone

from app.services.research_ingestion import observation_coverage, refresh_due


def test_readiness_is_distinct_from_fetch_success_and_frequency():
    assert observation_coverage(None, 'UK')['state'] == 'no_data'
    row = {'period_end': date.today(), 'revenue': 1}
    assert observation_coverage(row, 'UK')['state'] == 'partial'
    assert 'net_income' in observation_coverage(row, 'UK')['missing']
    assert observation_coverage(row, 'UK')['historical_comparability'] == 'unverified'


def test_annual_and_quarterly_staleness_differ():
    row = {'period_end': date.today() - timedelta(days=250)}
    assert observation_coverage(row, 'UK')['state'] == 'partial'
    assert observation_coverage(row, 'US')['state'] == 'stale'


def test_incremental_success_and_unavailable_have_bounded_polling():
    now = datetime.now(timezone.utc)
    assert not refresh_due({'status': 'success', 'updated_at': now}, 'UK', now)
    assert not refresh_due({'status': 'unavailable', 'updated_at': now}, 'US', now)
    assert refresh_due({'status': 'success', 'updated_at': now - timedelta(days=8)}, 'UK', now)
    assert refresh_due(None, 'US', now)


def test_incremental_partial_and_aging_reports_checked_daily():
    now = datetime.now(timezone.utc)
    previous = {'status': 'success', 'updated_at': now - timedelta(days=2),
                'coverage_json': {'state': 'partial'}}
    assert refresh_due(previous, 'UK', now)
    previous['coverage_json'] = {'state': 'ready', 'period_end': (now.date() - timedelta(days=110)).isoformat()}
    assert refresh_due(previous, 'US', now)
    assert not refresh_due(previous, 'UK', now)


def test_tick_is_finite_maintenance_work_with_limits_inside_lease(monkeypatch):
    from app.tasks.research_ingestion import research_ingestion_tick
    from app.services import research_ingestion
    from app.celery_app import celery_app
    calls = []
    monkeypatch.setattr(research_ingestion, 'enqueue_scheduled', lambda: calls.append('schedule'))
    monkeypatch.setattr(research_ingestion, 'run_one', lambda: calls.append('one') or True)
    monkeypatch.setattr(research_ingestion_tick, 'apply_async', lambda **kwargs: calls.append(kwargs))
    assert research_ingestion_tick.run() == {'processed': True}
    assert calls == ['schedule', 'one', {'countdown': 2}]
    assert 0 < research_ingestion_tick.soft_time_limit < research_ingestion_tick.time_limit < 240
    assert celery_app.conf.task_routes[research_ingestion_tick.name]['queue'] == 'maintenance'
