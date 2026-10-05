"""Manual evidence jobs, finite operations on the maintenance queue."""
from app.celery_app import celery_app


@celery_app.task(name='quantdinger.tasks.earnings_research_tick', soft_time_limit=120, time_limit=150)
def earnings_research_tick():
    from app.services.earnings_research.worker import enabled, run_one, pending
    if not enabled():
        return {'processed': False, 'disabled': True}
    processed = run_one()
    if pending():
        earnings_research_tick.apply_async(countdown=2 if processed else 30)
    return {'processed': processed}
