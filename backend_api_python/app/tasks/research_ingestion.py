"""One finite research-data operation; never a market-sized provider loop."""
from app.celery_app import celery_app


@celery_app.task(name='quantdinger.tasks.research_ingestion_tick', soft_time_limit=150, time_limit=180)
def research_ingestion_tick():
    from app.services.research_ingestion import enqueue_scheduled, run_one
    enqueue_scheduled()
    processed = run_one()
    if processed:
        research_ingestion_tick.apply_async(countdown=2)
    return {'processed': processed}
