"""Market-wide discovery runs only in a finite maintenance job."""
import os

from app.celery_app import celery_app


@celery_app.task(name="quantdinger.tasks.market_earnings_sync")
def run_market_earnings_sync():
    if os.getenv("ENABLE_MARKET_EARNINGS_SYNC", "false").strip().lower() not in {"true", "1", "yes", "on"}:
        return {"skipped": True}
    from app.services.market_earnings import sync_market_earnings
    return sync_market_earnings()
