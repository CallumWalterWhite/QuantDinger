"""Celery tasks for the earnings calendar and pre-event digests."""

from __future__ import annotations

import os

from app.celery_app import celery_app


def _enabled(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


@celery_app.task(
    bind=True,
    name="quantdinger.tasks.earnings_calendar_sync",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=3,
)
def run_earnings_calendar_sync(self):
    del self
    if not _enabled("ENABLE_EARNINGS_CALENDAR_SYNC", "true"):
        return {"skipped": True}
    from app.services.upcoming_events import sync_earnings_calendar

    return sync_earnings_calendar()


@celery_app.task(
    bind=True,
    name="quantdinger.tasks.pre_event_digest",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=2,
)
def run_pre_event_digest(self):
    del self
    if not _enabled("ENABLE_PRE_EVENT_DIGEST", "false"):
        return {"skipped": True}
    from app.services.event_radar import EventRadarService
    from app.services.pre_event_digest import run_pre_event_digests

    sec_user_agent = EventRadarService._settings()["sec_edgar_user_agent"]
    try:
        lead_days = min(7, max(0, int(os.getenv("PRE_EVENT_DIGEST_LEAD_DAYS", "3"))))
    except ValueError:
        lead_days = 3
    return run_pre_event_digests(
        lead_days=lead_days,
        sec_user_agent=sec_user_agent,
    )
