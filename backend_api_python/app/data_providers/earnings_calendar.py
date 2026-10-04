# app/data_providers/earnings_calendar.py
"""Next-earnings lookup backed by yfinance."""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Callable

from app.utils.logger import get_logger

logger = get_logger(__name__)


class CalendarUnavailable(Exception):
    """The provider failed; existing calendar data must be retained."""


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _to_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) or math.isinf(number) else number


def fetch_next_earnings(
    symbol: str,
    *,
    today: date | None = None,
    ticker_factory: Callable[[str], Any] | None = None,
) -> dict[str, Any] | None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    today = today or date.today()
    try:
        if ticker_factory is None:
            import yfinance as yf

            ticker_factory = yf.Ticker
        calendar = ticker_factory(normalized).calendar
    except Exception as exc:
        raise CalendarUnavailable(normalized) from exc

    # yfinance can silently return {} after a network failure. Without a
    # calendar status, an empty dict is not proof the earnings date was removed.
    if not isinstance(calendar, dict) or not calendar:
        raise CalendarUnavailable("unexpected_calendar_shape")
    raw_dates = calendar.get("Earnings Date") or []
    if not isinstance(raw_dates, (list, tuple)):
        raw_dates = [raw_dates]
    dates = [_to_date(v) for v in raw_dates]
    if raw_dates and not any(d is not None for d in dates):
        raise CalendarUnavailable("invalid_earnings_dates")
    upcoming = sorted(d for d in dates if d is not None and d >= today)
    if not upcoming:
        return None
    return {
        "symbol": normalized,
        "event_type": "earnings",
        "event_date": upcoming[0],
        "eps_estimate": _to_float(calendar.get("Earnings Average")),
        "revenue_estimate": _to_float(calendar.get("Revenue Average")),
        "source": "yfinance",
    }
