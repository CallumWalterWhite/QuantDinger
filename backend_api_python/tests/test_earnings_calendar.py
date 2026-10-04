# tests/test_earnings_calendar.py
from datetime import date
import pytest

from app.data_providers import earnings_calendar as module

TODAY = date(2026, 10, 1)


class FakeTicker:
    def __init__(self, calendar):
        self.calendar = calendar


def _factory(calendar):
    return lambda symbol: FakeTicker(calendar)


def test_returns_earliest_future_date_with_estimates():
    cal = {
        "Earnings Date": [date(2026, 10, 28), date(2026, 10, 27)],
        "Earnings Average": 2.31,
        "Revenue Average": 94_000_000_000,
    }
    result = module.fetch_next_earnings(" googl ", today=TODAY, ticker_factory=_factory(cal))
    assert result == {
        "symbol": "GOOGL",
        "event_type": "earnings",
        "event_date": date(2026, 10, 27),
        "eps_estimate": 2.31,
        "revenue_estimate": 94_000_000_000.0,
        "source": "yfinance",
    }


def test_ignores_past_dates():
    cal = {"Earnings Date": [date(2026, 7, 20)]}
    assert module.fetch_next_earnings("GOOGL", today=TODAY, ticker_factory=_factory(cal)) is None


def test_today_counts_as_upcoming():
    cal = {"Earnings Date": [TODAY]}
    result = module.fetch_next_earnings("GOOGL", today=TODAY, ticker_factory=_factory(cal))
    assert result["event_date"] == TODAY
    assert result["eps_estimate"] is None


def test_missing_or_empty_calendar_returns_none():
    for cal in ({"Earnings Date": []},):
        assert module.fetch_next_earnings("SPY", today=TODAY, ticker_factory=_factory(cal)) is None


def test_nan_estimates_become_none():
    cal = {"Earnings Date": [date(2026, 10, 27)], "Earnings Average": float("nan")}
    result = module.fetch_next_earnings("GOOGL", today=TODAY, ticker_factory=_factory(cal))
    assert result["eps_estimate"] is None


def test_provider_exception_is_distinct_from_empty_calendar():
    def boom(symbol):
        raise RuntimeError("yahoo down")

    with pytest.raises(module.CalendarUnavailable):
        module.fetch_next_earnings("GOOGL", today=TODAY, ticker_factory=boom)


@pytest.mark.parametrize("calendar", [None, {}, [], "unexpected", {"Earnings Date": ["invalid"]}])
def test_unknown_shape_is_failure(calendar):
    with pytest.raises(module.CalendarUnavailable):
        module.fetch_next_earnings("GOOGL", today=TODAY, ticker_factory=_factory(calendar))
