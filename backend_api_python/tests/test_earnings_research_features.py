from datetime import datetime, timezone
import pytest
from app.services.earnings_research.features import financial_trends, price_features


def test_periods_and_units_are_not_mixed():
    rows = [{'period_end': '2025-12-31', 'frequency': 'annual', 'currency': 'GBP', 'revenue': 100},
            {'period_end': '2024-12-31', 'frequency': 'annual', 'currency': 'GBp', 'revenue': 50}]
    assert financial_trends(rows)['revenue_growth_pct'] is None
    rows[1]['currency'] = 'GBP'
    assert financial_trends(rows)['revenue_growth_pct'] == 100
    rows[1]['period_end'] = '2024-06-30'
    assert financial_trends(rows)['revenue_growth_pct'] is None


def test_prices_need_recent_comparable_history():
    assert price_features([], {}, datetime.now(timezone.utc))['state'] == 'unavailable'


def test_extreme_finite_inputs_never_emit_infinity():
    import json
    prices = price_features(
        [{'session_date': '2026-09-30', 'close': 1e-308},
         {'session_date': '2026-10-01', 'close': 1e308}],
        {'currency': 'GBP', 'adjustment': 'split_dividend_adjusted'},
        datetime(2026, 10, 2, tzinfo=timezone.utc))
    assert prices['returns_pct']['1'] is None
    assert 'derived_feature_overflow' in prices['gaps']
    financials = financial_trends([
        {'period_end': '2025-12-31', 'frequency': 'annual', 'currency': 'GBP', 'revenue': 1e308, 'net_income': 1e308},
        {'period_end': '2024-12-31', 'frequency': 'annual', 'currency': 'GBP', 'revenue': 1e-308, 'net_income': -1e308}])
    assert financials['revenue_growth_pct'] is None and financials['net_income_change'] is None
    json.dumps({'prices': prices, 'financials': financials}, allow_nan=False)


def test_equity_volatility_uses_252_sessions_with_london_currency():
    import math
    import statistics
    from datetime import date, timedelta
    changes = [0.01, -0.005] * 10
    closes = [100.0]
    for change in changes:
        closes.append(closes[-1] * math.exp(change))
    # Provider normalization owns exchange-session validation; these are its
    # already completed bars, consumed in order without a crypto annualizer.
    start = date(2026, 8, 25)
    dates = [start + timedelta(days=index) for index in range(45)]
    dates = [day for day in dates if day.weekday() < 5][:21]
    bars = [{'session_date': day.isoformat(), 'close': close} for day, close in zip(dates, closes)]
    result = price_features(bars, {'currency': 'GBp', 'adjustment': 'split_dividend_adjusted'},
                            datetime.combine(dates[-1], datetime.min.time(), timezone.utc))
    assert result['annualized_volatility_pct'] == pytest.approx(statistics.stdev(changes) * math.sqrt(252) * 100)
    assert result['currency'] == 'GBp'
