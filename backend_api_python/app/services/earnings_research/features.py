"""Transparent, period-aligned observations; not predictive trading scores."""
from datetime import date
import math
import statistics


def _finite(value, gaps):
    if math.isfinite(value):
        return value
    if 'derived_feature_overflow' not in gaps:
        gaps.append('derived_feature_overflow')
    return None


def financial_trends(rows):
    result = {'revenue_growth_pct': None, 'net_income_change': None,
              'comparison': None, 'gaps': []}
    if len(rows) < 2:
        result['gaps'].append('insufficient_comparable_financials')
        return result
    latest = rows[0]
    period = date.fromisoformat(str(latest['period_end'])[:10])
    prior = next((row for row in rows[1:] if row['currency'] == latest['currency'] and latest['currency']
                  and row['frequency'] == latest['frequency']
                  and 330 <= (period - date.fromisoformat(str(row['period_end'])[:10])).days <= 400), None)
    if not prior:
        result['gaps'].append('insufficient_comparable_financials')
        return result
    result['comparison'] = {'latest': str(latest['period_end']), 'prior': str(prior['period_end']),
                            'frequency': latest['frequency'], 'currency': latest['currency']}
    for field, output in (('revenue', 'revenue_growth_pct'), ('net_income', 'net_income_change')):
        a, b = latest.get(field), prior.get(field)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)) and math.isfinite(a) and math.isfinite(b):
            if field == 'revenue' and b > 0:
                result[output] = _finite((a / b - 1) * 100, result['gaps'])
            elif field == 'net_income':
                result[output] = _finite(a - b, result['gaps'])
    return result


def price_features(bars, meta, now):
    result = {'state': 'unavailable', 'returns_pct': {}, 'annualized_volatility_pct': None,
              'trend_50_session_pct': None, 'gaps': []}
    if not bars or not meta.get('currency') or meta.get('adjustment') != 'split_dividend_adjusted':
        result['gaps'].append('unavailable_comparable_prices')
        return result
    closes = [row['close'] for row in bars]
    if any(not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0 for value in closes):
        result['gaps'].append('invalid_price_history')
        return result
    latest = date.fromisoformat(str(bars[-1]['session_date'])[:10])
    result['state'] = 'stale' if (now.date() - latest).days > 7 else 'partial' if len(bars) < 61 else 'ready'
    result['latest_session'] = latest.isoformat()
    result['currency'] = meta['currency']
    result['adjustment'] = meta['adjustment']
    for horizon in (1, 5, 20, 60):
        if len(closes) > horizon:
            result['returns_pct'][str(horizon)] = _finite((closes[-1] / closes[-horizon-1] - 1) * 100, result['gaps'])
    if len(closes) >= 50:
        result['trend_50_session_pct'] = _finite((closes[-1] / statistics.mean(closes[-50:]) - 1) * 100, result['gaps'])
    if len(closes) >= 21:
        # Subtract logs to avoid overflowing an otherwise finite price ratio.
        returns = [math.log(b) - math.log(a) for a, b in zip(closes[-21:-1], closes[-20:])]
        result['annualized_volatility_pct'] = statistics.stdev(returns) * math.sqrt(252) * 100
    if result['state'] != 'ready':
        result['gaps'].append('price_history_' + result['state'])
    return result
