"""Transport completeness is separate from Yahoo's best-effort coverage."""
from datetime import date

import pytest

from app.data_providers import market_earnings as provider

START = date(2026, 10, 5)
END = date(2027, 1, 3)


def listing(symbol="TSCO.L", name="TESCO PLC ORD 6 1/3P", exchange="LSE"):
    return {"symbol": symbol, "shortName": name, "exchange": exchange, "quoteType": "EQUITY"}


def calendar(rows, total=None):
    fields = ["ticker", "companyshortname", "eventname", "startdatetime", "startdatetimetype", "epsestimate"]
    return {"finance": {"error": None, "result": [{
        "total": len(rows) if total is None else total,
        "documents": [{"columns": [{"id": field} for field in fields], "rows": rows}],
    }]}}


def event(symbol="TSCO.L", when="2026-10-08T15:30:00.000Z"):
    return [symbol, "Tesco", "H1 2027 Earnings Announcement", when, "TNS", 1.2]


def test_directory_filters_products_and_aliases_and_keeps_venue_unknown():
    raw = [listing(), listing("BP-P.L", "BP CAPITAL PERP SUB"), listing("ETF.L", "INDEX ETF"),
           listing("ABC.XC"), listing("MISSING.L", ""), listing("YOU.L", "YOUGOV PLC ORD 0.2P")]
    result = provider.fetch_listings("UK", request_page=lambda *_: {"total": 6, "quotes": raw}, sleep=lambda _: None)
    assert [r["symbol"] for r in result["items"]] == ["TSCO.L", "YOU.L"]
    assert result["raw_count"] == 6 and result["excluded_count"] == 4
    assert all(r["segment"] == "unknown" for r in result["items"])


def test_directory_paginates_to_advertised_total(monkeypatch):
    monkeypatch.setattr(provider, "DIRECTORY_PAGE_SIZE", 1)
    calls = []
    def page(market, offset, size):
        calls.append(offset)
        return {"total": 2, "quotes": [listing("TSCO.L" if offset == 0 else "YOU.L")]}
    result = provider.fetch_listings("UK", request_page=page, sleep=lambda _: None)
    assert len(result["items"]) == 2 and calls == [0, 1]


@pytest.mark.parametrize("response", [{}, {"quotes": [], "total": 0}, {"quotes": [], "total": 1}, {"quotes": [], "total": -1},
                                       {"quotes": [], "total": "2"}, {"quotes": [], "total": True}])
def test_directory_incomplete_or_malformed_is_not_empty_success(response):
    with pytest.raises(provider.MarketEarningsUnavailable):
        provider.fetch_listings("UK", request_page=lambda *_: response, sleep=lambda _: None)


def test_repeated_page_and_total_change_are_rejected(monkeypatch):
    monkeypatch.setattr(provider, "DIRECTORY_PAGE_SIZE", 1)
    with pytest.raises(provider.MarketEarningsUnavailable):
        provider.fetch_listings("UK", request_page=lambda *_: {"total": 2, "quotes": [listing()]}, sleep=lambda _: None)
    with pytest.raises(provider.MarketEarningsUnavailable):
        provider.fetch_listings("UK", request_page=lambda m, o, s: {"total": 2 + o, "quotes": [listing()]}, sleep=lambda _: None)


def test_us_directory_rejects_wrong_exchange_and_retains_qualified_identity():
    rows = [listing("TSLA", "Tesla Inc.", "NMS"), listing("ABC", "ABC Inc.", "ASE"),
            listing("ABC", "ABC Inc.", "NYQ"), listing("ABC", "ABC Inc.", "PNK")]
    result = provider.fetch_listings("US", request_page=lambda *_: {"total": 4, "quotes": rows}, sleep=lambda _: None)
    assert len(result["items"]) == 3 and result["excluded_count"] == 1


def test_calendar_uses_ids_not_duplicate_labels_and_never_invents_currency():
    result = provider.fetch_calendar("UK", START, END, request_page=lambda *_: calendar([event()]), sleep=lambda _: None)
    row = result["items"][0]
    assert row["event_date"] == date(2026, 10, 8)
    assert row["date_status"] == "unknown"
    assert row["eps_estimate"] is None and row["estimate_currency"] is None
    assert row["revenue_estimate"] is None


def test_calendar_empty_with_valid_total_is_success():
    result = provider.fetch_calendar("UK", START, END, request_page=lambda *_: calendar([]), sleep=lambda _: None)
    assert result["items"] == []


@pytest.mark.parametrize("response", [{}, {"finance": {"error": {"code": "Unauthorized"}}},
                                       calendar([], total=1), calendar([event(when="bad")]),
                                       calendar([event(when="2026-01-01")])])
def test_bad_calendar_is_unavailable(response):
    with pytest.raises(provider.MarketEarningsUnavailable):
        provider.fetch_calendar("UK", START, END, request_page=lambda *_: response, sleep=lambda _: None)


def test_pagination_cap_fails_instead_of_publishing_truncated_snapshot(monkeypatch):
    monkeypatch.setattr(provider, "MAX_PAGES", 1)
    monkeypatch.setattr(provider, "DIRECTORY_PAGE_SIZE", 1)
    with pytest.raises(provider.MarketEarningsUnavailable):
        provider.fetch_listings("UK", request_page=lambda *_: {"total": 2, "quotes": [listing()]}, sleep=lambda _: None)


@pytest.mark.parametrize("market", ["GB", "all", "Crypto"])
def test_invalid_market_has_no_provider_call(market):
    with pytest.raises(ValueError):
        provider.fetch_listings(market, request_page=lambda *_: pytest.fail("provider called"))
