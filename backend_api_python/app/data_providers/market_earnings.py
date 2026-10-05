"""Bounded Yahoo snapshots for personal research, not guaranteed market coverage.

The visualization endpoint is an internal Yahoo contract. Missing or changing
transport metadata must fail closed; a successful transport is still best-effort
issuer coverage. No per-ticker requests or paid fallback are made here.
"""
from __future__ import annotations

from datetime import date, datetime
import json
import re
import time

DIRECTORY_PAGE_SIZE = 250
CALENDAR_PAGE_SIZE = 100
MAX_PAGES = 120
REQUEST_DELAY = 0.5
EXCHANGES = {"US": ("NMS", "NGM", "NCM", "NYQ", "ASE"), "UK": ("LSE",)}
EXCHANGE_NAMES = {"NMS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ", "NYQ": "NYSE", "ASE": "NYSE American", "LSE": "LSE"}
EXCLUDED_PRODUCT = re.compile(
    r"\b(ETF|ETN|FUND|TRUST|WARRANTS?|RIGHTS?|UNITS?|PREF(?:ERRED)?|PERP(?:ETUAL)?|SUB(?:ORDINATED)?|BONDS?|NOTES?|DEBENTURES?)\b", re.I
)


class MarketEarningsUnavailable(Exception):
    """No complete, validated transport snapshot is available."""


def _market(market):
    if market not in EXCHANGES:
        raise ValueError("invalid_market")


def _total(value):
    if type(value) is not int or value < 0:
        raise MarketEarningsUnavailable("invalid_total")
    return value


def _pages(request, parse, size, sleep):
    rows, expected, fingerprints = [], None, set()
    for page in range(MAX_PAGES):
        try:
            total, batch = parse(request(page * size, size))
            total = _total(total)
            if not isinstance(batch, list) or len(batch) > size:
                raise MarketEarningsUnavailable("invalid_page")
            if expected is not None and total != expected:
                raise MarketEarningsUnavailable("changing_total")
            expected = total
            if total > size * MAX_PAGES or len(rows) + len(batch) > total:
                raise MarketEarningsUnavailable("pagination_limit")
            if batch:
                fingerprint = json.dumps(batch, sort_keys=True, default=str)
                if fingerprint in fingerprints:
                    raise MarketEarningsUnavailable("repeated_page")
                fingerprints.add(fingerprint)
            rows.extend(batch)
            if len(rows) == total:
                return rows, total
            if len(batch) != size:
                raise MarketEarningsUnavailable("truncated_page")
            sleep(REQUEST_DELAY)
        except MarketEarningsUnavailable:
            raise
        except Exception as exc:
            # Never propagate credential-bearing provider errors to logs or APIs.
            raise MarketEarningsUnavailable("provider_unavailable") from exc
    raise MarketEarningsUnavailable("pagination_limit")


def _directory_request(market, offset, size):
    import yfinance as yf
    return yf.screen(yf.EquityQuery("is-in", ["exchange", *EXCHANGES[market]]),
                     offset=offset, size=size, sortField="ticker", sortAsc=True)


def fetch_listings(market, *, request_page=None, sleep=time.sleep):
    _market(market)
    request_page = request_page or _directory_request
    def parse(response):
        if not isinstance(response, dict):
            raise MarketEarningsUnavailable("invalid_directory")
        return response.get("total"), response.get("quotes")
    raw, count = _pages(lambda offset, size: request_page(market, offset, size), parse,
                        DIRECTORY_PAGE_SIZE, sleep)
    items, seen = [], set()
    for row in raw:
        if not isinstance(row, dict):
            raise MarketEarningsUnavailable("invalid_listing")
        symbol = str(row.get("symbol") or "").strip().upper()
        exchange = str(row.get("exchange") or "").strip().upper()
        name = str(row.get("shortName") or row.get("longName") or "").strip()
        if (not symbol or len(symbol) > 50 or not name or exchange not in EXCHANGES[market]
                or row.get("quoteType") != "EQUITY" or EXCLUDED_PRODUCT.search(name)
                or (market == "UK" and not symbol.endswith(".L"))):
            continue
        key = (exchange, symbol)
        if key in seen:
            raise MarketEarningsUnavailable("duplicate_listing")
        seen.add(key)
        items.append({"market": market, "exchange": exchange, "symbol": symbol,
                      "provider_symbol": symbol, "name": name[:300],
                      "instrument_type": "equity_unverified", "segment": "unknown"})
    # These established exchanges cannot legitimately have no eligible stocks.
    # A zero-result screener response is not sufficient proof of a deleted catalog.
    if not items:
        raise MarketEarningsUnavailable("no_eligible_listings")
    return {"items": items, "raw_count": count, "excluded_count": count - len(items)}


def _calendar_request(market, start, end, offset, size):
    from yfinance.data import YfData
    operand = lambda op, values: {"operator": op, "operands": values}
    body = {
        "entityIdType": "sp_earnings", "sortField": "startdatetime", "sortType": "ASC",
        "includeFields": ["ticker", "companyshortname", "eventname", "startdatetime", "startdatetimetype", "epsestimate"],
        "size": size, "offset": offset,
        "query": operand("AND", [operand("EQ", ["region", "us" if market == "US" else "gb"]),
            operand("OR", [operand("EQ", ["eventtype", "EAD"]), operand("EQ", ["eventtype", "ERA"])]),
            operand("GTE", ["startdatetime", start.isoformat()]), operand("LTE", ["startdatetime", end.isoformat()])]),
    }
    response = YfData().post("https://query1.finance.yahoo.com/v1/finance/visualization",
        params={"lang": "en-US", "region": "US" if market == "US" else "GB"}, body=body, timeout=30)
    response.raise_for_status()
    return response.json()


def _parse_calendar(response):
    try:
        finance = response["finance"]
        if finance.get("error"):
            raise ValueError("provider_error")
        results = finance["result"]
        if len(results) != 1 or len(results[0]["documents"]) != 1:
            raise ValueError("invalid_documents")
        document = results[0]["documents"][0]
        fields = [column["id"] for column in document["columns"]]
        if len(fields) != len(set(fields)) or not {"ticker", "startdatetime", "eventname"} <= set(fields):
            raise ValueError("invalid_columns")
        rows = document["rows"]
        if not isinstance(rows, list) or any(not isinstance(row, list) or len(row) != len(fields) for row in rows):
            raise ValueError("invalid_rows")
        return results[0]["total"], [dict(zip(fields, row)) for row in rows]
    except (KeyError, TypeError, IndexError, ValueError) as exc:
        raise MarketEarningsUnavailable("invalid_calendar") from exc


def fetch_calendar(market, start: date, end: date, *, request_page=None, sleep=time.sleep):
    _market(market)
    if not isinstance(start, date) or not isinstance(end, date) or not 0 <= (end - start).days <= 90:
        raise ValueError("invalid_window")
    request_page = request_page or _calendar_request
    raw, count = _pages(lambda offset, size: request_page(market, start, end, offset, size),
                        _parse_calendar, CALENDAR_PAGE_SIZE, sleep)
    items, seen = [], set()
    for row in raw:
        symbol = str(row.get("ticker") or "").strip().upper()
        try:
            # Yahoo's date-only event must not be converted through a browser timezone.
            when = datetime.fromisoformat(str(row["startdatetime"]).replace("Z", "+00:00")).date()
        except (TypeError, ValueError) as exc:
            raise MarketEarningsUnavailable("invalid_event_date") from exc
        if not symbol or len(symbol) > 50 or not start <= when <= end:
            raise MarketEarningsUnavailable("invalid_event")
        key = (symbol, when)
        if key in seen:
            continue  # Yahoo may expose announcement and report records for one event.
        seen.add(key)
        items.append({"symbol": symbol, "event_type": "earnings", "event_date": when,
                      "reporting_period": str(row.get("eventname") or "")[:200], "date_status": "unknown",
                      "eps_estimate": None, "revenue_estimate": None, "estimate_currency": None,
                      "source": "yahoo"})
    return {"items": items, "raw_count": count}
