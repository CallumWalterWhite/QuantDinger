# app/routes/events.py
"""Upcoming catalysts and pre-event digest endpoints for the signed-in user."""

from flask import g, jsonify, request

from app.openapi.blueprint import HumanBlueprint as Blueprint
from app.services import events_read
from app.utils.auth import login_required

events_blp = Blueprint("events", __name__)


def _ok(data):
    return jsonify({"code": 1, "msg": "common.success", "data": data})


def _bad(code: str):
    return jsonify({"code": 0, "msg": code, "data": None}), 400


def _int_arg(name: str, default: int):
    try:
        return int(request.args.get(name, default))
    except (TypeError, ValueError):
        return None


@events_blp.route("/upcoming", methods=["GET"])
@login_required
def get_upcoming_events():
    """Upcoming earnings for the current user's US-stock watchlist and manual positions."""
    days = _int_arg("days", 30)
    if days is None:
        return _bad("invalid_days")
    return _ok(events_read.list_upcoming_for_user(int(g.user_id), days=days))


@events_blp.route("/market-calendar", methods=["GET"])
@login_required
def get_market_calendar():
    """Cached best-effort US/UK earnings, with search, pagination and coverage."""
    params = {"market": request.args.get("market", "all"), "days": _int_arg("days", 30),
              "query": request.args.get("q", ""), "page": _int_arg("page", 1), "page_size": _int_arg("page_size", 50)}
    try:
        events_read.validate_market_calendar(**params)
    except ValueError as exc:
        return _bad(str(exc))
    return _ok(events_read.list_market_earnings(**params))


@events_blp.route("/digests", methods=["GET"])
@login_required
def get_event_digests():
    """Pre-event digest history for the current user, newest first."""
    limit = _int_arg("limit", 50)
    if limit is None:
        return _bad("invalid_limit")
    return _ok(events_read.list_digests_for_user(int(g.user_id), limit=limit))


@events_blp.route("/digest-settings", methods=["GET"])
@login_required
def get_digest_settings():
    """Current user's digest preferences plus the global master switch."""
    return _ok(events_read.get_digest_settings(int(g.user_id)))


@events_blp.route("/digest-settings", methods=["PUT"])
@login_required
def put_digest_settings():
    """Save the current user's digest preferences."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return _bad("invalid_settings")
    if not isinstance(payload.get("enabled"), bool):
        return _bad("invalid_enabled")
    try:
        data = events_read.save_digest_settings(
            int(g.user_id),
            enabled=payload["enabled"],
            lead_days=payload.get("lead_days"),
        )
    except ValueError as exc:
        return _bad(str(exc))
    return _ok(data)
