# tests/test_events_routes.py
import inspect
import pytest

from flask import g

from app.routes import events as routes


def _call(app, view, path, method="GET", json=None, user_id=7):
    with app.test_request_context(path, method=method, json=json):
        g.user_id = user_id
        response = inspect.unwrap(view)()
    if isinstance(response, tuple):
        return response[0].get_json(), response[1]
    return response.get_json(), 200


def test_upcoming_uses_authenticated_user(app, monkeypatch):
    calls = {}
    monkeypatch.setattr(routes.events_read, "list_upcoming_for_user",
                        lambda uid, *, days: calls.setdefault("args", (uid, days)) and [])
    body, status = _call(app, routes.get_upcoming_events, "/api/events/upcoming?days=14&user_id=99")
    assert status == 200 and body["code"] == 1
    assert calls["args"] == (7, 14)


def test_upcoming_rejects_non_integer_days(app):
    body, status = _call(app, routes.get_upcoming_events, "/api/events/upcoming?days=abc")
    assert status == 400 and body["msg"] == "invalid_days"


def test_digests_reject_non_integer_limit(app):
    body, status = _call(app, routes.get_event_digests, "/api/events/digests?limit=x")
    assert status == 400 and body["msg"] == "invalid_limit"


def test_put_settings_validates(app, monkeypatch):
    def save(uid, *, enabled, lead_days):
        raise ValueError("invalid_lead_days")

    monkeypatch.setattr(routes.events_read, "save_digest_settings", save)
    body, status = _call(app, routes.put_digest_settings, "/api/events/digest-settings",
                         method="PUT", json={"enabled": True, "lead_days": 9})
    assert status == 400 and body["msg"] == "invalid_lead_days"


def test_put_settings_saves_for_authenticated_user(app, monkeypatch):
    saved = {}
    monkeypatch.setattr(routes.events_read, "save_digest_settings",
                        lambda uid, *, enabled, lead_days: saved.update(uid=uid, enabled=enabled, lead_days=lead_days)
                        or {"enabled": enabled, "lead_days": lead_days, "global_enabled": True})
    body, status = _call(app, routes.put_digest_settings, "/api/events/digest-settings",
                         method="PUT", json={"enabled": False, "lead_days": 2})
    assert status == 200 and body["data"]["lead_days"] == 2
    assert saved == {"uid": 7, "enabled": False, "lead_days": 2}


def test_routes_are_registered(client):
    rules = {r.rule for r in client.application.url_map.iter_rules()}
    assert {"/api/events/upcoming", "/api/events/digests", "/api/events/digest-settings"} <= rules


@pytest.mark.parametrize("path", ["/api/events/upcoming", "/api/events/digests", "/api/events/digest-settings"])
def test_events_require_login(client, path):
    assert client.get(path).status_code == 401


@pytest.mark.parametrize("payload", [[], "bad", None, {"enabled": "false", "lead_days": 3}, {"lead_days": 3}])
def test_settings_reject_non_object_or_non_boolean(app, payload):
    body, status = _call(app, routes.put_digest_settings, "/api/events/digest-settings", method="PUT", json=payload)
    assert status == 400 and body["code"] == 0
