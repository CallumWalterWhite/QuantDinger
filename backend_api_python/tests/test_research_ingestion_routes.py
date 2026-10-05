"""Admin boundary and bounded submission/read contracts."""
import inspect

from flask import g
import pytest

from app.routes import research_ingestion as routes
from app.utils import auth

PREFIX = '/api/settings/research-ingestion'


def call(app, view, path='', method='GET', data=None):
    with app.test_request_context(PREFIX + path, method=method, json=data):
        g.user_id = 7
        result = inspect.unwrap(view)()
    return (result[0].get_json(), result[1]) if isinstance(result, tuple) else (result.get_json(), 200)


@pytest.mark.parametrize('path,method', [('/overview','get'),('/listings','get'),('/jobs/1','get'),
                                      ('/sync','post'),('/jobs/1/retry','post'),('/schedule','put')])
def test_auth_boundaries(client, monkeypatch, path, method):
    request = getattr(client, method)
    assert request(PREFIX + path).status_code == 401
    monkeypatch.setattr(auth, 'verify_token', lambda token: {'user_id': 7, '_verified_user_role': 'user', '_verified_username': 'test'})
    assert request(PREFIX + path, headers={'Authorization': 'Bearer test'}).status_code == 403


def test_sync_uses_authenticated_identity(app, monkeypatch):
    seen = []
    monkeypatch.setattr(routes.jobs, 'start_job', lambda *args: seen.append(args) or {'job_id': 4})
    response, status = call(app, routes.research_sync, '/sync', 'POST',
                            {'market': 'US', 'request_id': 'abc', 'requester_id': 99})
    assert status == 200 and response['data']['job_id'] == 4
    assert seen == [(7, 'US', 'abc', True)]


@pytest.mark.parametrize('data', [None, [], 'bad', {'market': []}, {'market': 'CN'},
                                {'market': 'US', 'request_id': []},
                                {'market': 'US', 'request_id': 'abc', 'incremental': 'false'}])
def test_invalid_submission_before_database(app, data):
    assert call(app, routes.research_sync, '/sync', 'POST', data)[1] == 400


@pytest.mark.parametrize('query', ['page=abc', 'page=0', 'page_size=201', 'market=CN', 'state=garbage'])
def test_invalid_listing_filters(app, query):
    assert call(app, routes.research_listings, '/listings?' + query)[1] == 400


def test_conflicts_and_no_provider_reads(app, monkeypatch):
    monkeypatch.setattr(routes.reads, 'overview', lambda: {'markets': []})
    assert call(app, routes.research_overview)[0]['data'] == {'markets': []}
    def conflict(*args):
        raise ValueError('request_id_conflict')
    monkeypatch.setattr(routes.jobs, 'start_job', conflict)
    assert call(app, routes.research_sync, '/sync', 'POST', {'market': 'US', 'request_id': 'abc'})[1] == 409
