import pytest
from app.utils import auth
from app.services.earnings_research import read, repository as repo

PREFIX = '/api/events/research'


@pytest.mark.parametrize('path,method', [('/coverage','get'),('/listings','get'),('/listings/1/evidence','get'),
                                      ('/jobs','post'),('/jobs/1','get'),('/jobs/1/cancel','post'),('/listings/1/source','put')])
def test_admin_auth(client, monkeypatch, path, method):
    call = getattr(client, method)
    assert call(PREFIX + path).status_code == 401
    monkeypatch.setattr(auth, 'verify_token', lambda token: {'user_id': 1, '_verified_user_role': 'user', '_verified_username': 'test'})
    assert call(PREFIX + path, headers={'Authorization': 'Bearer test'}).status_code == 403


def admin(monkeypatch):
    monkeypatch.setattr(auth, 'verify_token', lambda token: {'user_id': 7, '_verified_user_role': 'admin', '_verified_username': 'test'})
    return {'Authorization': 'Bearer test'}


def test_cached_listing_filters_and_ownership(client, monkeypatch):
    headers = admin(monkeypatch)
    seen = []
    monkeypatch.setattr(read, 'listings', lambda *args, **kwargs: seen.append((args, kwargs)) or {'items': [], 'total': 0})
    response = client.get(PREFIX + '/listings?market=UK&mode=directory&page=2', headers=headers)
    assert response.status_code == 200 and response.json['code'] == 1
    assert seen[0][0][0] == 7 and seen[0][1]['page'] == 2
    monkeypatch.setattr(repo, 'cancel_job', lambda user, job: {'user': user, 'job': job})
    assert client.post(PREFIX + '/jobs/3/cancel', headers=headers).json['data']['user'] == 7


def test_default_disabled_makes_no_job_or_dispatch(client, monkeypatch):
    headers = admin(monkeypatch)
    monkeypatch.delenv('ENABLE_EARNINGS_RESEARCH_EVIDENCE', raising=False)
    monkeypatch.setattr(repo, 'start_job', lambda *args: pytest.fail('disabled feature cannot start a job'))
    assert client.post(PREFIX + '/jobs', json={'market':'US','days':30,'request_id':'x'}, headers=headers).status_code == 409


@pytest.mark.parametrize('query', ['page=0','page_size=201','days=91','market=CN','mode=bad','q='+'x'*101])
def test_invalid_filters(client, monkeypatch, query):
    assert client.get(PREFIX + '/listings?' + query, headers=admin(monkeypatch)).status_code == 400
