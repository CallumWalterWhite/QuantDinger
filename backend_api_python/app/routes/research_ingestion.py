"""Administrative research-data status and durable ingestion commands."""
from flask import g, jsonify, request

from app.routes.settings import settings_blp
from app.services import research_ingestion as jobs, research_ingestion_read as reads
from app.utils.auth import admin_required, login_required

ADMIN = {'security': [{'HumanJWT': []}], 'responses': {
    '400': {'description': 'Invalid input'}, '401': {'description': 'Login required'},
    '403': {'description': 'Administrator required'}, '409': {'description': 'Active job or request-id conflict'}}}
PAGES = [
    {'in': 'query', 'name': 'page', 'schema': {'type': 'integer', 'minimum': 1, 'maximum': 1000000, 'default': 1}},
    {'in': 'query', 'name': 'page_size', 'schema': {'type': 'integer', 'minimum': 1, 'maximum': 200, 'default': 50}},
]
MARKET = {'type': 'string', 'enum': ['US', 'UK']}
REQUEST_ID = {'type': 'string', 'pattern': r'^[A-Za-z0-9:_\-]{1,100}$',
              'description': 'Reuse after an ambiguous response; scoped to the authenticated administrator.'}


def request_doc(properties, required):
    return {'required': True, 'content': {'application/json': {'schema': {
        'type': 'object', 'properties': properties, 'required': required}}}}


def respond(fn):
    try:
        return jsonify(code=1, msg='success', data=fn())
    except ValueError as exc:
        message = str(exc)
        status = 404 if message == 'job_not_found' else 409 if message in {'job_active', 'request_id_conflict'} else 400
        return jsonify(code=0, msg='researchIngestion.' + message, data=None), status


def body():
    value = request.get_json(silent=True)
    if not isinstance(value, dict):
        raise ValueError('invalid_body')
    return value


def pagination():
    try:
        return int(request.args.get('page', '1')), int(request.args.get('page_size', '50'))
    except (ValueError, TypeError):
        raise ValueError('invalid_page') from None


@settings_blp.route('/research-ingestion/overview', methods=['GET'])
@settings_blp.doc(**ADMIN)
@login_required
@admin_required
def research_overview():
    """Read market directory, calendar and financial coverage without providers."""
    return respond(reads.overview)


@settings_blp.route('/research-ingestion/listings', methods=['GET'])
@settings_blp.doc(**ADMIN, parameters=PAGES + [
    {'in': 'query', 'name': 'market', 'schema': MARKET},
    {'in': 'query', 'name': 'q', 'schema': {'type': 'string', 'maxLength': 100}},
    {'in': 'query', 'name': 'state', 'schema': {'type': 'string', 'enum': list(reads.STATES)}}])
@login_required
@admin_required
def research_listings():
    def run():
        page, size = pagination()
        return reads.listings(request.args.get('market', 'US'), page, size,
                              request.args.get('q', ''), request.args.get('state', ''))
    return respond(run)


@settings_blp.route('/research-ingestion/jobs/<int:job_id>', methods=['GET'])
@settings_blp.doc(**ADMIN, parameters=PAGES)
@login_required
@admin_required
def research_job(job_id):
    return respond(lambda: reads.job_detail(job_id, *pagination()))


@settings_blp.route('/research-ingestion/sync', methods=['POST'])
@settings_blp.doc(**ADMIN, requestBody=request_doc(
    {'market': MARKET, 'request_id': REQUEST_ID, 'incremental': {'type': 'boolean', 'default': True}},
    ['market', 'request_id']))
@login_required
@admin_required
def research_sync():
    def run():
        data = body()
        return jobs.start_job(g.user_id, data.get('market'), data.get('request_id'), data.get('incremental', True))
    return respond(run)


@settings_blp.route('/research-ingestion/jobs/<int:job_id>/retry', methods=['POST'])
@settings_blp.doc(**ADMIN, requestBody=request_doc({'request_id': REQUEST_ID}, ['request_id']))
@login_required
@admin_required
def research_retry(job_id):
    def run():
        data = body()
        row = jobs.query('SELECT market FROM qd_research_ingestion_jobs WHERE id=?', (job_id,))
        if not row:
            raise ValueError('job_not_found')
        return jobs.start_job(g.user_id, row['market'], data.get('request_id'), False, job_id)
    return respond(run)


@settings_blp.route('/research-ingestion/schedule', methods=['PUT'])
@settings_blp.doc(**ADMIN, requestBody=request_doc({'market': MARKET, 'enabled': {'type': 'boolean'}}, ['market', 'enabled']))
@login_required
@admin_required
def research_schedule():
    def run():
        data = body()
        return jobs.set_schedule(g.user_id, data.get('market'), data.get('enabled'))
    return respond(run)
