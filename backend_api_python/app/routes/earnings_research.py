"""Administrative cached evidence reads and durable manual commands."""
from flask import g, jsonify, request
from app.routes.events import events_blp
from app.routes.research_ingestion import ADMIN, PAGES, request_doc
from app.services.earnings_research import repository as repo, read, evidence, worker
from app.utils.auth import admin_required, login_required

WINDOW = [
    {'in': 'query', 'name': 'market', 'schema': {'type': 'string', 'enum': ['all', 'US', 'UK'], 'default': 'all'}},
    {'in': 'query', 'name': 'days', 'schema': {'type': 'integer', 'minimum': 1, 'maximum': 90, 'default': 30}},
]
LIST_FILTERS = [
    {'in': 'query', 'name': 'q', 'schema': {'type': 'string', 'maxLength': 100}},
    {'in': 'query', 'name': 'state', 'schema': {'type': 'string', 'enum': list(read.STATES)}},
    {'in': 'query', 'name': 'mode', 'schema': {'type': 'string', 'enum': ['directory', 'candidates'], 'default': 'directory'}},
]


def respond(call):
    try:
        return jsonify(code=1, msg='success', data=call())
    except ValueError as exc:
        reason = str(exc)
        status = 404 if reason in ('listing_not_found', 'job_not_found') else 409 if reason in ('request_id_conflict', 'evidence_disabled') else 400
        return jsonify(code=0, msg='earningsResearch.error.' + reason, data=None), status


def filters():
    try:
        days = int(request.args.get('days', '30'))
        page = int(request.args.get('page', '1'))
        page_size = int(request.args.get('page_size', '50'))
    except (TypeError, ValueError):
        raise ValueError('invalid_page') from None
    market = request.args.get('market', 'all')
    query = request.args.get('q', '')
    state = request.args.get('state', '')
    mode = request.args.get('mode', 'directory')
    repo.validate(market, days)
    read.validate_page(page, page_size)
    if len(query) > 100 or mode not in ('directory', 'candidates') or state and state not in read.STATES:
        raise ValueError('invalid_filters')
    return dict(market=market, days=days, page=page, page_size=page_size, query=query, state=state, mode=mode)


def body():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise ValueError('invalid_body')
    return data


@events_blp.route('/research/coverage', methods=['GET'])
@events_blp.doc(**ADMIN, parameters=WINDOW)
@login_required
@admin_required
def research_coverage():
    def run():
        params = filters()
        return read.coverage(g.user_id, params['market'], params['days'])
    return respond(run)


@events_blp.route('/research/listings', methods=['GET'])
@events_blp.doc(**ADMIN, parameters=PAGES + WINDOW + LIST_FILTERS)
@login_required
@admin_required
def research_listings():
    return respond(lambda: read.listings(g.user_id, **filters()))


@events_blp.route('/research/listings/<int:listing_id>/evidence', methods=['GET'])
@events_blp.doc(**ADMIN)
@login_required
@admin_required
def research_evidence(listing_id):
    return respond(lambda: evidence.bundle(listing_id))


@events_blp.route('/research/listings/<int:listing_id>/source', methods=['PUT'])
@events_blp.doc(**ADMIN, requestBody=request_doc({'issuer_url': {'type':'string','maxLength':8192}, 'confirmed': {'type':'boolean'}}, ['issuer_url','confirmed']))
@login_required
@admin_required
def research_source(listing_id):
    def run():
        data = body()
        return repo.verify_issuer_source(g.user_id, listing_id, data.get('issuer_url'), data.get('confirmed'))
    return respond(run)


@events_blp.route('/research/jobs', methods=['POST'])
@events_blp.doc(**ADMIN, requestBody=request_doc({'market': {'type':'string','enum':['all','US','UK']},
    'days': {'type':'integer','minimum':1,'maximum':90}, 'request_id': {'type':'string','maxLength':100}}, ['market','request_id']))
@login_required
@admin_required
def research_start():
    def run():
        data = body()
        if not worker.enabled():
            raise ValueError('evidence_disabled')
        result = repo.start_job(g.user_id, data.get('market'), data.get('days', 30), data.get('request_id'))
        # Broker failure does not undo the committed request. Replaying the same
        # request_id dispatches again; claims fence duplicate task messages.
        try:
            from app.tasks.earnings_research import earnings_research_tick
            earnings_research_tick.apply_async()
            result['dispatched'] = True
        except Exception:
            result['dispatched'] = False
        return result
    return respond(run)


@events_blp.route('/research/jobs/<int:job_id>', methods=['GET'])
@events_blp.doc(**ADMIN, parameters=PAGES)
@login_required
@admin_required
def research_job(job_id):
    def run():
        params = filters()
        return read.job_detail(g.user_id, job_id, params['page'], params['page_size'])
    return respond(run)


@events_blp.route('/research/jobs/<int:job_id>/cancel', methods=['POST'])
@events_blp.doc(**ADMIN)
@login_required
@admin_required
def research_cancel(job_id):
    return respond(lambda: repo.cancel_job(g.user_id, job_id))
