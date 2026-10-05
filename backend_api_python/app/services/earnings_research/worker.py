"""One finite deterministic source operation. No AI or broker imports."""
import os
from app.data_providers.earnings_evidence import fetch_evidence
from app.services.earnings_research import repository as repo


def enabled():
    return os.getenv('ENABLE_EARNINGS_RESEARCH_EVIDENCE', 'false').lower() == 'true'


def run_one(fetch=fetch_evidence):
    if not enabled():
        return False
    item = repo.claim_one()
    if not item:
        return False
    if item.get('skipped'):
        return True
    try:
        result = fetch(item)
    except Exception:
        result = {'status': 'retry', 'provider': 'bounded_adapter', 'data': {}, 'gaps': ['provider_unavailable']}
    try:
        repo.publish(item, result)
    except ValueError:
        repo.publish(item, {'status': 'unavailable', 'provider': 'bounded_adapter', 'data': {}, 'gaps': ['invalid_provider_result']})
    return True


def pending():
    return bool(repo.query("SELECT 1 FROM qd_earnings_research_jobs WHERE status='running' LIMIT 1"))
