"""Closure evidence and the shared admission gate for short-note consumers."""
import re
from urllib.parse import urlsplit

from .queue import digest, require

VALUE_VERDICTS = ('trivial', 'incremental', 'substantial')
GAP_FIELDS = {'statement', 'note_locator', 'source_doi', 'source_arxiv',
              'source_url', 'source_locator', 'why_it_matters'}
UPLIFT_LIMIT = 2
ARCHIVE_REASON = 'trivial after uplift'


def nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def doi(value):
    if not isinstance(value, str):
        return ''
    return re.sub(r'^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)', '',
                  value.strip(), flags=re.I).casefold()


def arxiv(value):
    if not isinstance(value, str):
        return ''
    return re.sub(r'^(?:https?://arxiv\.org/(?:abs|pdf)/|arxiv:\s*)', '',
                  value.strip(), flags=re.I).removesuffix('.pdf').casefold()


def validate_gaps(gaps):
    require(isinstance(gaps, list), 'formalization_gaps must be a list')
    for gap in gaps:
        require(isinstance(gap, dict) and set(gap) == GAP_FIELDS,
                'Invalid formalization gap fields')
        require(all(nonempty(gap[key]) for key in
                    ('statement', 'note_locator', 'source_locator', 'why_it_matters')),
                'Each formalization gap needs the exact statement, both locators and its significance')
        require(gap['source_doi'] is None or re.fullmatch(r'10\.\d{4,}/\S+', doi(gap['source_doi'])),
                'Invalid formalization gap DOI')
        require(gap['source_arxiv'] is None or re.fullmatch(
            r'(?:\d{4}\.\d{4,5}|[a-z][a-z.\-]*/\d{7})(?:v[1-9]\d*)?', arxiv(gap['source_arxiv'])),
            'Invalid formalization gap arXiv ID')
        url = gap['source_url']
        if url is not None:
            require(nonempty(url) and not any(c.isspace() for c in url), 'Invalid gap source URL')
            parsed = urlsplit(url)
            require(parsed.scheme == 'https' and parsed.hostname and not parsed.username
                    and not parsed.password, 'Use a public HTTPS source URL')
        require(any(gap[key] is not None for key in ('source_doi', 'source_arxiv', 'source_url')),
                'Each formalization gap needs a DOI, arXiv ID or public source URL')


def validate_result(result, required=False):
    if required or 'formalization_gaps' in result:
        validate_gaps(result.get('formalization_gaps'))
    if required or 'value_assessment' in result:
        value = result.get('value_assessment')
        require(isinstance(value, dict) and set(value) == {'verdict', 'reasons'}
                and value['verdict'] in VALUE_VERDICTS and nonempty(value['reasons']),
                'Short-note value assessment needs a valid verdict and concrete reasons')


def trivial(result):
    return (result.get('value_assessment') or {}).get('verdict') == 'trivial'


def complete(result):
    return 'formalization_gaps' in result and 'value_assessment' in result


def eligible(row):
    """Final round eligibility. Already accepted legacy PASSes may lack closure."""
    result = row['data'].get('result', {})
    return (row['kind'] == 'shortnote' and row['request']['inputs'].get('note_round') != 'draft'
            and not trivial(result)
            and (complete(result) or row['data'].get('review', {}).get('verdict') == 'PASS'))


def retry_reason(row):
    if row['kind'] != 'shortnote' or row['state'] != 'awaiting-review':
        return None
    result = row['data'].get('result', {})
    if result.get('outcome') != 'completed':
        return None
    if row['request']['inputs'].get('note_round') == 'draft':
        return 'draft'
    if trivial(result):
        return 'trivial'
    if not complete(result):
        return 'missing-closure'
    return None


def validate_sources(links):
    require(isinstance(links, list), 'note_sources must be a list')
    for link in links:
        require(isinstance(link, dict) and set(link) == {'task_id', 'result_hash'}
                and nonempty(link['task_id']) and isinstance(link['result_hash'], str)
                and re.fullmatch(r'[0-9a-f]{64}', link['result_hash']),
                'Each note source needs a task ID and exact result hash')


def source_gate(request, tasks, result=None):
    """Check explicitly declared notes in manually staged mail and handoffs too."""
    if request['kind'] not in {'firstcontact', 'reply', 'mathesis'}:
        return
    from .tasks import review_current

    inputs = request['inputs']
    sources = {}
    for links in (inputs.get('note_sources', []), (result or {}).get('note_sources', [])):
        validate_sources(links)
        for link in links:
            sources[link['task_id'], link['result_hash']] = True
    pipeline = inputs.get('pipeline') or {}
    note = inputs.get('note')
    if isinstance(note, dict) and note.get('task_id'):
        sources.setdefault((note['task_id'], digest(note)), False)
    if pipeline.get('note_task_id'):
        sources.setdefault((pipeline['note_task_id'], pipeline.get('note_result_hash')), False)
    for (task_id, result_hash), explicit in sources.items():
        parent = tasks.get(task_id)
        published = parent['data'].get('result', {})
        require(eligible(parent) and parent['state'] in {'approved', 'handoff-ready', 'verified'}
                and parent['data'].get('review', {}).get('verdict') == 'PASS'
                and published.get('outcome') == 'completed'
                and digest(published) == result_hash,
                'Source short note needs an exact PASS and a non-trivial final round')
        # Legacy pipeline rows previously checked their PASS and result
        # separately. Explicit references and newly closed notes use the full binding.
        if explicit or complete(published):
            require(review_current(parent, parent['request']['workflow_hash']),
                    'Source short note PASS no longer binds the exact result')
