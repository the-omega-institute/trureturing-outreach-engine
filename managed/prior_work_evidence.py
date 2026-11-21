"""Project the reviewed prior-work check onto scientific issue fields."""
from calendar import monthrange
from datetime import date, datetime
import re
from urllib.parse import urlsplit

from .location import DATA_REPO
from .location import DATA_REPO
from .formalization_requests import compact
from .pre_reviewed import prior_section, prior_section_text
from .queue import require

CONTEXT = {'result_id', 'statement', 'posing_locator', 'posing_url', 'resolution_date', 'lean_url'}
UNKNOWN = {'', 'date unknown', 'unknown', 'undated', 'n.d.', '—', '-'}
# Source wording only. Mathematical uses of "correspondence" stay public.
PRIVATE = re.compile(
    r'\b(?:private|confidential)(?:ly)?\b|\be-?mail(?:s|ed)?\b'
    r'|\bpers(?:onal|\.)\s*(?:communication|comm\b|correspondence)'
    r'|\b(?:our|their|his|her) correspondence\b|\b(?:in|by|via|through) correspondence\b(?! with\b)'
    r'|\bcorrespondence (?:with|from) (?:the |its |his |her |their )?(?:posing |original )?'
    r'(?:authors?|posers?|prof(?:essor)?|dr)\b', re.I)
UNVERIFIED = re.compile(r'\b(?:unverified|unread|unchecked|not (?:checked|verified)|partial(?: overlap)?)\b', re.I)
NONSETTLEMENT = re.compile(r'\b(?:does not|did not|doesn.t) (?:settle|prove|refute|answer|resolve)\b', re.I)
EMAIL = re.compile(r'<?\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b>?')


class UnparseableEvidence(ValueError):
    pass


def line(value):
    if not isinstance(value, str):
        return ''
    # Issue text is public. An address never belongs in it.
    return compact(EMAIL.sub('', re.sub(r'<!--.*?-->', '', value, flags=re.S)))


def public_url(value):
    if not isinstance(value, str):
        return ''
    value = value.strip().strip('<>').strip('`')
    link = re.fullmatch(r'\[[^\]]+\]\((https?://\S+)\)', value)
    value = link[1] if link else value
    if not value or any(c.isspace() or c in '<>"`' for c in value) or EMAIL.search(value):
        return ''
    try:
        url = urlsplit(value)
        host = url.hostname or ''
    except ValueError:
        return ''
    if (url.scheme not in {'http', 'https'} or '.' not in host or url.username or url.password
            or host in {'chatgpt.com', 'outlook.office.com', 'outlook.office365.com', 'mail.google.com'}
            or host.endswith('.slack.com')
            or (host == 'github.com' and url.path.lower().startswith(f'/{DATA_REPO.lower()}/'))):
        return ''
    return value


def date_bounds(value):
    """Return the range covered by a date, preserving year/month precision."""
    value = line(value)
    try:
        if re.fullmatch(r'\d{4}', value):
            year = int(value)
            return date(year, 1, 1), date(year, 12, 31)
        if re.fullmatch(r'\d{4}-\d{2}', value):
            year, month = map(int, value.split('-'))
            return date(year, month, 1), date(year, month, monthrange(year, month)[1])
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
            day = date.fromisoformat(value)
            return day, day
        if re.match(r'\d{4}-\d{2}-\d{2}T', value):
            day = datetime.fromisoformat(value.replace('Z', '+00:00')).date()
            return day, day
        value = re.sub(r'\bSept\b', 'Sep', value, flags=re.I).replace(',', '').replace('.', '')
        for fmt in ('%B %Y', '%b %Y', '%d %B %Y', '%d %b %Y', '%B %d %Y', '%b %d %Y'):
            try:
                day = datetime.strptime(value, fmt).date()
            except ValueError:
                continue
            return day, day.replace(day=monthrange(day.year, day.month)[1]) if '%d' not in fmt else day
    except ValueError:
        return None
    return None


def validate(check):
    require(isinstance(check, dict) and check.get('verdict') in ('first-known', 'prior-exists', 'unclear'),
            'Invalid prior_work_check verdict')
    require(isinstance(check.get('items'), list) and all(isinstance(item, dict) for item in check['items']),
            'prior_work_check needs a list of evidence items')


def project(check):
    """Keep public scientific fields and recognize historical private reports."""
    validate(check)
    projected = {key: line(check.get(key)) for key in CONTEXT}
    projected.update(verdict=check['verdict'], items=[])
    for item in check['items']:
        status = line(item.get('status'))
        authors = item.get('authors')
        if isinstance(authors, list):
            authors = ', '.join(filter(None, map(line, authors)))
        authors = line(authors)
        locator = line(item.get('locator'))
        proves = line(item.get('proves') or item.get('what_it_proves'))
        if item.get('private') not in (None, False) or any(PRIVATE.search(value) for value in (
                authors, locator, proves, status, *(line(item.get(key)) for key in ('kind', 'visibility', 'source')))):
            projected['items'].append({'private': True})
            continue
        if item.get('verified') is False or item.get('settles') is False or UNVERIFIED.search(status):
            continue
        url = public_url(item.get('url'))
        if not (url and authors and proves) or UNVERIFIED.search(proves) or NONSETTLEMENT.search(proves):
            continue
        dated = str(item.get('date') or item.get('year') or '').strip()
        dated = '' if dated.lower() in UNKNOWN else dated
        when = dated if date_bounds(dated) else ('unrecognized date' if dated else '')
        projected['items'].append({'url': url, 'authors': authors, 'date': when,
                                   'locator': locator, 'proves': proves})
    return projected


LABELS = {
    'author': 'authors', 'authors': 'authors', 'date': 'date', 'year': 'year',
    'year/date': 'date', 'release date': 'date', 'locator': 'locator',
    'url': 'url', 'public url': 'url', 'what it proves': 'proves', 'what exactly it proves': 'proves',
    'proves': 'proves', 'kind': 'kind', 'visibility': 'visibility', 'status': 'status', 'source': 'source',
    'result id': 'result_id', 'statement': 'statement', 'posed statement': 'statement',
    'posing locator': 'posing_locator', 'posing url': 'posing_url',
    'resolution date': 'resolution_date', 'upstream resolution date': 'resolution_date',
    'trureturing resolution date': 'resolution_date', 'lean url': 'lean_url', 'lean link': 'lean_url',
}


def label(value):
    return LABELS.get(value.strip(' *_`').lower().replace('_', ' '))


def parse_text(text):
    verdict = prior_section(text)
    if not verdict:
        return None
    check = {'verdict': verdict['verdict'], 'items': []}
    if check['verdict'] != 'prior-exists':
        return project(check)
    item, columns = None, []
    for raw in prior_section_text(text).splitlines():
        raw = raw.strip()
        if raw.startswith('|'):
            cells = [value.strip() for value in re.split(r'(?<!\\)\|', raw.strip('|'))]
            fields = [label(value) for value in cells]
            if 'url' in fields and 'authors' in fields:
                if item:
                    check['items'].append(item)
                item = None
                columns = fields
            elif len(cells) == len(columns) and not all(re.fullmatch(r'[: -]+', c) for c in cells):
                check['items'].append({key: value for key, value in zip(columns, cells) if key})
            continue
        heading = re.match(
            r'^(?:#+ +|[-*] +|\d+[.)] +)(?:\*\*)?'
            r'(?:item\s*\d*\b|verified item|prior (?:item|work)|private (?:report|correspondence)|reported by)',
            raw, re.I)
        if heading or re.match(r'^#{1,6} +', raw):
            if item:
                check['items'].append(item)
            item = {}
            if PRIVATE.search(raw):
                item['private'] = True
            continue
        stripped = re.sub(r'^(?:[-*] |\d+[.)] )', '', raw)
        match = re.match(r'^([\w */_`-]+):[ *_`]*(.*)$', stripped)
        key = label(match[1]) if match else None
        if key in CONTEXT:
            check[key] = match[2].strip(' *')
        elif key:
            value = match[2].strip(' *')
            if item is None or key in item:
                raise UnparseableEvidence('Prior-work evidence requires one heading per entry')
            if key == 'kind' and PRIVATE.search(value) and item and not item.get('private'):
                raise UnparseableEvidence('Prior-work evidence requires one heading per entry')
            item[key] = value
    if item:
        check['items'].append(item)
    return project(check)
