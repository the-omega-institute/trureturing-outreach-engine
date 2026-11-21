"""Evidence-bearing author proposals; only the local operator edits the registry."""
import hashlib
import hmac
import re
import shlex

from .queue import canonical, digest, require

INSTRUCTIONS = (
    'Return author_contacts in the final result: one proposal for every author of the '
    'primary source that posed the problem, not the authors of our new note. Each has '
    'full_name, title (Professor, Dr., or none), title_evidence_url, affiliation '
    '(current; explicitly state unresolved if necessary), source_url (primary posing '
    'source establishing authorship), emails (a list of {email, evidence_url} verified '
    'from an institutional page, paper or ORCID), problem_ids, and in_refusals (boolean). '
    'Read tables/refusals.json at policy_revision (or source_revision for a historical task) in the private data repository; '
    'check names and all verified addresses/aliases, and record that revision in '
    'the refusals_checked_revision field inside EACH author_contacts entry. '
    'refusals_checked_revision must never appear at the top level of the result. '
    'Omit unverifiable emails, never guess; emails=[] needs '
    'email_omission_reason. Use title=none if no academic title is verified; its evidence '
    'URL must support the identity/status assessment, never invent a degree. Cover all '
    'known authors even when unreachable. If no author can be identified/reached, an '
    'explicit author_contacts=[] is allowed only with a substantive '
    'author_contacts_reason explaining the source search and missing evidence. '
    'These are proposals only: never edit shared tables or grant approval.'
)


def nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def url(value):
    require(isinstance(value, str) and re.fullmatch(r'https?://[^\s/]+(?:/[^\s]*)?', value),
            'Author evidence needs an HTTP(S) URL')
    return value


def problem_ids(value):
    require(isinstance(value, list) and value and all(nonempty(p) for p in value),
            'Author links need nonempty problem_ids')
    require(len(value) == len(set(value)), 'Duplicate author problem ID')


def validate(result, request=None):
    from .registry import email
    authors = result.get('author_contacts')
    require(isinstance(authors, list), 'Short note needs author_contacts proposals')
    if not authors:
        require(nonempty(result.get('author_contacts_reason')),
                'Empty author_contacts needs author_contacts_reason')
    if 'author_contacts_reason' in result:
        require(nonempty(result['author_contacts_reason']), 'Invalid author_contacts_reason')
    seen, seen_addresses = set(), set()
    for author in authors:
        fields = {'full_name', 'title', 'title_evidence_url', 'affiliation', 'source_url',
                  'emails', 'problem_ids', 'in_refusals', 'refusals_checked_revision'}
        require(isinstance(author, dict) and fields <= set(author)
                and set(author) <= fields | {'email_omission_reason'}, 'Invalid author_contacts entry')
        require(nonempty(author['full_name']) and nonempty(author['affiliation']),
                'Author needs full name and current affiliation/status')
        require(isinstance(author['title'], str) and author['title'] in {'Professor', 'Dr.', 'none'},
                'Invalid verified academic title')
        url(author['title_evidence_url']); url(author['source_url'])
        problem_ids(author['problem_ids'])
        require(type(author['in_refusals']) is bool, 'Author needs the pinned refusal check')
        revision = author['refusals_checked_revision']
        require(isinstance(revision, str) and re.fullmatch(r'[0-9a-f]{40}', revision),
                'Pin the refusal-table revision')
        if request:
            require(revision == (request.get('policy_revision') or request['source_revision']), 'Refusal check must use the pinned task revision')
            rid = request.get('inputs', {}).get('result', {}).get('id')
            if rid:
                require(rid in author['problem_ids'], 'Author proposal must cover the task result ID')
        require(isinstance(author['emails'], list), 'Author emails must be a list')
        if not author['emails']:
            require(nonempty(author.get('email_omission_reason')), 'Unreachable author needs email_omission_reason')
        addresses = []
        for item in author['emails']:
            require(isinstance(item, dict) and set(item) == {'email', 'evidence_url'},
                    'Every email needs its own source evidence')
            addresses.append(email(item['email'])); url(item['evidence_url'])
        require(len(addresses) == len(set(addresses)), 'Duplicate author email')
        require(not seen_addresses.intersection(addresses), 'An email cannot identify two author proposals')
        seen_addresses.update(addresses)
        key = (author['full_name'].casefold(), tuple(sorted(author['problem_ids'])))
        require(key not in seen, 'Duplicate author proposal')
        seen.add(key)


def reviewed(value, secret=None):
    """Bind proposals to the complete result, never a detached edited extraction."""
    from .control import validate_result
    from .tasks import validate_request
    require(isinstance(value, dict) and set(value) == {'request', 'result', 'review'},
            'Save the complete reviewed_proposal from work')
    request, result, review = value['request'], value['result'], value['review']
    validate_request(request)
    require(request['kind'] == 'shortnote', 'Only a reviewed short note proposes author registration')
    validate_result(result, {'id':request['id'], 'kind':'shortnote', 'request':request})
    binding = review.get('binding', {})
    require(result['outcome'] == 'completed' and review.get('verdict') == 'PASS'
            and review.get('reviewer') == 'local-claude-loop'
            and binding.get('task_id') == request['id']
            and binding.get('result_hash') == digest(result)
            and binding.get('workflow_hash') == request['workflow_hash'],
            'Author registration requires the exact independently reviewed PASS result')
    if secret is not None:
        require(bool(secret), 'The operator review key is required')
        unsigned = {k:v for k,v in review.items() if k != 'signature'}
        expected = hmac.new(secret.encode(), canonical(unsigned).encode(), hashlib.sha256).hexdigest()
        require(isinstance(review.get('signature'), str)
                and hmac.compare_digest(expected, review['signature']), 'Author proposal review signature failed')
    return result['author_contacts']


def stop_reasons(author, checks):
    reasons = {reason for check in checks for reason in check['reasons']
               if reason in {'table-3-refusal-or-stop-boundary', 'excluded-region-mainland-china'}}
    if author.get('in_refusals'):
        reasons.add('refused-at-reviewed-pinned-revision; operator reconciliation required')
    return sorted(reasons)


def pending(row, root):
    from .registry import people, preflight, email
    value = {'request':row['request'], 'result':row['data']['result'], 'review':row['data']['review']}
    authors = reviewed(value)
    persons = people(root)
    proposals = []
    blocked = []
    for author in authors:
        addresses = []
        if not author['emails'] and nonempty(author.get('email_omission_reason')):
            blocked.append({'full_name': author['full_name'], 'emails': [],
                            'reason': 'evidenced omission: ' + author['email_omission_reason']})
            continue
        checks = [preflight(item['email'], author['full_name'], root) for item in author['emails']]
        reasons = stop_reasons(author, checks)
        if reasons:
            blocked.append({'full_name': author['full_name'],
                            'emails': [check['email'] for check in checks],
                            'reason': '；'.join(reasons)})
            continue
        for check in checks:
            address = email(check['email'])
            person = next((p for p in persons if address in p['emails']), None)
            linked = {rid for r in person['records'] for rid in r.get('problem_ids', [])} if person else set()
            missing = sorted(set(author['problem_ids']) - linked)
            if not person or missing:
                args = ['python3', '-m', 'managed.registry', 'register' if not person else 'link-problem',
                        '--email', address]
                if not person: args += ['--name', author['full_name']]
                for rid in missing: args += ['--problem-id', rid]
                args += ['--evidence', author['source_url'], '--title', author['title'],
                         '--title-evidence', author['title_evidence_url'], '--affiliation', author['affiliation']]
                addresses.append({'email':address, 'missing_problem_ids':missing,
                                  'preflight':check,
                                  'command':shlex.join(args)})
        if addresses:
            proposals.append({'proposal':author, 'addresses':addresses})
    if not proposals and not blocked: return None
    result = {'task_id':row['id'], 'result_hash':digest(value['result']), 'proposals':proposals,
              'reviewed_proposal':value,
            'next_action':'Save reviewed_proposal unchanged to a local JSON file; run '
                          'python3 -m managed.registry apply-author-contacts <reviewed-proposal.json>'}
    if blocked:
        result['blocked'] = blocked
    return result


def blocked(value, root):
    """Return authors with a permanent boundary or an evidenced email omission.

    This is used when replaying an old work snapshot: a formerly actionable
    proposal must not become a recurring registry exception after a refusal or
    region exclusion has been recorded. Existing threads can still gain links.
    """
    from .registry import preflight
    result = value.get('result', {}) if isinstance(value, dict) else {}
    authors = result.get('author_contacts', []) if isinstance(result, dict) else []
    found = []
    for author in authors:
        addresses = author.get('emails', [])
        if not addresses and nonempty(author.get('email_omission_reason')):
            found.append({'full_name': author.get('full_name', ''), 'emails': [],
                          'reason': 'evidenced omission: ' + author['email_omission_reason']})
            continue
        checks = [preflight(item['email'], author.get('full_name', ''), root) for item in addresses]
        reasons = stop_reasons(author, checks)
        if reasons:
            found.append({'full_name': author.get('full_name', ''),
                          'emails': [check['email'] for check in checks],
                          'reason': '；'.join(reasons)})
    return found


def apply(value, root, secret, render_views=True):
    from .registry import preflight, register, link_problem, render, read, write
    authors = reviewed(value, secret)
    # Check every address against current table 3/history before any mutation.
    checks = [(author, [preflight(item['email'], author['full_name'], root) for item in author['emails']])
              for author in authors]
    applied, linked_existing, skipped = [], [], []
    for author, addresses in checks:
        # All emails in one source-backed author entry are verified aliases.
        # A stop on one address must not be bypassed through another. History
        # permits linking only through a registered address, never a new row.
        reasons = sorted({reason for check in addresses for reason in check['reasons']
                          if reason != 'undeliverable-address'})
        if author['in_refusals']:
            reasons.append('refused-at-reviewed-pinned-revision; operator reconciliation required')
        existing = (bool(reasons) and set(reasons) == {'already-contacted-use-the-existing-thread'}
                    and any(c['registered'] for c in addresses))
        if reasons and not existing:
            for check in addresses:
                skipped.append({'email':check['email'], 'reasons':reasons,
                                'refusal_ids':sorted({rid for c in addresses for rid in c['refusal_ids']})})
            continue
        targets = ([next(c for c in addresses if c['registered'])] if existing
                   else sorted(addresses, key=lambda c: not c['registered']))
        for check in targets:
            kwargs = dict(root=root,
                           problem_ids=author['problem_ids'], evidence=author['source_url'],
                           title=author['title'], title_evidence=author['title_evidence_url'],
                           affiliation=author['affiliation'])
            row = (link_problem(check['email'], **kwargs) if existing
                   else register(check['email'], author['full_name'], **kwargs))
            data = read(root, 'contacts')
            current = next(r for r in data['contacts'] if r['id'] == row['id'])
            sources = current.setdefault('author_contact_sources', [])
            if author not in sources: sources.append(author)
            # Legacy rows may carry an explicit null relationship.
            rel = current.get('relationship') or {}
            current['relationship'] = rel
            rel['aliases'] = sorted(set(rel.get('aliases') or []) | {c['email'] for c in addresses})
            write(root, 'contacts', data)
            (linked_existing if existing else applied).append(
                {'email':check['email'], 'contact_id':row['id'], 'problem_ids':row['problem_ids']})
    if render_views: render(root)
    return {'registered_or_linked':applied, 'linked_existing':linked_existing, 'skipped':skipped,
            'unreachable':[a for a in authors if not a['emails']],
            'author_contacts_reason':value['result'].get('author_contacts_reason'),
            'enqueued':False, 'sent':False}
