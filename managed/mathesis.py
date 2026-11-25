"""Consume Mathesis's existing versioned manuscript delivery API.

Preparation never asserts an author's consent. Live delivery requires an exact
decision record; test delivery is confined to private synthetic manuscripts.
"""
from . import recording

import argparse
import base64
import hashlib
import json
import os
import re
import uuid
from pathlib import Path
from urllib.parse import urlencode, quote
from urllib.request import Request, build_opener

from .cma import NoRedirect
from .queue import canonical, digest, require

from .location import settings


class Mathesis:
    def __init__(self, token, base=None):
        configured = settings('mathesis')['base_url']
        base = base or configured
        require(base == configured and base.startswith('https://'), 'Use the verified Mathesis deployment')
        require(bool(token), 'A supported ordinary submitting identity is required')
        self.base, self.token = base, token

    def request(self, path, method='GET', body=None):
        require(path.startswith('/api/v1/') and '..' not in path, 'Use the existing Mathesis API')
        headers = {'Authorization': 'Bearer ' + self.token, 'User-Agent': 'trureturing-outreach/mathesis'}
        raw = None
        if body is not None:
            headers['Content-Type'] = 'application/json'
            raw = canonical(body).encode()
        with recording.open('mathesis', build_opener(NoRedirect()), Request(self.base + path, data=raw, method=method, headers=headers), timeout=60) as response:
            content = response.read()
        return json.loads(content) if content else {}

    def verify_owner(self):
        me = self.request('/api/v1/me')
        require(me.get('id') == settings("credentials")["nyx_owner"], 'Mathesis authenticated another submitting account')
        return me['id']

    def find(self, metadata):
        fields = {key: metadata[key] for key in ('producer', 'work_id')}
        fields['purpose'] = metadata['paper']['purpose']
        return self.request('/api/v1/manuscript-deliveries?' + urlencode(fields))

    def status(self, submission, version=None):
        path = '/api/v1/submissions/' + quote(submission, safe='') + '/delivery'
        return self.request(path + ('?' + urlencode({'version': version}) if version else ''))

    def withdraw(self, submission):
        """Withdraw an ordinary submission owned by this submitting identity."""
        path = '/api/v1/submissions/' + quote(submission, safe='') + '/withdraw'
        return self.request(path, 'POST')

    def deliver(self, metadata, source):
        source = Path(source)
        raw = source.read_bytes()
        require(0 < len(raw) <= 30 * 1024 * 1024, 'Mathesis accepts at most 30 MiB of source')
        self.verify_owner()
        receipt = self.request('/api/v1/manuscript-deliveries', 'POST',
                               {'metadata': metadata, 'filename': source.name,
                                'source_base64': base64.b64encode(raw).decode()})
        require(receipt.get('schema') == 'mathesis.manuscript-receipt.v1' and
                receipt.get('producer') == metadata['producer'] and
                receipt.get('work_id') == metadata['work_id'] and
                receipt.get('upstream_version') == metadata['version'] and
                receipt.get('source_sha256') == hashlib.sha256(raw).hexdigest(),
                'Mathesis receipt does not match the delivered source identity')
        if metadata['paper']['purpose'] == 'review_only':
            require(receipt.get('publication') is None, 'A private review unexpectedly has a publication')
        return receipt

    def confirm(self, receipt, claims):
        require(receipt.get('current') is True, 'A historical delivery cannot confirm the current source')
        require(isinstance(claims, list) and claims, 'Supply independently checked exact statement confirmations')
        payload = {key: receipt[key] for key in ('version', 'source_sha256', 'expected_claims_revision')}
        payload['claims'] = claims
        return self.request('/api/v1/submissions/' + quote(receipt['submission'], safe='') +
                            '/delivery-confirmation', 'POST', payload)

    def collected(self, work_id=None, version=None, **search):
        """Read the public collection projection (which deliberately omits hashes)."""
        path = '/api/v1/collected-papers'
        if work_id is not None:
            require(re.fullmatch(r'[a-z0-9-]{1,160}', work_id), 'Invalid collected work slug')
            path += '/' + work_id
        query = {'version': version} if version is not None else search
        return self.request(path + ('?' + urlencode(query) if query else ''))

    def collected_file(self, work_id, version, kind):
        require(re.fullmatch(r'[a-z0-9-]{1,160}', work_id)
                and type(version) is int and version > 0 and kind in {'pdf', 'source'},
                'Read an exact collected file version')
        path = f'/api/v1/collected-papers/{work_id}/files/{kind}?version={version}'
        with recording.open('mathesis', build_opener(NoRedirect()), Request(self.base + path,
                headers={'User-Agent': 'trureturing-outreach/mathesis'}), timeout=60) as response:
            raw = response.read(30 * 1024 * 1024 + 1)
        require(0 < len(raw) <= 30 * 1024 * 1024, 'Invalid collected file size')
        return raw

    def deliver_pre_reviewed(self, metadata, pdf, source):
        """Replay the same immutable envelope/filenames after uncertain POSTs.

        Mathesis returns the same receipt on an identical retry, including a
        historical version. A 409 is never converted into a replacement version.
        """
        boundary = 'outreach-' + uuid.uuid4().hex
        parts = []
        parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="metadata"\r\n'
                      'Content-Type: application/json\r\n\r\n').encode() + canonical(metadata).encode() + b'\r\n')
        for kind, path, magic, extension, mime in (
                ('pdf', Path(pdf), b'%PDF-', '.pdf', 'application/pdf'),
                ('source', Path(source), b'PK\x03\x04', '.zip', 'application/zip')):
            raw = path.read_bytes()
            require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,199}', path.name)
                    and path.suffix.lower() == extension and raw.startswith(magic)
                    and 0 < len(raw) <= 30 * 1024 * 1024
                    and hashlib.sha256(raw).hexdigest() == metadata['review'][kind + '_sha256'],
                    'Pre-reviewed upload differs from the exact PASS file')
            parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="{kind}"; '
                          f'filename="{path.name}"\r\nContent-Type: {mime}\r\n\r\n').encode() + raw + b'\r\n')
        parts.append(f'--{boundary}--\r\n'.encode())
        self.verify_owner()
        headers = {'Authorization': 'Bearer ' + self.token,
                   'User-Agent': 'trureturing-outreach/mathesis',
                   'Content-Type': 'multipart/form-data; boundary=' + boundary}
        with recording.open('mathesis', build_opener(NoRedirect()), Request(self.base + '/api/v1/pre-reviewed-deliveries',
                data=b''.join(parts), headers=headers, method='POST'), timeout=60) as response:
            receipt = json.loads(response.read())
        return collection_receipt(receipt, metadata)


def collection_receipt(receipt, metadata):
    require(receipt.get('schema') == 'mathesis.pre-reviewed-receipt.v1'
            and receipt.get('status') == 'collected'
            and receipt.get('work_id') == metadata['work_id']
            and type(receipt.get('version')) is int and receipt['version'] == metadata['version']
            and all(receipt.get(k) == metadata['review'][k] for k in ('pdf_sha256', 'source_sha256')),
            'Mathesis receipt differs from the exact reviewed PDF/source version')
    return receipt


def plan(directory, decisions):
    directory = Path(directory)
    handoff = json.loads((directory / 'handoff.json').read_text())
    require(handoff.get('schema_version') == 'outreach-mathesis-handoff.v1' and
            handoff.get('private_correspondence_included') is False, 'Select a scientific-only Outreach handoff')
    note = handoff['source_note']
    require(decisions.get('decision_ref') and decisions.get('processing_consent') and decisions.get('ai_disclosure'),
            'Record actual author choices, processing consent and disclosure before delivery')
    require(decisions.get('purpose') in {'publication', 'review_only'}, 'Choose the actual submission purpose')
    for name, expected in handoff['files'].items():
        require(Path(name).name == name and hashlib.sha256((directory / name).read_bytes()).hexdigest() == expected,
                'Handoff source differs from its preserved revision')
    source = decisions['source_file']
    require(source in handoff['files'] and source.endswith(('.zip', '.tar.gz', '.tex', '.pdf')),
            'Select the existing scientific source archive or manuscript')
    authors = [{k: value for k, value in author.items() if k in {'name', 'affiliation', 'orcid'}}
               for author in note['creators']]
    metadata = {'schema': 'mathesis.manuscript-delivery.v1', 'producer': 'trureturing-outreach',
                'work_id': note['id'], 'version': str(note['revision']),
                'previous_version': decisions.get('previous_version'),
                'paper': {'kind': 'note', 'purpose': decisions['purpose'], 'title': note['title'],
                          'abstract_text': note['abstract'], 'authors': authors,
                          'processing_consent': decisions['processing_consent'],
                          'ai_disclosure': decisions['ai_disclosure'],
                          'make_public_after_acceptance': decisions.get('make_public_after_acceptance', False),
                          'open_to_contributors': False},
                'origin': {'problem_ids': note['problem_ids'], 'source_url': note['evidence_url'],
                           'license': note['license'], 'evidence': decisions.get('evidence', [])}}
    if decisions['purpose'] == 'review_only':
        metadata['paper']['make_public_after_acceptance'] = False
    if decisions.get('doi'):
        metadata['paper']['doi'] = decisions['doi']
    return {'metadata': metadata, 'source_file': source,
            'source_sha256': hashlib.sha256((directory / source).read_bytes()).hexdigest(),
            'decision_ref': decisions['decision_ref']}


def live_delivery(client, metadata, source, state_path, test_only=False):
    if test_only:
        require(metadata.get('producer') == 'outreach-infrastructure-test' and
                metadata.get('work_id', '').startswith('test-') and
                metadata['paper']['purpose'] == 'review_only' and
                metadata['paper'].get('make_public_after_acceptance') is False and
                not metadata['paper'].get('open_to_contributors'), 'Test delivery must remain a private synthetic manuscript')
    path = Path(state_path)
    binding = digest({'metadata': metadata, 'filename': Path(source).name,
                      'source_sha256': hashlib.sha256(Path(source).read_bytes()).hexdigest()})
    state = json.loads(path.read_text()) if path.exists() else {'request_hash': binding, 'phase': 'delivery-pending'}
    require(state['request_hash'] == binding, 'Do not mutate a previously attempted delivery')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2) + '\n')
    # Mathesis explicitly makes exact producer/work/version retries safe. Preserve
    # that identity and source even after a lost response; never invent another ID.
    receipt = client.deliver(metadata, source)
    state.update(phase='delivered', receipt=receipt)
    path.write_text(json.dumps(state, indent=2) + '\n')
    return receipt


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['doctor', 'plan', 'test-deliver', 'status'])
    p.add_argument('--directory', type=Path)
    p.add_argument('--decisions', type=Path)
    p.add_argument('--metadata', type=Path)
    p.add_argument('--source', type=Path)
    p.add_argument('--state', type=Path)
    p.add_argument('--submission')
    p.add_argument('--output', type=Path)
    args = p.parse_args()
    if args.action == 'plan':
        result = plan(args.directory, json.loads(args.decisions.read_text()))
    else:
        token = os.environ.get('MATHESIS_ACCESS_TOKEN') or os.environ.get('CMA_ACCESS_TOKEN')
        require(token, 'Supply the supported submitting session through the environment')
        client = Mathesis(token)
        if args.action == 'doctor':
            result = {'identity_verified': bool(client.verify_owner()), 'policy': client.request('/api/v1/policy')}
        elif args.action == 'status':
            result = client.status(args.submission)
        else:
            result = live_delivery(client, json.loads(args.metadata.read_text()), args.source, args.state, test_only=True)
    if args.output:
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
