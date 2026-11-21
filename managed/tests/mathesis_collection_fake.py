"""Immutable Mathesis collection contract fixture; no venue review endpoints."""
import copy
import hashlib
import json
from pathlib import Path
from urllib.error import HTTPError


class CollectionClient:
    def __init__(self):
        self.posts = 0
        self.versions = {}
        self.lose_response = False
        self.fail_status = False
        self.corrupt = None
        self.before_post = lambda: None

    def verify_owner(self):
        return 'verified-owner'

    def deliver(self, *args):
        raise AssertionError('Shortnotes must never use manuscript-deliveries')

    def deliver_pre_reviewed(self, metadata, pdf, source):
        self.before_post()
        self.posts += 1
        key = (metadata['work_id'], metadata['version'])
        files = {'pdf': Path(pdf).read_bytes(), 'source': Path(source).read_bytes()}
        for kind, raw in files.items():
            assert hashlib.sha256(raw).hexdigest() == metadata['review'][kind + '_sha256']
        value = (copy.deepcopy(metadata), files, (Path(pdf).name, Path(source).name))
        if key in self.versions:
            if self.versions[key] != value:
                raise HTTPError('fixture', 409, 'Immutable version', {}, None)
        else:
            previous = max((version for work, version in self.versions if work == key[0]), default=None)
            if metadata['previous_version'] != previous:
                raise HTTPError('fixture', 409, 'Wrong predecessor', {}, None)
            self.versions[key] = value
        if self.lose_response:
            raise TimeoutError('Accepted; response lost')
        return {'schema': 'mathesis.pre-reviewed-receipt.v1', 'status': 'collected',
                'work_id': key[0], 'version': key[1], 'public_path': f'/papers/{key[0]}?version={key[1]}',
                **{kind + '_sha256': hashlib.sha256(raw).hexdigest() for kind, raw in files.items()}}

    def collected(self, work=None, version=None, **search):
        if self.fail_status:
            raise TimeoutError('Readback unavailable')
        if version is None:
            version = max(v for w, v in self.versions if w == work)
        metadata = self.versions[work, version][0]
        return {'id': work, 'revision': str(version),
                'previous_version': str(metadata['previous_version']) if metadata['previous_version'] else None,
                **copy.deepcopy(metadata['paper']),
                **{kind: {'url': f'/api/v1/collected-papers/{work}/files/{kind}?version={version}'}
                   for kind in ('pdf', 'source')}}

    def collected_file(self, work, version, kind):
        return b'changed' if self.corrupt == kind else self.versions[work, version][1][kind]

    def public(self, path):
        for work, version in self.versions:
            endpoint = f'/api/v1/collected-papers/{work}'
            if path == endpoint + f'?version={version}':
                return json.dumps(self.collected(work, version)).encode()
            for kind in ('pdf', 'source'):
                if path == endpoint + f'/files/{kind}?version={version}':
                    return self.collected_file(work, version, kind)
        raise AssertionError('Unexpected public collection read: ' + path)
