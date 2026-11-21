"""Credential custody for the single Outreach coordinator.

An environment secret is read at job start, after the workflow concurrency lock.
Rotated pairs are encrypted and saved before the coordinator can use them.
Nothing in this module enables production or sends mail.
"""
import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener

from .cma import NoRedirect
from .queue import require

from .location import ENGINE_REPO, settings
from .run_log import register_secret

ENVIRONMENT = 'outreach-runtime'
SCOPES = ['User.Read', 'Mail.ReadWrite', 'Mail.Send']
ORACLE_STATUS_PATH = '/api/v1/oracle/pools/chrono-chatgpt-pro-pool/status'


def claims(token, allow_expired=False):
    try:
        value = json.loads(base64.urlsafe_b64decode(token.split('.')[1] + '==='))
    except (ValueError, IndexError, KeyError):
        raise ValueError('Credential has no readable expiry') from None
    require(isinstance(value.get('exp'), (int, float)), 'Credential has no expiry')
    require(allow_expired or value['exp'] > time.time(), 'Credential has expired; sign in again')
    return value


TRANSIENT_STATUS = {502, 503, 504}
RETRY_ATTEMPTS = 3
RETRY_DELAYS = (2, 5)


def mask(value):
    register_secret(value)


def request(url, method='GET', body=None, token=None):
    require(url.startswith(settings('credentials')['nyx_base_url'] + '/'), 'Credential operations are limited to the NyxID origin')
    headers = {'User-Agent': 'trureturing-outreach/credentials'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    raw = None
    if body is not None:
        raw = json.dumps(body).encode()
        headers['Content-Type'] = 'application/json'
    # Idempotent reads and secret writes retry transient gateway errors
    # (2026-11-15: two coordinator ticks failed on a single NyxID 502/503).
    # Session refresh (POST) is never repeated: it may rotate the token.
    attempts = RETRY_ATTEMPTS if method in {'GET', 'PUT'} else 1
    for attempt in range(attempts):
        try:
            with build_opener(NoRedirect()).open(Request(url, data=raw, method=method, headers=headers),
                                                 timeout=30) as response:
                data = response.read()
                return json.loads(data) if data.strip() else {}
        except HTTPError as exc:
            if exc.code not in TRANSIENT_STATUS or attempt == attempts - 1:
                raise
        except URLError:
            if attempt == attempts - 1:
                raise
        time.sleep(RETRY_DELAYS[attempt])


def oracle_pool_status(fetcher=None):
    """Read the broker pool through the NyxID proxy when hosted custody exists.

    ``fetcher`` is injectable for offline tests.  The coordinator deliberately
    has no fallback to a local CLI or an unscoped token: Actions exports the
    renewed NyxID access token as ``CMA_ACCESS_TOKEN`` before the tick.
    """
    if fetcher is not None:
        return fetcher()
    token = os.environ.get('CMA_ACCESS_TOKEN')
    if not token:
        return None
    return request(settings('credentials')['nyx_base_url'] + '/api/v1/proxy/s/oracle' + ORACLE_STATUS_PATH, token=token)


class SecretStore:
    def __init__(self, access_token):
        self.access_token = access_token

    def github(self, path, method='GET', body=None):
        separator = '&' if '?' in path else '?'
        github = settings('github')
        return request(settings('credentials')['nyx_base_url'] + '/api/v1/proxy/s/' + github['service']
                       + '/' + path + separator + '_nyxid_via=' + github['user_service_id'],
                       method, body, self.access_token)

    def save(self, name, value):
        require(name in {'OUTREACH_NYX_SESSION', 'OUTREACH_GRAPH_CACHE'}, 'Unknown credential slot')
        from nacl.public import PublicKey, SealedBox
        from nacl.encoding import Base64Encoder
        prefix = 'repos/' + ENGINE_REPO + '/environments/' + ENVIRONMENT + '/secrets/'
        key = self.github(prefix + 'public-key')
        sealed = SealedBox(PublicKey(key['key'].encode(), Base64Encoder)).encrypt(value.encode())
        self.github(prefix + name, 'PUT', {'key_id': key['key_id'],
                                         'encrypted_value': base64.b64encode(sealed).decode()})


def validate_session(session):
    require(session.get('schema_version') == 1, 'Unsupported session record')
    require(not session.get('rotation_pending'),
            'Previous rotation did not finish custody; reconnect the dedicated session before retrying')
    access = claims(session['access_token'], allow_expired=True)
    refresh = claims(session['refresh_token'])
    config = settings('credentials')
    issuers = {config['nyx_base_url'], config['nyx_legacy_issuer']}
    require(access.get('iss') in issuers and refresh.get('iss') in issuers and
            access.get('sub') == config['nyx_owner'] and refresh.get('sub') == config['nyx_owner'],
            'Session identity/issuer differs from the dedicated Outreach owner')
    require(access.get('token_type') == 'access' and refresh.get('token_type') == 'refresh',
            'CMA requires an ordinary user session')
    return access


# The refresh JWT slides about 7 days ahead on each rotation, so a 7-day
# threshold would always warn. Two days left means rotation has stalled.
WARN_SECONDS = 2 * 86400


def session_status(session, now=time.time):
    """Expose expiry timestamps only, never the session or token claims."""
    refresh_expiry = claims(session['refresh_token'], allow_expired=True)['exp']
    return {'generation': session.get('generation', 0),
            'access_expires_at': claims(session['access_token'], allow_expired=True)['exp'],
            'refresh_expires_at': refresh_expiry,
            'refresh_expiry_warning': (
                'NyxID refresh credential expires within 2 days or has expired; rotation may have stopped; '
                'reconnect the dedicated session before expiry. Rolling access renewal '
                'does not guarantee extension of the absolute session deadline.'
                if refresh_expiry - now() <= WARN_SECONDS else None)}


def refresh_session(session, save, force=False, now=time.time):
    config = settings('credentials')
    access = validate_session(session)
    current = dict(session)
    if force or access['exp'] - now() < 3 * 3600:
        # Persist the fence before touching the single-use refresh token. If this
        # process stops or cannot save the new pair, future scheduled jobs must
        # never replay the old token outside NyxID's recovery window.
        fenced = {**current, 'rotation_pending': True}
        for attempt in range(3):
            try:
                save(fenced)
                break
            except Exception:
                if attempt == 2:
                    raise RuntimeError('Could not preserve the rotation fence; no refresh was attempted') from None
        # A retry of this exact old refresh token is supported by NyxID only
        # within its 120-second recovery window. Never defer a failed rotation.
        rotated = None
        for _ in range(3):
            try:
                rotated = request(config['nyx_base_url'] + '/api/v1/auth/refresh', 'POST',
                                  {'refresh_token': current['refresh_token']})
                break
            except Exception:
                pass
        require(rotated is not None, 'Session refresh outcome is uncertain; reconnect dedicated custody before retrying later')
        current.update(access_token=rotated['access_token'], refresh_token=rotated['refresh_token'],
                       generation=current.get('generation', 0) + 1)
        mask(current['access_token']); mask(current['refresh_token'])
        validate_session(current)
        error = None
        for _ in range(3):
            try:
                save(current)
                error = None
                break
            except Exception as exc:
                error = exc
        if error:
            raise RuntimeError('Rotated session could not be preserved; stop and reconnect dedicated custody') from None
    me = request(config['nyx_base_url'] + '/api/v1/users/me', token=current['access_token'])
    require(me.get('id') == config['nyx_owner'], 'Provider did not verify the expected session owner')
    return current


def local_graph_cache():
    import msal_extensions
    folder = Path.home() / '.config/trureturing-outreach/graph'
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    persistence = msal_extensions.KeychainPersistence(str(folder / 'cache-signal'),
                                                     service_name='trureturing-outreach-graph',
                                                     account_name=settings('mailbox')['account'])
    return msal_extensions.PersistedTokenCache(persistence)


def graph_access(cache, interactive=False, force_refresh=False):
    import msal
    config = settings('credentials')
    address = settings('mailbox')['account'].casefold()
    app = msal.PublicClientApplication(config['graph_client'],
                                      authority='https://login.microsoftonline.com/' + config['graph_tenant'],
                                      token_cache=cache)
    accounts = app.get_accounts(username=address)
    result = app.acquire_token_silent(SCOPES, account=accounts[0], force_refresh=force_refresh) if accounts else None
    if not result and interactive:
        result = app.acquire_token_interactive(SCOPES, login_hint=address, timeout=300)
    require(result is not None and 'access_token' in result,
            'Microsoft requires interactive sign-in; no mailbox operation was attempted')
    token = result['access_token']
    mask(token)
    from .mailbox import Graph
    me = Graph(token).request('/me?$select=mail,userPrincipalName')
    require(address in {str(me.get(k) or '').lower() for k in ('mail', 'userPrincipalName')},
            'Microsoft authenticated a different mailbox')
    return token


def export_env(name, value):
    require(name in {'CMA_ACCESS_TOKEN', 'OUTREACH_GRAPH_ACCESS_TOKEN'}, 'Unexpected runtime variable')
    require('\n' not in value and '\r' not in value, 'Invalid credential encoding')
    path = os.environ.get('OUTREACH_CREDENTIAL_FILE')
    require(bool(path), 'Hosted credentials require a private runner credential file')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, 'a') as stream:
        stream.write(name + '=' + value + '\n')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['hosted', 'bootstrap-nyx', 'login-graph', 'sync-graph'])
    parser.add_argument('--force-refresh', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    if args.action == 'bootstrap-nyx':
        profile = Path.home() / '.nyxid/profiles/outreach-actions'
        require(not (profile / 'remote-custody.json').exists(), 'Dedicated session already has hosted custody')
        session = {'schema_version': 1, 'generation': 0,
                   'access_token': (profile / 'access_token').read_text().strip(),
                   'refresh_token': (profile / 'refresh_token').read_text().strip()}
        validate_session(session)
        SecretStore(session['access_token']).save('OUTREACH_NYX_SESSION', json.dumps(session))
        # Avoid later use of a stale local refresh token revoking the hosted session.
        (profile / 'refresh_token').unlink()
        (profile / 'remote-custody.json').write_text(json.dumps({'environment': ENVIRONMENT,
                                                              'transferred_at': datetime.now(timezone.utc).isoformat()}))
        result = {'nyx_session': 'preserved-in-environment', **session_status(session)}
    elif args.action in {'login-graph', 'sync-graph'}:
        cache = local_graph_cache()
        token = graph_access(cache, args.action == 'login-graph')
        if args.action == 'sync-graph':
            profile = Path.home() / '.nyxid/profiles/outreach-actions'
            SecretStore((profile / 'access_token').read_text().strip()).save(
                'OUTREACH_GRAPH_CACHE', json.dumps(json.loads(cache.serialize()), separators=(',', ':')))
        result = {'graph': 'authenticated', 'cache': 'macOS Keychain', 'exported': args.action == 'sync-graph'}
    else:
        session = json.loads(os.environ['OUTREACH_NYX_SESSION'])
        for name in ('access_token', 'refresh_token'):
            mask(session[name])
        session = refresh_session(session,
                                  lambda s: SecretStore(s['access_token']).save('OUTREACH_NYX_SESSION', json.dumps(s)),
                                  args.force_refresh)
        export_env('CMA_ACCESS_TOKEN', session['access_token'])
        result = {'nyx_session': 'ready', **session_status(session)}
        graph = os.environ.get('OUTREACH_GRAPH_CACHE')
        if graph:
            import msal
            cache = msal.SerializableTokenCache()
            cache.deserialize(graph)
            token = graph_access(cache, force_refresh=args.force_refresh)
            if cache.has_state_changed:
                SecretStore(session['access_token']).save(
                    'OUTREACH_GRAPH_CACHE', json.dumps(json.loads(cache.serialize()), separators=(',', ':')))
            export_env('OUTREACH_GRAPH_ACCESS_TOKEN', token)
            result['graph'] = 'ready'
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    from .run_log import public_run
    raise SystemExit(public_run(main, 'credentials'))
