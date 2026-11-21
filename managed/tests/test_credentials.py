import base64
import contextlib
import io
import json
import os
import time
import unittest
from unittest.mock import patch

from fixture_config import test_config
from fixture_config import use_example_config
from managed import credentials as c


def token(kind, expires=3600, owner=test_config()['credentials']['nyx_owner']):
    value = {'exp': time.time() + expires, 'iss': test_config()['credentials']['nyx_base_url'], 'sub': owner, 'token_type': kind}
    encoded = base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip('=')
    return 'test.' + encoded + '.test'


def session(expires=3600):
    return {'schema_version': 1, 'access_token': token('access', expires),
            'refresh_token': token('refresh', 86400), 'generation': 1}


class CredentialTests(unittest.TestCase):
    def test_status_reports_refresh_expiry_and_warns_at_seven_days(self):
        now=time.time()
        for remaining in (2*86400+1,2*86400,1,-1):
            with self.subTest(remaining=remaining), patch.object(c.time,'time',return_value=now):
                value=session(28000);value['refresh_token']=token('refresh',remaining)
                result=c.session_status(value,now=lambda:now)
                self.assertEqual(result['refresh_expires_at'],now+remaining)
                self.assertEqual(result['access_expires_at'],now+28000)
                self.assertEqual(result['generation'],1)
                self.assertEqual(bool(result['refresh_expiry_warning']),remaining<=2*86400)
                self.assertNotIn(value['access_token'],json.dumps(result))
                self.assertNotIn(value['refresh_token'],json.dumps(result))

    def test_hosted_status_uses_rotated_refresh_expiry_without_tokens(self):
        original=session();rotated=session(28000)
        rotated['refresh_token']=token('refresh',2*86400);rotated['generation']=2
        output=io.StringIO()
        with patch.dict(os.environ,{'OUTREACH_NYX_SESSION':json.dumps(original)},clear=True), \
                patch.object(c,'refresh_session',return_value=rotated), \
                patch.object(c,'export_env') as export, \
                patch.object(c,'request') as request, contextlib.redirect_stdout(output):
            c.main(['hosted'])
        result=json.loads(output.getvalue())
        self.assertEqual(result,{'nyx_session':'ready',**c.session_status(rotated)})
        self.assertIn('reconnect',result['refresh_expiry_warning'])
        for value in (original,rotated):
            for name in ('access_token','refresh_token'):self.assertNotIn(value[name],output.getvalue())
        export.assert_called_once_with('CMA_ACCESS_TOKEN',rotated['access_token'])
        request.assert_not_called()

    def test_expired_access_renews_with_live_refresh_before_use(self):
        events = []
        def request(url, *args, **kwargs):
            if url.endswith('/refresh'):
                events.append('rotate')
                return {'access_token': token('access', 28000), 'refresh_token': token('refresh', 86400)}
            events.append('verify-owner')
            return {'id': test_config()['credentials']['nyx_owner']}
        with patch.object(c, 'request', request):
            result = c.refresh_session(session(-100), lambda s: events.append('save'))
        self.assertEqual(events, ['save', 'rotate', 'save', 'verify-owner'])
        self.assertEqual(result['generation'], 2)

    def test_live_access_does_not_rotate_on_every_tick(self):
        with patch.object(c, 'request', return_value={'id': test_config()['credentials']['nyx_owner']}) as request:
            result = c.refresh_session(session(28000), lambda s: self.fail('unexpected rotation'))
        self.assertEqual(request.call_count, 1)
        self.assertEqual(result['generation'], 1)

    def test_uncertain_refresh_recovers_in_same_attempt(self):
        renewed = {'access_token': token('access', 28000), 'refresh_token': token('refresh', 86400)}
        saved = []
        with patch.object(c, 'request', side_effect=[TimeoutError(), renewed, {'id': test_config()['credentials']['nyx_owner']}]):
            c.refresh_session(session(), saved.append)
        self.assertEqual(len(saved), 2)
        self.assertTrue(saved[0]['rotation_pending'])
        self.assertNotIn('rotation_pending', saved[1])

    def test_failed_persistence_never_releases_a_rotated_token(self):
        renewed = {'access_token': token('access', 28000), 'refresh_token': token('refresh', 86400)}
        saved = []
        def save(value):
            if value.get('rotation_pending'):
                saved.append(value)
            else:
                raise TimeoutError()
        with patch.object(c, 'request', return_value=renewed), patch.object(c, 'mask'):
            with self.assertRaisesRegex(RuntimeError, 'could not be preserved'):
                c.refresh_session(session(), save)
        with patch.object(c, 'request') as request:
            with self.assertRaisesRegex(ValueError, 'Previous rotation'):
                c.refresh_session(saved[0], save)
        request.assert_not_called()

    def test_fence_failure_prevents_refresh(self):
        with patch.object(c, 'request') as request:
            with self.assertRaisesRegex(RuntimeError, 'no refresh was attempted'):
                c.refresh_session(session(), lambda s: (_ for _ in ()).throw(TimeoutError()))
        request.assert_not_called()

    def test_uncertain_refresh_blocks_future_jobs_from_replaying_old_token(self):
        saved = []
        with patch.object(c, 'request', side_effect=TimeoutError()):
            with self.assertRaisesRegex(ValueError, 'uncertain'):
                c.refresh_session(session(), saved.append)
        with patch.object(c, 'request') as request:
            with self.assertRaisesRegex(ValueError, 'Previous rotation'):
                c.refresh_session(saved[-1], saved.append)
        request.assert_not_called()

    def test_wrong_owner_refused_before_network(self):
        value = session()
        value['refresh_token'] = token('refresh', owner='other-owner')
        with patch.object(c, 'request') as request:
            with self.assertRaises(ValueError):
                c.refresh_session(value, lambda s: None)
        request.assert_not_called()

    def test_expired_refresh_requires_sign_in(self):
        value = session()
        value['refresh_token'] = token('refresh', -1)
        with self.assertRaisesRegex(ValueError, 'expired'):
            c.validate_session(value)

    def test_github_connection_is_selected_explicitly(self):
        with patch.object(c, 'request', return_value={}) as request:
            c.SecretStore('test-token').github('repos/test/example/actions/secrets/public-key')
        url = request.call_args.args[0]
        self.assertIn('/proxy/s/api-github-pat/', url)
        self.assertTrue(url.endswith('_nyxid_via=' + test_config()['github']['user_service_id']))

    def test_only_known_secret_slots_allowed(self):
        with patch.object(c, 'request') as request:
            with self.assertRaises(ValueError):
                c.SecretStore('test-token').save('UNRELATED_SECRET', 'test')
        request.assert_not_called()


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
