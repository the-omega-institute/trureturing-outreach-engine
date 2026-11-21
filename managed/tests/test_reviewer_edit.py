"""Signed body-only PASS edits through the offline coordinator and delivery fakes."""
from contextlib import contextmanager
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from fixture_config import use_example_config
from fixture_config import use_cli_config
from managed import automation, control, delivery, registry
from managed.queue import canonical, digest
from managed.tasks import review_current
from test_automation import Mailbox
from test_packet_artifact import PacketReleases
from test_tasks import CMA, request as task_request


SECRET = 'fixture-review-key'
EDIT = 'Dear Professor Example,\n\nThe calculation gives 5.\n\nBest regards,\nAvery and Blaire\n'
FIRST_BODY = (
    'Dear Professor Example,\n\n'
    'I am Avery Reed at the Example University and Example Research Group. '
    'With Blaire Mercer, I build trureturing (https://github.com/the-omega-institute/trureturing), '
    'an AI orchestration system that connects knowledge (https://www.trureturing.org/). '
    'The result came out of trureturing and was verified in Lean.\n\n'
    'The calculation gives 5.\n\n'
    "We'd welcome corrections, and we'd be glad to look at it together if you're interested.\n\n"
    'More broadly, we are building a connected knowledge graph of mathematics '
    '(our Mathematical Atlas: https://www.trureturing.org/atlas) with an open community, '
    'pairing human intuition with AI for searching, checking, and formalizing, through open mathematics initiatives.\n\n'
    "If you'd like to try the system behind the result above, we recently launched Mathesis "
    '(https://mathesis.example.invalid/). You may submit a short note or a conjecture bothering you, '
    'and our review pipeline maps it against existing formal libraries to suggest further directions and formalization '
    'targets and assigns a DOI to protect your priority and authorship.\n\n'
    'Best regards,\nAvery and Blaire\nExample University; Example Research Group\n'
)


def awaiting(ledger, kind='reply', route='auto', *, packet_artifact=False):
    request = task_request(kind, 'contact-0', kind)
    packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
    packet['cc'] = ['colleague@example.invalid']
    if kind == 'reply':
        request['inputs'] = {'thread_id': 'thread', 'latest_inbound_message_id': '<inbound>', 'user_decision_ever': True}
        packet.update(kind=kind, **request['inputs'])
    elif kind == 'firstcontact':
        request['inputs'] = {k:packet[k] for k in ('to', 'cc')}
        request['inputs']['recipient_emails'] = packet['to'] + packet['cc']
        packet['body'] = FIRST_BODY.replace('The calculation gives 5.', 'The answer is 5.')
    else:
        packet = None
    row = ledger.tasks.add(request)
    result = {'schema_version': 1, 'task_id': row['id'], 'outcome': 'completed', 'report': 'Checked.',
              'packet': packet, 'artifacts': [],
              'routing': {'mail': route, 'questions': [], 'independent_submission': None}}
    if route == 'owner':
        result['owner_brief'] = {'summary_zh': '这封信处理了当前请求。', 'decisions_zh': [], 'cautions_zh': []}
    if kind == 'shortnote':
        result.update(formalization_gaps=[], value_assessment={'verdict': 'substantial', 'reasons': 'Synthetic fixture.'})
    if packet_artifact:
        path = ledger.folder / 'original-packet.json'
        raw = canonical(packet).encode()
        path.write_bytes(raw)
        tag = 'outreach-task-' + row['id']
        ledger.releases.preserve_task(path, tag)
        path.unlink()
        result['artifacts'].append({'release_tag': tag, 'name': path.name, 'role': 'mail-packet',
                                    'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
        ledger.releases.calls.clear()
    return ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result})


def unsigned(ledger, row, **fields):
    return {'binding': ledger.tasks.review_binding(row), 'reviewer': 'local-claude-loop',
            'verdict': 'PASS', 'reason': 'Exact letter checked.', 'evidence_ref': 'fixture:review', **fields}


def ingest(ledger, envelope):
    ledger.releases.assets['review-' + digest(envelope) + '.json'] = canonical(envelope).encode()
    return ledger.ingest(SECRET)


class ReviewerEditTests(unittest.TestCase):
    def setUp(self):
        for patcher in (patch.object(control, 'review_key', return_value=SECRET),
                        patch('managed.daily_local.status', return_value={})):
            patcher.start()
            self.addCleanup(patcher.stop)

    @contextmanager
    def ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tables = root / 'tables'
            tables.mkdir()
            recipients = ['author@example.invalid', 'colleague@example.invalid']
            (tables / 'contacts.json').write_text(canonical({'contacts': [
                {'id': f'contact-{i}', 'name': 'Professor Example', 'email': address,
                 'problem_ids': [], 'hub': {'workspace_paths': ['../mail/person']}}
                for i, address in enumerate(recipients)]}))
            (tables / 'refusals.json').write_text(canonical({'refusals': []}))
            with patch.object(control, 'firstcontact_gate',
                              side_effect=lambda request, _: registry.firstcontact_gate(request, root)):
                ledger = control.Ledger(PacketReleases(), root)
                try:
                    yield ledger
                finally:
                    ledger.tasks.close()

    def test_edit_ingests_routes_and_delivers_exact_packet(self):
        for kind, route in [('reply', 'auto'), ('reply', 'owner'), ('firstcontact', 'auto')]:
            with self.subTest(kind=kind, route=route), self.ledger() as ledger:
                self.check_edit_delivery(ledger, kind, route)

    def check_edit_delivery(self, ledger, kind, route):
        row = awaiting(ledger, kind, route, packet_artifact=route == 'owner')
        ledger.save(force=True)
        prior = row['data']['result']
        body = FIRST_BODY if kind == 'firstcontact' else EDIT
        edit = body.replace('\n', '\r\n').replace('5.\r\n', '5.\r', 1)
        envelope = control.sign_review(unsigned(ledger, row, edited_body=edit), SECRET, kind=kind)
        with patch.object(ledger.releases, 'ensure', return_value=None, create=True):
            receipt = control.submit_review(envelope, ledger.releases, ledger.folder)
        self.assertIs(receipt['reviewer_edit_valid'], True)
        self.assertEqual(ledger.tasks.get(row['id']), row)
        self.assertEqual(ledger.releases.calls, [])
        self.assertEqual(ledger.ingest(SECRET), [])
        approved = ledger.tasks.get(row['id'])
        self.assertEqual(approved['state'], 'approved')
        self.assertEqual(approved['request'], row['request'])
        data = approved['data']
        result, review = data['result'], data['review']
        old_hash = digest(prior)
        self.assertNotEqual(old_hash, digest(result))
        self.assertEqual(review['binding']['result_hash'], digest(result))
        self.assertTrue(review_current(approved, control.workflow(kind)))
        self.assertEqual(data['review_history'][old_hash], {
            'body': prior['packet']['body'],
            'artifacts': [a for a in prior['artifacts'] if a['role'] == 'mail-packet'],
        })
        event = ledger.tasks.db.execute('SELECT payload FROM control_events WHERE id=?',
                                        (review['signed_review_hash'],)).fetchone()
        self.assertEqual(json.loads(event[0]), envelope)
        packet = {**prior['packet'], 'body': body}
        self.assertEqual(canonical(result['packet']), canonical(packet))
        self.assertEqual(result['routing'], prior['routing'])
        artifact, = result['artifacts']
        self.assertEqual(artifact['sha256'], digest(packet))
        self.assertEqual(ledger.releases.files[artifact['release_tag'], artifact['name']],
                         canonical(packet).encode())
        if prior['artifacts']:
            original, = prior['artifacts']
            self.assertEqual(ledger.releases.files[original['release_tag'], original['name']],
                             canonical(prior['packet']).encode())
        self.assertEqual(ledger.releases.calls, ['preserve', 'readback'])
        self.assertEqual(ingest(ledger, envelope), [])
        self.assertEqual(ledger.tasks.get(row['id']), approved)
        self.assertEqual(ledger.releases.calls, ['preserve', 'readback'])
        marker = automation.work(ledger.tasks, ledger.folder)['affairs_tasks'][0]['reviewer_edit']
        self.assertEqual(marker, {
            'from_result_hash': old_hash,
            'body_sha256_before': hashlib.sha256(prior['packet']['body'].encode()).hexdigest(),
            'body_sha256_after': hashlib.sha256(body.encode()).hexdigest(),
        })

        cma = CMA()
        config = {'paused': False, 'kinds': {kind: True}, 'max_active': 1, 'agent_template': {},
                  'automation': {'automatic_mail': True}}
        with patch.object(control, 'ROOT', ledger.folder):
            control.advance(ledger, cma, {**config, 'paused': True}, ledger.folder)
        expected = 'owner-draft' if route == 'owner' else 'awaiting-mailbox'
        routed = ledger.tasks.get(row['id'])
        self.assertEqual(routed['state'], expected)
        # A later workflow change must identify the current PASS event, even
        # after state transitions and with its original HMAC kept separately.
        with patch.object(control, 'workflow', return_value='c' * 64):
            self.assertEqual(ledger.tasks.review_binding(routed), review['binding'])
        self.assertFalse(cma.creates)
        self.assertFalse(cma.commands)
        mailbox = Mailbox()
        mailbox.preflight = Mock(wraps=mailbox.preflight)
        delivery.advance(ledger, config, ledger.folder, mailbox)
        self.assertEqual(mailbox.preflight.call_args.args[0], packet)
        if route == 'owner':
            self.assertEqual(mailbox.saved[0][0], packet)
            self.assertEqual(ledger.tasks.get(row['id'])['data']['draft_receipt']['content_hash'], digest(packet))
            self.assertFalse(mailbox.calls)
        else:
            self.assertEqual(mailbox.calls[0][0], packet)
            delivery.advance(ledger, config, ledger.folder, mailbox)
            self.assertEqual(ledger.tasks.get(row['id'])['state'], 'verified')
            self.assertFalse(mailbox.saved)

    def test_edit_signature_and_envelope_are_strict(self):
        for change in [{'edited_body': 'Tampered'}, {'to': ['someone@example.invalid']},
                       {'attachments': []}, {'signature': None}]:
            with self.subTest(change=change), self.ledger() as ledger:
                row = awaiting(ledger)
                envelope = control.sign_review(unsigned(ledger, row, edited_body=EDIT), SECRET, kind='reply')
                forged = {**envelope, **change}
                if set(change) & {'to', 'attachments'}:
                    forged.pop('signature')
                    forged = control.sign_review(forged, SECRET, kind='reply')
                self.assertIn('Invalid signed review', ingest(ledger, forged)[0]['error'])
                self.assertEqual(ledger.tasks.get(row['id']), row)
                self.assertEqual(ledger.releases.calls, [])

    def test_only_pass_may_edit(self):
        for verdict in ('BOUNCE', 'HOLD'):
            with self.subTest(verdict=verdict), self.ledger() as ledger:
                row = awaiting(ledger)
                value = unsigned(ledger, row, edited_body=EDIT, verdict=verdict)
                with self.assertRaisesRegex(ValueError, 'Only PASS'):
                    control.sign_review(value, SECRET, kind='reply')
                # Build an authentic signature independently to exercise the ingest check.
                value['signature'] = hmac.digest(SECRET.encode(), canonical(value).encode(), 'sha256').hex()
                self.assertIn('Only PASS', ingest(ledger, value)[0]['error'])
                self.assertEqual(ledger.tasks.get(row['id']), row)

    def test_nonmail_kind_cannot_edit(self):
        for kind in ('shortnote', 'mathesis', 'research'):
            with self.subTest(kind=kind), self.ledger() as ledger:
                row = awaiting(ledger, kind, route='none')
                value = unsigned(ledger, row, edited_body=EDIT)
                with self.assertRaisesRegex(ValueError, 'only allowed for reply and firstcontact'):
                    control.sign_review(value, SECRET, kind=kind)
                envelope = control.sign_review(value, SECRET)
                self.assertIn('only allowed for reply and firstcontact', ingest(ledger, envelope)[0]['error'])
                self.assertEqual(ledger.tasks.get(row['id']), row)

    def test_stale_edit_leaves_the_newer_result_awaiting_review(self):
        with self.ledger() as ledger:
            row = awaiting(ledger)
            envelope = control.sign_review(unsigned(ledger, row, edited_body=EDIT), SECRET, kind='reply')
            result = {**row['data']['result'], 'report': 'A newer agent result.'}
            current = ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result})
            self.assertIn('obsolete result or workflow', ingest(ledger, envelope)[0]['error'])
            self.assertEqual(ledger.tasks.get(row['id']), current)
            self.assertEqual(ledger.releases.calls, [])

    def test_invalid_body_is_rejected_in_preflight_and_ingest(self):
        for kind, body, error in [
            ('firstcontact', FIRST_BODY.replace("if you're interested.", 'if you wish.'), 'First-contact closing'),
            ('firstcontact', FIRST_BODY.replace('Avery Reed', 'Avery'), 'First-contact opening'),
            ('reply', EDIT + 'Example University\n', 'Reply sign-off'),
            ('reply', EDIT.replace('gives 5', 'gives a value — 5'), 'sentence punctuation'),
        ]:
            with self.subTest(kind=kind, error=error), self.ledger() as ledger:
                row = awaiting(ledger, kind)
                ledger.save(force=True)
                envelope = control.sign_review(unsigned(ledger, row, edited_body=body), SECRET, kind=kind)
                with self.assertRaisesRegex(ValueError, error):
                    control.review_dry_run(envelope, ledger.releases)
                self.assertIn(error, ingest(ledger, envelope)[0]['error'])
                self.assertEqual(ledger.tasks.get(row['id']), row)
                self.assertEqual(ledger.releases.calls, [])

    def test_missing_packet_empty_body_and_packet_limit(self):
        with self.ledger() as ledger:
            row = awaiting(ledger)
            with self.assertRaisesRegex(ValueError, 'nonempty string'):
                control.sign_review(unsigned(ledger, row, edited_body=' \r\n'), SECRET, kind='reply')
            envelope = control.sign_review(unsigned(ledger, row, edited_body=EDIT), SECRET, kind='reply')
            with patch.object(control, 'MAIL_PACKET_MAX_BYTES', 50):
                self.assertIn('Edited mail packet exceeds 25 MiB', ingest(ledger, envelope)[0]['error'])
            self.assertEqual(ledger.tasks.get(row['id']), row)
            result = {**row['data']['result'], 'outcome': 'no-reply', 'packet': None}
            row = ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result})
            ledger.releases.assets.clear()
            envelope = control.sign_review(unsigned(ledger, row, edited_body=EDIT), SECRET, kind='reply')
            self.assertIn('completed result with a mail packet', ingest(ledger, envelope)[0]['error'])
            self.assertEqual(ledger.tasks.get(row['id']), row)

    def test_failed_packet_readback_rolls_back_and_can_retry(self):
        with self.ledger() as ledger:
            row = awaiting(ledger)
            envelope = control.sign_review(unsigned(ledger, row, edited_body=EDIT), SECRET, kind='reply')
            ledger.releases.corrupt = True
            self.assertIn('Artifact readback failed', ingest(ledger, envelope)[0]['error'])
            self.assertEqual(ledger.tasks.get(row['id']), row)
            self.assertEqual(ledger.tasks.db.execute('SELECT count(*) FROM control_events').fetchone()[0], 0)
            ledger.releases.corrupt = False
            self.assertEqual(ledger.ingest(SECRET), [])
            self.assertEqual(ledger.tasks.get(row['id'])['state'], 'approved')

    def test_unedited_pass_keeps_the_existing_envelope_and_result(self):
        with self.ledger() as ledger:
            row = awaiting(ledger)
            envelope = control.sign_review(unsigned(ledger, row), SECRET, kind='reply')
            self.assertEqual(ingest(ledger, envelope), [])
            approved = ledger.tasks.get(row['id'])
            self.assertEqual(approved['state'], 'approved')
            self.assertEqual(approved['data'], {**row['data'], 'review': envelope})
            self.assertEqual(ledger.releases.calls, [])


def setUpModule():
    use_cli_config()
    use_example_config()
