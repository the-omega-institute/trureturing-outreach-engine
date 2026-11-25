#!/usr/bin/env python3
"""Refresh synthetic scenarios from selected original tests, retaining assertions.

This development tool imports test fixtures. runner.py never imports those tests.
A stage invocation becomes a separate scenario; test-map.json records provenance.
"""
import copy
from contextlib import ExitStack
import importlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from runner import ROOT, NOW, KEY, dump, encode, files, writes, error_value, canonical, logical
sys.path[:0] = [str(ROOT / 'managed/tests'), str(ROOT / 'scripts/short_notes')]
from managed import control, mathesis_delivery, mailbox, publication, sent_events
from managed.tasks import Tasks

SELECTION = {
 ('test_mathesis_delivery', 'DeliveryTests', 'mathesis'): [
 'pass_signed_selection_and_persisted_receipt_finish_without_platform_review',
 'lost_post_response_restores_original_identity_even_while_paused',
 'get_timeout_preserves_original_handle_before_another_current_version',
 'checkpoint_failure_prevents_external_post',
 'pause_disabled_kind_and_prepare_mode_never_post',
 'test_mode_cannot_write_production_ledger',
 'same_work_in_two_tasks_reuses_persisted_receipt',
 'changed_metadata_for_same_manuscript_version_cannot_post',
 'artifact_readback_hash_failure_cannot_post',
 'wrong_post_or_get_receipt_cannot_finish',
 'http_conflict_stays_visible_and_does_not_retry',
 'conflict_revises_our_current_delivery_with_exact_passed_source',
 'conflict_identical_sha_adopts_current_receipt_without_posting_again',
 'conflict_other_producer_keeps_diagnostic_and_never_revises',
 'lost_conflict_revision_response_replays_without_another_version',
 'paused_conflict_can_adopt_but_cannot_post_a_revision',
 'new_workflow_or_revoked_review_prevents_post'],
 ('test_pre_reviewed', 'PreReviewedTests', 'mathesis'): [
 'exact_envelope_pass_authors_hashes_decisions_and_pinned_lean',
 'unattempted_old_handoff_upgrades_without_mutating_signed_selection',
 'lost_link_queue_response_retries_without_another_collection_post',
 'release_file_mutation_never_posts', 'checkpoint_failure_prevents_collection_post',
 'conflicts_keep_the_original_version_without_allocating_another',
 'lost_post_response_replays_identical_selection_after_restore',
 'readback_hash_failure_keeps_receipt_and_reconciles_while_paused',
 'wrong_version_or_source_hash_never_verifies',
 'missing_prior_work_is_recorded_without_blocking_collection',
 'legacy_accepted_is_skipped_with_operator_reason',
 'uncertain_withdrawal_retries_from_checkpoint',
 'collection_is_delivered_and_read_back_before_withdrawal',
 ],
 ('test_mailbox_triage', 'TriageTests', 'intake'): [
 'intake_skips_operator_staged_inbound', 'intake_records_covered_unmapped_sender_as_handled',
 'intake_skips_hydrated_reply_on_latest_inbound', 'intake_stages_reply_while_research_covers_the_message',
 'intake_stages_when_covering_task_is_terminal', 'known_new_thread_and_unknown_first_message_get_separate_dispositions',
 'each_observed_message_gets_a_reason_when_latest_has_a_sent_answer',
 'hello_stranger_stays_in_attention'],
 ('test_sent_events', 'SentEventsTests', 'sent'): [
 'owner_draft_sent_uses_exact_sent_receipt_and_reviewed_headline',
 'operator_created_draft_sent_without_ledger_task',
 'sent_evidence_preserves_conversation_account_and_sender',
 'operator_direct_send_with_exact_sent_receipt_uses_owner_brief',
 'sent_message_matches_immutable_draft_receipt', 'unreviewed_task_never_exports_report_or_packet',
 'internet_identity_matches_receipt_after_graph_id_changes',
 'unregistered_recipient_is_skipped_without_registration',
 'two_ticks_and_duplicate_page_rows_are_idempotent'],
 ('test_publication', 'PublicationTests', 'publication'): [
 'old_workflow_pass_still_enqueues_exact_reviewed_bytes',
 'correspondent_note_is_published_before_backlog',
 'mathesis_child_publishes_instead_of_its_passed_source_note',
 'bounced_or_agent_held_earlier_revisions_do_not_block_passed_note',
 'passed_note_after_guard_stopped_continuation_is_published',
 'family_first_author_names_are_not_joint_work',
 'shortnote_waits_untouched_for_non_passed_mathesis_child',
 'mathesis_is_preferred_for_same_revision_without_explicit_lineage']}


class Capture:
    def __init__(self): self.entries = []
    def proxy(self, name, target): return Proxy(self, name, target)
    def call(self, name, method, function, args, kwargs):
        entry = {'adapter': name, 'method': method, 'args': encode(args), 'kwargs': encode(kwargs)}
        self.entries.append(entry)
        try:
            result = function(*args, **kwargs); entry['result'] = encode(result)
            if name == 'releases' and method == 'run' and '--dir' in args:
                path = Path(args[args.index('--dir') + 1]) / args[args.index('--pattern') + 1]
                entry['download'] = {'name': path.name, 'base64': encode(path)['base64']}
            return result
        except Exception as error:
            entry['error'] = error_value(error); raise


class Proxy:
    def __init__(self, capture, name, target): self.capture, self.name, self.target = capture, name, target
    def __getattr__(self, name):
        attr = getattr(self.target, name)
        if callable(attr): return lambda *a, **kw: self.capture.call(self.name, name, attr, a, kw)
        return attr


def main():
    out = Path(__file__).parent / 'cases'; mapping = {}; total = 0
    for path in out.glob('*.json'): path.unlink()
    for (module, cls, stage), methods in SELECTION.items():
        mod = importlib.import_module(module)
        if hasattr(mod, 'setUpModule'): mod.setUpModule()
        for method in methods:
            identity = f'{module}.{cls}.test_{method}'
            test = getattr(mod, cls)('test_' + method)
            mapping[identity] = []
            actual = {'mathesis': mathesis_delivery.advance, 'intake': mailbox.intake,
                      'publication': publication.advance, 'sent': sent_events.reconcile}[stage]
            target = {'mathesis': 'managed.mathesis_delivery.advance', 'intake': 'managed.mailbox.intake',
                      'publication': 'managed.publication.advance', 'sent': 'managed.sent_events.reconcile'}[stage]
            def invoke(ledger, config, root, *args, **kwargs):
                nonlocal total
                capture = Capture(); root = Path(root)
                if module == 'test_pre_reviewed' and not (root / 'tables/contacts.json').exists():
                    (root / 'tables/contacts.json').write_text(canonical({'contacts': []}))
                    (root / 'tables/refusals.json').write_text(canonical({'refusals': []}))
                    (root / 'tables/positive.json').write_text(canonical({'entries': []}))
                case = {'schema_version': 1, 'id': f'{stage}-{method}-{len(mapping[identity])+1}',
                        'source_test': identity, 'stage': stage, 'now': NOW,
                        'workflow': control.workflow('mathesis'), 'installation': __import__('managed.location', fromlist=['example_config']).example_config(), 'config': copy.deepcopy(config),
                        'initial_rows': dump(ledger.tasks), 'initial_files': files(root),
                        'adapter_attributes': {'releases': {k: getattr(ledger.releases, k) for k in ('repo', 'tag') if hasattr(ledger.releases, k)}}}
                old_save, old_remote, old_clock = ledger.save, ledger.releases, ledger.tasks.clock
                def save(*a, **kw):
                    remote = ledger.releases; ledger.releases = old_remote
                    try: return capture.call('ledger', 'save', old_save, a, kw)
                    finally: ledger.releases = remote
                ledger.save = save
                ledger.releases = capture.proxy('releases', old_remote); ledger.tasks.clock = lambda: NOW
                wrapped = list(args)
                if stage == 'mathesis':
                    wrapped[1] = capture.proxy('mathesis', args[1]); wrapped[2] = 'secret'
                    if len(wrapped) > 3: wrapped[3] = capture.proxy('publication', args[3])
                elif stage == 'publication': wrapped[1] = capture.proxy('publication', args[1]); wrapped[2] = 'secret'
                else:
                    box = args[0]; wrapped[0] = capture.proxy('mailbox', box)
                    case['adapter_attributes']['mailbox'] = {'settings': copy.deepcopy(box.settings)}
                    if stage == 'intake': case.update(revision=kwargs['revision'], datetime=kwargs['now'].isoformat())
                    else: case['datetime'] = args[1].isoformat()
                with patch('managed.sent_events.commit_tables', lambda *a, **kw: capture.call('registry', 'commit_tables', lambda: None, (), {})):
                    try: result = actual(ledger, config, root, *wrapped, **kwargs)
                    except Exception as error: case['error'] = error_value(error); result = None
                case.update(script=capture.entries, expected_final_rows=logical(dump(ledger.tasks)), expected_final_files=files(root), expected_writes=writes(capture.entries))
                ledger.save, ledger.releases, ledger.tasks.clock = old_save, old_remote, old_clock
                (out / (case['id'] + '.json')).write_text(json.dumps(case, ensure_ascii=False, indent=2) + '\n')
                mapping[identity].append(case['id']); total += 1
                if 'error' in case:
                    from runner import raise_error
                    raise_error(case['error'])
                return result
            with ExitStack() as stack:
                original_init = Tasks.__init__
                stack.enter_context(patch.object(Tasks, '__init__', lambda self, path, clock=None: original_init(self, path, clock=clock or (lambda: NOW - 100))))
                stack.enter_context(patch('managed.control.workflow', return_value='e' * 64))
                stack.enter_context(patch('test_tasks.workflow', return_value='e' * 64))
                stack.enter_context(patch(target, side_effect=invoke))
                result = unittest.TestResult(); test.run(result)
                if result.errors or result.failures:
                    raise AssertionError(identity + '\n' + str(result.errors + result.failures))
    (Path(__file__).parent / 'test-map.json').write_text(json.dumps(mapping, indent=2) + '\n')
    print(canonical({'tests': len(mapping), 'scenarios': total}))


if __name__ == '__main__': main()
