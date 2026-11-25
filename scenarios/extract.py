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
     name[5:] for name in dir(__import__('test_sent_events').SentEventsTests)
     if name.startswith('test_') and name not in {
         'test_commit_and_push_only_scoped_files_preserves_unrelated_staged_change',
         'test_commit_tables_rebases_once_when_main_moved',
         'unused'}],
 ('test_publication', 'PublicationTests', 'publication'): [
     name[5:] for name in dir(__import__('test_publication').PublicationTests)
     if name.startswith('test_') and name not in {
         'test_registration_uses_current_table_sha_and_preserves_existing_pause',
         'test_tick_delivers_existing_then_publishes_then_delivers_new_selections',
         'test_ready_delivery_precedes_publication_that_overruns_the_tick',
         'test_delivery_selected_late_in_a_tick_still_goes_out_that_tick',
         'test_legacy_migration_stays_after_publication',
         'test_both_delivery_passes_share_deliveries_per_tick',
         'test_delivery_batch_takes_new_deliveries_oldest_first_before_repolls'}]}



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
            sent_advance = stage == 'sent' and method == 'failure_logs_status_without_mail_content'
            if sent_advance: actual = sent_events.advance
            target = {'mathesis': 'managed.mathesis_delivery.advance', 'intake': 'managed.mailbox.intake',
                      'publication': 'managed.publication.advance', 'sent': 'managed.sent_events.advance' if sent_advance else 'managed.sent_events.reconcile'}[stage]
            def invoke(ledger, config, root, *args, **kwargs):
                nonlocal total
                capture = Capture(); root = Path(root)
                if module == 'test_pre_reviewed' and not (root / 'tables/contacts.json').exists():
                    (root / 'tables/contacts.json').write_text(canonical({'contacts': []}))
                    (root / 'tables/refusals.json').write_text(canonical({'refusals': []}))
                    (root / 'tables/positive.json').write_text(canonical({'entries': []}))
                case = {'schema_version': 1, 'id': f'{stage}-{method}-{len(mapping[identity])+1}',
                        'source_test': identity, 'stage': stage, 'now': ledger.tasks.clock(),
                        'workflow': control.workflow('mathesis'), 'installation': __import__('managed.location', fromlist=['example_config']).example_config(), 'config': copy.deepcopy(config),
                        'initial_rows': dump(ledger.tasks), 'initial_files': files(root),
                        'adapter_attributes': {'releases': {k: getattr(ledger.releases, k) for k in ('repo', 'tag') if hasattr(ledger.releases, k)}}}
                old_save, old_remote, old_clock = ledger.save, ledger.releases, ledger.tasks.clock
                def save(*a, **kw):
                    remote = ledger.releases; ledger.releases = old_remote
                    try:
                        value = capture.call('ledger', 'save', old_save, a, kw)
                        entry = capture.entries[-1]
                        entry['saved_snapshot'] = {'generation': ledger.generation,
                            'digest': ledger.snapshot['sha256']}
                        return value
                    finally: ledger.releases = remote
                ledger.save = save
                ledger.releases = capture.proxy('releases', old_remote)
                wrapped = list(args)
                if stage == 'mathesis':
                    wrapped[1] = capture.proxy('mathesis', args[1]); wrapped[2] = 'secret'
                    if len(wrapped) > 3: wrapped[3] = capture.proxy('publication', args[3])
                elif stage == 'publication': wrapped[1] = capture.proxy('publication', args[1]); wrapped[2] = 'secret'
                else:
                    box = args[0]; wrapped[0] = capture.proxy('mailbox', box) if box is not None else None
                    case['adapter_attributes']['mailbox'] = {'settings': copy.deepcopy(box.settings)} if box else {}
                    if stage == 'intake': case.update(revision=kwargs['revision'], datetime=kwargs['now'].isoformat())
                    else:
                        case['datetime'] = (args[1] if len(args) > 1 else __import__('test_sent_events').NOW).isoformat()
                        case['options'] = {'max_pages': sent_events.MAX_PAGES, 'mailbox': box is not None, 'advance': sent_advance}
                case['options'] = {**case.get('options', {}), 'previous': list(getattr(getattr(ledger, 'tick', None), 'previous', {}).get(stage, []))}
                from managed import recording, registry
                frame = {'admissions': [], '_errors': {}}
                token = recording._STAGE.set(frame)
                original_package, original_render = publication.package, registry.render
                def package(row, directory, releases):
                    def read(task_id, result_hash):
                        metadata, prepared, source = original_package(row, directory, old_remote)
                        return {'metadata': metadata, 'source': source, 'files': {
                            f.name: encode(f) for f in sorted(Path(prepared).iterdir())}}
                    response = capture.call('shortnote', 'package', read, (row['id'], __import__('managed.queue', fromlist=['digest']).digest(row['data']['result'])), {})
                    prepared = Path(directory) / 'prepared'; prepared.mkdir(exist_ok=True)
                    from managed.scenario_contract import decode
                    for name, data in response['files'].items():
                        path = decode(data, prepared)
                        if name == response['source']:
                            (Path(directory) / name).write_bytes(path.read_bytes())
                    return response['metadata'], prepared, response['source']
                def render(*a, **kw):
                    contacts = registry.read(root, 'contacts')
                    def perform(value):
                        original_render(*a, **kw)
                        return {k:v for k,v in files(root).items() if k in {'tables/contacts.json', 'tables/CONTACTS.md', 'tables/POSITIVE.md'}}
                    capture.call('registry', 'render', perform, (contacts,), {})
                old_commit = sent_events.commit_tables
                old_needs_push = sent_events.tables_need_push
                def commit(*a, **kw):
                    return capture.call('registry', 'commit_tables', lambda: old_commit(*a, **kw), (), {})
                def needs_push(*a, **kw):
                    return capture.call('registry', 'needs_push', lambda: old_needs_push(*a, **kw), (), {})
                with ExitStack() as hooks:
                    hooks.enter_context(patch('managed.sent_events.commit_tables', commit))
                    hooks.enter_context(patch('managed.sent_events.tables_need_push', needs_push))
                    if stage == 'publication':
                        hooks.enter_context(patch.object(publication, 'package', package))
                        from managed import pre_reviewed
                        def boundary(module, method, args_key):
                            function = getattr(module, method)
                            def call(*a, **kw):
                                entry = {'adapter': 'boundary', 'method': method, 'args': args_key(a, kw), 'kwargs': {}}
                                capture.entries.append(entry)
                                try:
                                    result = function(*a, **kw); entry['result'] = None; return result
                                except BaseException as error:

                                    if capture.entries[-1] is entry: entry['error'] = error_value(error)
                                    else: entry['result'] = None
                                    raise
                            hooks.enter_context(patch.object(module, method, call))
                        enqueue = publication.notes.enqueue
                        def enqueue_observed(*a, **kw):
                            try:
                                response = enqueue(*a, **kw)
                            except Exception as error:
                                entry = {'adapter': 'boundary', 'method': 'enqueue', 'args': [], 'kwargs': {}, 'result': None}
                                if not capture.entries or 'error' not in capture.entries[-1]: entry['error'] = error_value(error)
                                capture.entries.append(entry)
                                raise
                            capture.entries.append({'adapter': 'boundary', 'method': 'enqueue', 'args': [], 'kwargs': {}, 'result': None})
                            return response
                        hooks.enter_context(patch.object(publication.notes, 'enqueue', enqueue_observed))
                        boundary(publication, 'source_gate', lambda a, k: [a[0]['id']])
                        boundary(pre_reviewed, 'select', lambda a, k: [a[0]['id']])
                        boundary(mathesis_delivery, 'ingest', lambda a, k: [a[1]['task_id']])
                        boundary(publication, 'record', lambda a, k: [a[1], a[2]['phase']])
                    if sent_advance:
                        frozen = hooks.enter_context(patch('managed.sent_events.datetime'))
                        frozen.now.return_value = __import__('test_sent_events').NOW
                    if stage == 'sent': hooks.enter_context(patch.object(registry, 'render', render))
                    try: result = actual(ledger, config, root, *wrapped, **kwargs)
                    except BaseException as error: case['error'] = error_value(error); caught = error; result = None
                if sent_advance:
                    pages = [e for e in capture.entries if e['method'] == 'sent_events_page']
                    if pages: case['datetime'] = pages[0]['args'][1]
                recording._STAGE.reset(token)
                case['admissions'] = frame['admissions']
                case.update(script=capture.entries, expected_final_rows=logical(dump(ledger.tasks)), expected_final_files=files(root), expected_writes=writes(capture.entries))
                ledger.save, ledger.releases, ledger.tasks.clock = old_save, old_remote, old_clock
                (out / (case['id'] + '.json')).write_text(json.dumps(case, ensure_ascii=False, indent=2) + '\n')
                mapping[identity].append(case['id']); total += 1
                if 'error' in case:
                    raise caught
                return result
            with ExitStack() as stack:
                original_init = Tasks.__init__
                stack.enter_context(patch.object(Tasks, '__init__', lambda self, path, clock=None: original_init(self, path, clock=clock or (lambda: NOW - 100))))
                stack.enter_context(patch.object(publication.notes.w, 'now', return_value='2026-11-01T00:00:00+00:00'))
                stack.enter_context(patch('managed.control.workflow', return_value='e' * 64))
                stack.enter_context(patch('test_tasks.workflow', return_value='e' * 64))
                stack.enter_context(patch(target, side_effect=invoke))
                result = unittest.TestResult(); test.run(result)
                if result.errors or result.failures:
                    raise AssertionError(identity + '\n' + str(result.errors + result.failures))
    # Adapter and coordinator integration tests keep their native assertions.
    for module_name, class_name in [('test_publication', 'PublicationTests'), ('test_sent_events', 'SentEventsTests'),
                                   ('test_sent_events', 'SentEventsRecoveryTests'), ('test_sent_events', 'CommitTablesTests')]:
        cls = getattr(importlib.import_module(module_name), class_name)
        for name in dir(cls):
            if name.startswith('test_'):
                mapping.setdefault(f'{module_name}.{class_name}.{name}', [])
    (Path(__file__).parent / 'test-map.json').write_text(json.dumps(mapping, indent=2) + '\n')
    print(canonical({'converted_tests': sum(bool(v) for v in mapping.values()), 'native_tests': sum(not v for v in mapping.values()), 'scenarios': total}))


if __name__ == '__main__': main()
