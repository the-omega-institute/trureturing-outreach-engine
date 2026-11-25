"""Committed synthetic independent-review reproducers and their command contracts."""
import json
from pathlib import Path
from fixtures import NOW, base, build, write
from compare import compare


def run(binary, folder):
    total = failures = 0
    cases = json.loads(Path(__file__).with_name('review_findings.json').read_text())
    for case in cases:
        root = folder / case['name']
        config = base(root)
        config.update(case.get('config', {}))
        for key in case.get('remove_config', []): config.pop(key, None)
        write(root / 'config.json', config)
        for name, value in case.get('tables', {}).items():
            write(root / 'tables' / (name + '.json'), value)
        for name in case.get('remove_tables', []):
            (root / 'tables' / (name + '.json')).unlink()
        for name in case.get('invalid_tables', []):
            (root / 'tables' / (name + '.json')).write_text('{invalid')
        event_fixture = case.get('event_fixture')
        rows = case.get('rows', [])
        if event_fixture:
            tasks = event_fixture['tasks']
            reviewable = event_fixture['reviewable_tasks']
            rows = [
                {
                    'id': f't{n}',
                    'kind': 'reply',
                    'state': 'awaiting-review' if n < reviewable else 'verified',
                    'data': {'result': {'outcome': 'completed', 'report': 'Synthetic'}},
                }
                for n in range(tasks)
            ]
        snapshot, work = build(root, rows, case.get('work', {}), bulk=bool(event_fixture))
        if event_fixture:
            import sqlite3
            events_per_task = event_fixture['events_per_task']
            with sqlite3.connect(snapshot) as db:
                db.execute('DELETE FROM task_events')
                db.executemany(
                    'UPDATE tasks SET revision=? WHERE id=?',
                    [(events_per_task, f't{n}') for n in range(event_fixture['tasks'])],
                )
                db.executemany(
                    'INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
                    [
                        (
                            f't{n}',
                            'awaiting-review' if n < reviewable else 'verified',
                            revision,
                            1.0,
                            '{}',
                        )
                        for n in range(tasks)
                        for revision in range(1, events_per_task + 1)
                    ],
                )
                db.commit()
        if case.get('pipeline_sources'):
            import sqlite3
            with sqlite3.connect(snapshot) as db:
                db.executemany(
                    'INSERT INTO pipeline_sources(result_id,identity,payload,baseline,task_id) VALUES(?,?,?,?,?)',
                    [
                        (
                            source['result_id'],
                            source.get('identity', ''),
                            source.get('payload', '{}'),
                            source.get('baseline', 0),
                            source.get('task_id'),
                        )
                        for source in case['pipeline_sources']
                    ],
                )
                db.commit()
        if case.get('historical_state'):
            import sqlite3
            with sqlite3.connect(snapshot) as db:
                db.execute('UPDATE task_events SET state=? WHERE revision=1', (case['historical_state'],))
        if 'task_events' in case:
            import sqlite3
            from fixtures import canonical
            with sqlite3.connect(snapshot) as db:
                db.execute('DELETE FROM task_events')
                db.executemany(
                    'INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
                    [(event['id'], event['state'], event['revision'], NOW, canonical(event['data']))
                     for event in case['task_events']],
                )
                for identity in {event['id'] for event in case['task_events']}:
                    revision = max(event['revision'] for event in case['task_events'] if event['id'] == identity)
                    db.execute('UPDATE tasks SET revision=? WHERE id=?', (revision, identity))
        for suffix, content in case.get('sidecars', {}).items():
            Path(str(snapshot) + suffix).write_bytes(content.encode())
        engine = None
        if 'engine_files' in case:
            engine = root / 'synthetic-engine'
            engine.mkdir()
            for name, text in case['engine_files'].items():
                path = engine / 'managed' / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
        commands = []
        for command in case['commands']:
            args = [str(work) if a == '<work>' else a for a in command]
            commands.append(args)
        count = compare(binary, root, snapshot, work, NOW, commands=commands,
                        expect_failure=case.get('failure', False), engine_root=engine)
        total += count
        if case.get('failure'): failures += count
        print(f"{case['name']}: {count} {'expected failures' if case.get('failure') else 'equal outputs'}", flush=True)
    return total, failures
