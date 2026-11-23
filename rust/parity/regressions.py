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
        snapshot, work = build(root, case.get('rows', []), case.get('work', {}))
        if case.get('historical_state'):
            import sqlite3
            with sqlite3.connect(snapshot) as db:
                db.execute('UPDATE task_events SET state=? WHERE revision=1', (case['historical_state'],))
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
