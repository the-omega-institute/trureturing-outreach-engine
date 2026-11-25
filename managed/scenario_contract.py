"""Language-independent scenario value and logical ledger contracts (no effects)."""
import base64
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from .queue import canonical

JSON_COLUMNS = {'request', 'data', 'payload', 'packet', 'receipt', 'evidence', 'response', 'snapshot', 'envelope', 'authority', 'readback'}
NOW = 2_000_000_000.0


def dump(tasks):
    result = {}
    for (name,) in tasks.db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        info = list(tasks.db.execute('PRAGMA table_info("' + name + '")'))
        columns = [r[1] for r in info]
        rows = []
        for row in tasks.db.execute('SELECT * FROM "' + name + '" ORDER BY rowid'):
            item = dict(zip(columns, row))
            for column, value in item.items():
                if column in JSON_COLUMNS and isinstance(value, str):
                    try: item[column] = json.loads(value)
                    except ValueError: pass
            rows.append(item)
        result[name] = {'columns': columns, 'primary_key': [r[1] for r in sorted(info, key=lambda r:r[5]) if r[5]] or ['name'], 'rows': rows}
    return result


def logical(tables):
    value = json.loads(canonical(tables))
    for table in value.values():
        table['rows'].sort(key=lambda row: tuple(row[k] for k in table['primary_key']))
    return value


def load(tasks, tables):
    for name, table in tables.items():
        assert name.replace('_', '').isalnum()
        tasks.db.execute('DELETE FROM "' + name + '"')
        for row in table['rows']:
            values = [canonical(row[c]) if c in JSON_COLUMNS and not isinstance(row[c], str) else row[c] for c in table['columns']]
            tasks.db.execute('INSERT INTO "' + name + '" (' + ','.join('"'+c+'"' for c in table['columns']) + ') VALUES (' + ','.join('?' for _ in values) + ')', values)


def encode(value):
    if isinstance(value, Path):
        if value.is_file(): return {'$file': value.name, 'base64': base64.b64encode(value.read_bytes()).decode()}
        return {'$directory': True}
    if isinstance(value, bytes): return {'$bytes': base64.b64encode(value).decode()}
    if isinstance(value, datetime): return {'$datetime': value.isoformat()}
    if isinstance(value, sqlite3.Connection): return {'$database': True}
    if callable(value): return {'$callback': True}
    if isinstance(value, (list, tuple)): return [encode(x) for x in value]
    if isinstance(value, dict): return {k: encode(v) for k, v in value.items()}
    if isinstance(value, str) and value.startswith(('/private/var/folders/', '/var/folders/', '/tmp/', '/private/tmp/')):
        return {'$temporary': Path(value).name} if Path(value).is_file() else {'$directory': True}
    return value


def decode(value, directory):
    if isinstance(value, dict):
        if '$bytes' in value: return base64.b64decode(value['$bytes'])
        if '$file' in value:
            if Path(value['$file']).name != value['$file']: raise ValueError('Unsafe scenario file name')
            path = directory / value['$file']; path.write_bytes(base64.b64decode(value['base64'])); return path
        if '$datetime' in value: return datetime.fromisoformat(value['$datetime'])
        return {k: decode(v, directory) for k, v in value.items()}
    if isinstance(value, list): return [decode(v, directory) for v in value]
    return value



def files(root):
    return {str(p.relative_to(root)): base64.b64encode(p.read_bytes()).decode()
            for folder in ('tables', 'managed') if (root / folder).exists()
            for p in sorted((root / folder).rglob('*')) if p.is_file()}

