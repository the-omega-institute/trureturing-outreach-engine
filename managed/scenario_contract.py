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



def capture_tables(tasks, baseline=None, limit=512 * 1024):
    """Stream logical hashes and a bounded delta from a prior row-hash index.

    The index retains hashes/rowids, never historical row payloads. Delta entries
    retain SQL arrival order for new/replaced rows; unchanged rows come from the
    caller's original snapshot or a preceding replay frame.
    """
    import hashlib
    result, index, delta, size, overflow = {}, {}, {}, 0, False
    previous = (baseline or {}).get('tables', {})
    for (name,) in tasks.db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        info = list(tasks.db.execute('PRAGMA table_info("' + name + '")'))
        columns = [r[1] for r in info]
        keys = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5]] or ['name']
        schema = {'columns': columns, 'primary_key': keys}
        rows, changes = {}, []
        old = previous.get(name, {}).get('rows', {})
        h = hashlib.sha256()
        h.update(canonical({**schema, 'rows': []})[:-2].encode())
        first = True
        order = ','.join('"' + k + '"' for k in keys)
        for raw_row in tasks.db.execute('SELECT rowid,* FROM "' + name + '" ORDER BY ' + order):
            rowid, *row = raw_row
            item = dict(zip(columns, row))
            for column, value in item.items():
                if column in JSON_COLUMNS and isinstance(value, str):
                    try: item[column] = json.loads(value)
                    except ValueError: pass
            raw = canonical(item).encode()
            if not first: h.update(b',')
            first = False; h.update(raw)
            key = canonical([item[k] for k in keys])
            fingerprint = [hashlib.sha256(raw).hexdigest(), rowid]
            rows[key] = fingerprint
            if baseline is not None and old.get(key) != fingerprint:
                size += len(raw) + len(key.encode()) + 128
                if size > limit: overflow = True
                elif not overflow:
                    changes.append({'key': key, 'row': item, 'append': key not in old or old[key][1] != rowid, '_rowid': rowid})
        h.update(b']}'); result[name] = h.hexdigest()
        index[name] = {**schema, 'rows': rows}
        if baseline is not None:
            deleted = sorted(set(old) - set(rows))
            size += sum(len(k.encode()) + 8 for k in deleted)
            if size > limit: overflow = True
            if changes or deleted or name not in previous:
                changes.sort(key=lambda c: c['_rowid'])
                for change in changes: change.pop('_rowid')
                delta[name] = {**schema, 'changes': changes, 'deleted': deleted}
    if baseline is not None:
        for name in set(previous) - set(index): delta[name] = {'drop': True}
    return result, {'digests': result, 'tables': index}, delta, overflow


def table_digests(tasks):
    return capture_tables(tasks)[0]
