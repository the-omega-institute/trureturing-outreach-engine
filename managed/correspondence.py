"""Classify note work from the current registry and immutable task lineage."""
from collections import defaultdict
from pathlib import Path

from .registry import people
from .tasks import ACTIVE, NOTE_KINDS


OPEN_REPLY = ACTIVE | {'queued', 'awaiting-review', 'approved', 'awaiting-mailbox',
                      'owner-draft', 'awaiting-user', 'decision-recorded', 'bounced'}
LANE = 'correspondence'


def enabled(config):
    return (config.get('sandbox_pool') or {}).get('prioritize_correspondence_notes', False)


def field_ids(*objects):
    ids = set()
    for fields in objects:
        if not isinstance(fields, dict):
            continue
        for key in ('result_id', 'problem_id', 'id', 'result_ids', 'problem_ids'):
            value = fields.get(key)
            values = value if isinstance(value, list) else [value]
            ids.update(item for item in values if isinstance(item, str) and item)
    return ids


def delivery_problem_ids(metadata):
    return field_ids(metadata.get('origin'), (metadata.get('review') or {}).get('record'))


def problem_ids(row):
    inputs = row['request']['inputs']
    data = row['data']
    delivery = data.get('mathesis_delivery') or {}
    metadata = (delivery.get('collection_envelope') or delivery.get('envelope') or {}).get('metadata', {})
    ids = field_ids(inputs, inputs.get('pipeline'), inputs.get('result'), data.get('publication_loop'))
    ids.update(delivery_problem_ids(metadata))
    if row['owner'].startswith('result-'):
        ids.add(row['owner'][len('result-'):])  # Legacy result-owned requests.
    return ids


def reply_contact(row, by_id, by_email):
    inputs = row['request']['inputs']
    observation = inputs.get('mailbox_observation') or {}
    address = observation.get('email', '').strip().casefold()
    return (by_email.get(address) or by_id.get(row['owner'])
            or {'id': row['owner'], 'name': row['owner'], 'email': address or None})


def linked_contacts(rows, root, config):
    """Return correspondence contacts by task ID, without modifying the ledger."""
    if not enabled(config) or not any(row['kind'] in NOTE_KINDS for row in rows):
        return {}
    tables = Path(root) / 'tables'
    persons = people(root) if (tables / 'contacts.json').is_file() else []
    by_id, by_email = {}, {}
    for person in persons:
        contact = {key: person[key] for key in ('id', 'name', 'email')}
        by_id.update((record['id'], contact) for record in person['records'])
        by_email.update((address, contact) for address in person['emails'])
    open_contacts = {reply_contact(row, by_id, by_email)['id'] for row in rows
                     if row['kind'] == 'reply' and row['state'] in OPEN_REPLY}

    by_problem = defaultdict(dict)
    for person in persons:
        records = person['records']
        # Only a live exchange (an open reply task) links a contact's results;
        # past praise or an old reply alone would move the whole backlog.
        if person['id'] in open_contacts:
            for record in records:
                for problem in record.get('problem_ids', []):
                    by_problem[problem][person['id']] = by_id[person['id']]

    by_task = {row['id']: row for row in rows}
    linked = {}
    for row in rows:
        if row['kind'] not in NOTE_KINDS:
            continue
        contacts = {}
        pending, seen = [row], set()
        while pending:
            ancestor = pending.pop()
            if ancestor['id'] in seen:
                continue
            seen.add(ancestor['id'])
            if ancestor['kind'] == 'reply':
                contact = reply_contact(ancestor, by_id, by_email)
                contacts[contact['id']] = contact
                continue
            for problem in problem_ids(ancestor):
                contacts.update(by_problem.get(problem, {}))
            inputs = ancestor['request']['inputs']
            origin = by_task.get(inputs.get('reply_origin_task_id'))
            if origin and origin['kind'] == 'reply':
                pending.append(origin)
            for key, field in (('automation_parent', 'task_id'), ('pipeline', 'note_task_id')):
                link = inputs.get(key)
                if isinstance(link, dict) and isinstance(link.get(field), str) and link[field] in by_task:
                    pending.append(by_task[link[field]])
            for link in inputs.get('note_sources') or []:
                source = by_task.get(link['task_id'])
                if source and source['kind'] == 'shortnote':
                    pending.append(source)
        if contacts:
            linked[row['id']] = [contacts[contact_id] for contact_id in sorted(contacts)]
    return linked
