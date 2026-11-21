"""Work-conserving per-kind quotas; running CMA work is never preempted."""
from collections import Counter, deque

from .correspondence import LANE as CORRESPONDENCE, enabled as correspondence_priority
from .sandbox_pool import group_config, task_group
from .tasks import ACTIVE, KINDS, NOTE_KINDS


def lane(row, config, correspondence=()):
    if row['state'] == 'queued':
        if row['id'] in correspondence:
            return CORRESPONDENCE
    elif row['data'].get('pool_lane') == CORRESPONDENCE:
        return CORRESPONDENCE
    if (config.get('followup_lane') and row['kind'] in NOTE_KINDS
            and row['request']['inputs'].get('owner_ids')):
        return 'followup'
    return row['kind']


def validate(config):
    # Older/offline callers can omit the quota policy.
    if type(config.get('followup_lane',False)) is not bool:
        raise ValueError('followup_lane must be a boolean')
    pool = config.get('sandbox_pool') or {}
    if 'prioritize_correspondence_notes' in pool:
        group_config(pool, config['kinds'])
    if 'kind_quota' not in config:return
    if config['kind_quota'] != 'equal':
        raise ValueError('kind_quota must be equal')
    if type(config['max_active']) is not int or config['max_active'] < 0:
        raise ValueError('max_active must be a nonnegative integer')
    if any(k not in KINDS for k in config['kinds']):
        raise ValueError('Quota kinds must be known')


def _group_quotas(config):
    if 'kind_quota' not in config:
        return {}, {}
    pool = config.get('sandbox_pool') or {}
    limits, kind_groups = group_config(pool, config['kinds'])
    group_lanes = {}
    for kind, enabled in config['kinds'].items():
        if not enabled:
            continue
        names = group_lanes.setdefault(kind_groups.get(kind), set())
        names.add(kind)
        if config.get('followup_lane') and kind in NOTE_KINDS:
            names.add('followup')
    if correspondence_priority(config) and any(config['kinds'].get(kind) for kind in NOTE_KINDS):
        group_lanes.setdefault(kind_groups['reply'], set()).add(CORRESPONDENCE)
    shares = {}
    for group, names in group_lanes.items():
        capacity = config['max_active'] if group is None else limits[group] * pool['agents_per_sandbox']
        base, remainder = divmod(capacity, len(names))
        shares[group] = {name: base + (index < remainder) for index, name in enumerate(sorted(names))}
    return shares, kind_groups


def quotas(config):
    """Report each lane's total share, including followup across groups."""
    shares, _ = _group_quotas(config)
    total = Counter()
    for group_shares in shares.values():
        total.update(group_shares)
    return dict(total)


def summary(rows, config, correspondence=()):
    shares, kind_groups = _group_quotas(config)
    if not shares:
        return {}
    active = Counter()
    queued = Counter()
    for row in rows:
        key = (task_group(row, kind_groups, correspondence), lane(row, config, correspondence))
        if row['state'] in ACTIVE:
            active[key] += 1
        elif row['state'] == 'queued':
            queued[key] += 1
    result = {}
    for group, group_shares in shares.items():
        for name, quota in group_shares.items():
            key = (group, name)
            counts = result.setdefault(name, {'active': 0, 'queued': 0, 'quota': 0, 'borrowing': 0})
            counts['active'] += active[key]
            counts['queued'] += queued[key]
            counts['quota'] += quota
            counts['borrowing'] += max(0, active[key] - quota)
    return result


def interleave(rows, config, correspondence=()):
    """Round-robin kinds, preserving each kind's priority and arrival order."""
    kinds=quotas(config)
    if not kinds:return rows
    queues={}
    for row in rows:
        if row['state']=='queued' and (name := lane(row, config, correspondence)) in kinds:
            queues.setdefault(name,deque()).append(row)
    turn=deque(queues)
    result=[]
    for row in rows:
        if row['state']=='queued' and lane(row, config, correspondence) in kinds:
            kind=turn.popleft();result.append(queues[kind].popleft())
            if queues[kind]:turn.append(kind)
        else:result.append(row)
    return result


def at_share(row, rows, config, eligible, correspondence=()):
    shares, kind_groups = _group_quotas(config)
    group = task_group(row, kind_groups, correspondence)
    group_shares = shares.get(group, {})
    name = lane(row, config, correspondence)
    if name not in group_shares:
        return False
    active = Counter(lane(r, config, correspondence) for r in rows
                     if r['state'] in ACTIVE and task_group(r, kind_groups, correspondence) == group)
    if active[name] < group_shares[name]:
        return False
    below_share = {name for name, quota in group_shares.items() if active[name] < quota}
    return bool(below_share) and any(
        r['state'] == 'queued' and task_group(r, kind_groups, correspondence) == group
        and lane(r, config, correspondence) in below_share and eligible(r) for r in rows)
