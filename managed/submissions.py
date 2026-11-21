"""Validate and render the compact independent-submission board."""
from collections import Counter
from datetime import date
import json
from pathlib import Path, PurePosixPath
import re

from .queue import require

from .location import DATA_ROOT as ROOT, settings
STAGES = {
    'verification_needed': '待核验',
    'needs_work': '有阻塞',
    'ready_to_submit': '可交接',
    'submitted': '已投稿',
}
ROW_KEYS = {
    'id', 'title', 'authors', 'version_date', 'stage', 'target_journal',
    'blocker', 'next_step_owner', 'workspace',
}
ARXIV_KEYS = {
    'arxiv_id', 'arxiv_url', 'arxiv_version', 'arxiv_status',
    'arxiv_title', 'arxiv_date', 'arxiv_evidence',
}
ARXIV_ID = re.compile(r'\d{4}\.\d{4,5}')
ARXIV_VERSION = re.compile(r'v[1-9]\d*')


def local_path(value):
    """Accept portable pointers into the local mail root."""
    if not (isinstance(value, str) and value.startswith('../mail/')
            and len(PurePosixPath(value).parts) >= 4
            and '..' not in PurePosixPath(value).parts[2:]
            and not any(c in value for c in '\n\r\\')):
        return False
    return True


def validate_data(data):
    require(data.get('schema_version') == 1, 'Unknown submissions schema')
    date.fromisoformat(data['updated_on'])
    require(data.get('scope') == settings('identity')['submission_scope'], 'Independent submission scope required')
    rows = data.get('papers')
    require(isinstance(rows, list), 'Submissions must be a list')
    require(len({r.get('id') for r in rows}) == len(rows), 'Duplicate submission ID')
    for row in rows:
        require(set(row) in (ROW_KEYS, ROW_KEYS | ARXIV_KEYS),
                'Submission row must use the compact schema')
        require(re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', row['id']), 'Invalid submission ID')
        require(row['title'] and isinstance(row['title'], str), 'Missing submission title')
        require(isinstance(row['authors'], list) and row['authors']
                and len(row['authors']) == len(set(row['authors']))
                and set(row['authors']) <= set(settings('identity')['team_authors']),
                'Independent authors are required')
        date.fromisoformat(row['version_date'])
        require(row['stage'] in STAGES, 'Invalid submission stage')
        require(isinstance(row['target_journal'], str) and row['target_journal'],
                'Missing target journal')
        require(isinstance(row['blocker'], str) and '\n' not in row['blocker']
                and len(row['blocker']) <= 160, 'Blocker must be one short line')
        require(isinstance(row['next_step_owner'], str) and row['next_step_owner'],
                'Missing next-step owner')
        require(local_path(row['workspace']), 'Use a relative mail workspace path')
        if ARXIV_KEYS <= set(row):
            arxiv_status = row['arxiv_status']
            require(arxiv_status in {'submitted', 'public', 'withdrawn'},
                    'Invalid arXiv status')
            if row['arxiv_id']:
                require(isinstance(row['arxiv_id'], str) and ARXIV_ID.fullmatch(row['arxiv_id']),
                        'Invalid arXiv identifier')
            if row['arxiv_version']:
                require(isinstance(row['arxiv_version'], str) and ARXIV_VERSION.fullmatch(row['arxiv_version']),
                        'Invalid arXiv version')
            for key in ('arxiv_url', 'arxiv_evidence'):
                if row[key]:
                    require(isinstance(row[key], str) and row[key].startswith('https://arxiv.org/'),
                            'arXiv evidence must be a public arXiv URL')
            if row['arxiv_title']:
                require(isinstance(row['arxiv_title'], str) and row['arxiv_title'].strip(),
                        'Invalid public arXiv title')
            if row['arxiv_date']:
                date.fromisoformat(row['arxiv_date'])
            if arxiv_status == 'public':
                require(row['arxiv_id'] and row['arxiv_url'] and row['arxiv_version']
                        and row['arxiv_title'] and row['arxiv_date'] and row['arxiv_evidence'],
                        'Public arXiv rows require a verified identity and evidence')
    return counts(data)


def counts(data):
    stages = Counter(row['stage'] for row in data['papers'])
    return {
        'total': len(data['papers']),
        'ready_to_submit': stages['ready_to_submit'],
        'blocked': stages['needs_work'] + stages['verification_needed'],
        'submitted': stages['submitted'],
    }


def reconcile_tasks(papers, tasks):
    """Compare existing submission identities and pinned board snapshots offline."""
    from .automation import paper_identity, paper_summary
    from .queue import digest
    from .thread_staging import effective_inputs

    def title(value):
        return ' '.join(str(value or '').casefold().split())

    issues = []; matched = set()
    by_id = {p['id']: p for p in papers}
    for task in tasks:
        if task['kind'] != 'submission' or task['state'] == 'superseded':
            continue
        inputs = effective_inputs(task)
        pinned = inputs.get('paper') or {}; link = inputs.get('submission_source') or {}
        paper = by_id.get(pinned.get('id')) or by_id.get(link.get('paper_id'))
        candidates = [p for p in papers if title(p['title']) == title(pinned.get('title'))]
        if paper is None and len(candidates) == 1:
            paper = candidates[0]
        if paper is None:
            issues.append({'task_id': task['id'], 'paper_id': pinned.get('id') or link.get('paper_id') or '?',
                           'reason': 'Submission task paper is not on the board'})
            continue
        matched.add(paper['id'])
        reasons = []
        if any(value and value != paper['id'] for value in (pinned.get('id'), link.get('paper_id'))):
            reasons.append('paper id mismatch')
        if pinned.get('title') and title(pinned['title']) != title(paper['title']):
            reasons.append('title mismatch')
        current = paper_summary(paper)
        if link and link.get('identity') not in {digest(current), paper_identity(current)}:
            reasons.append('source identity mismatch')
        elif any(key in pinned and pinned[key] != current[key] for key in ('authors', 'workspace')):
            reasons.append('source identity mismatch')
        version = pinned.get('version_date') or pinned.get('version', {}).get('date')
        if version and version != current['version_date']:
            reasons.append('version mismatch')
        if pinned.get('stage') and pinned['stage'] != current['stage']:
            reasons.append('stage mismatch')
        if reasons:
            issues.append({'task_id': task['id'], 'paper_id': paper['id'], 'reason': '; '.join(reasons)})
    issues += [{'paper_id': p['id'], 'reason': 'Board paper has no submission task'}
               for p in papers if p['id'] not in matched]
    return issues


def escape(value):
    return (str(value).replace('&', '&amp;').replace('<', '&lt;')
            .replace('>', '&gt;').replace('|', '&#124;').replace('\n', '<br>'))


def view(data):
    metrics = validate_data(data)
    lines = [
        '# 独立投稿看板',
        '',
        (f'总计 {metrics["total"]} 篇 · 可交接 {metrics["ready_to_submit"]} 篇 · '
         f'有阻塞 {metrics["blocked"]} 篇 · 已投稿 {metrics["submitted"]} 篇。'),
        '',
        '| 论文 | 阶段 | 目标期刊 | arXiv | 卡在哪 | 下一步在谁 | 目录 |',
        '| --- | --- | --- | --- | --- | --- | --- |',
    ]
    for row in data['papers']:
        blocker = escape(row['blocker']) if row['blocker'] else '—'
        arxiv = '—'
        if set(row) == ROW_KEYS | ARXIV_KEYS:
            if row['arxiv_status'] == 'public':
                arxiv = f'[{escape(row["arxiv_id"] + row["arxiv_version"])}]({escape(row["arxiv_url"])})'
            elif row['arxiv_status'] == 'submitted':
                arxiv = '已提交·待公开 ID'
        lines.append('| ' + ' | '.join([
            escape(row['title']),
            STAGES[row['stage']],
            escape(row['target_journal']),
            arxiv,
            blocker,
            escape(row['next_step_owner']),
            f'`{escape(row["workspace"])}`',
        ]) + ' |')
    return '\n'.join(lines) + '\n'


def render(root=ROOT, check=False):
    root = Path(root)
    data = json.loads((root / 'tables/submissions.json').read_text())
    content = view(data)
    path = root / 'tables/SUBMISSIONS.md'
    if check:
        require(path.exists() and path.read_text() == content,
                'Regenerate submissions: python3 -m managed.registry render')
    else:
        path.write_text(content)
    return counts(data)


def validate(root=ROOT):
    return render(root, check=True)
