"""the owner's read-only to-do tables from a saved coordinator work inventory."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import re

from .draft_status import event_date, timestamp
from .mail_domains import HOME_RECIPIENT_REASON
from .mailbox import address
from .mailbox_triage import EDITORIAL_CONFIRMATION, manuscript_ids
from .registry import activity, people


SECTIONS = (
    ('confirmations', '只有你能操作的确认'),
    ('questions', '需要你决定的问题'),
    ('mailbox_holds', '需要处理的邮件重试'),
    ('drafts', '草稿箱待你发送'),
    ('owner_controlled', '你自己掌握的来信'),
    ('old_drafts', '很久未回（低优先级）'),
)
OPERATOR_TITLE = '供操作员核对（不用给她看）'
EARLIEST = datetime.min.replace(tzinfo=timezone.utc)
DECISION_REASONS = (
    (r'authorship|author[_ -]?order|coauthor|署名|作者', '涉及署名或作者顺序'),
    (r'journal|submi(?:t|ssion)|publi(?:sh|cation)|arxiv|投稿|期刊|发表|出版|发布',
     '涉及投稿或发表安排'),
    (r'collaborat|commitment|cooperat|合作|承诺', '涉及合作或个人承诺'),
    (r'meeting|invitation|seminar|conference|visit|邀请|会议|会面|讲座|访问',
     '涉及邀请或时间安排'),
    (r'identity|affiliation|funding|tools?|\bai\b|project|身份|机构|单位|资助|经费|工具|人工智能|项目',
     '涉及个人身份或项目情况'),
    (r'personal|health|个人|私人|健康', '涉及个人事项'),
)
PROCESS_NOISE = r'未保存|尚未保存|独立\s*Claude|待独立|发送批准|协调器|仅生成|仅准备|拟议|提案，|只是待'


def _decision_reason(question):
    text = (question.get('id') or '') + ' ' + question.get('question', '')
    for pattern, reason in DECISION_REASONS:
        if re.search(pattern, text, re.I):
            return reason
    return '需要你本人决定'


def _shorten(value, limit=60):
    value = ' '.join(value.split())
    return value if len(value) <= limit else value[:limit - 1] + '…'


def _date(value):
    return value.date().isoformat() if value else None


def _latest_inbound(inputs, account, thread_id=None):
    expected = inputs.get('latest_inbound_message_id')
    candidates = []
    for message in inputs.get('thread') or []:
        sender = message.get('from')
        email = sender.strip() if isinstance(sender, str) else address(sender)
        received = timestamp(message.get('receivedDateTime'))
        if (not received or message.get('isDraft') or message.get('folder') == 'sentitems'
                or message.get('direction') in {'outgoing', 'sent'}
                or (account and email.casefold() == account)
                or (thread_id and message.get('conversationId') not in (None, thread_id))):
            continue
        if expected and message.get('internetMessageId') == expected:
            return email, received
        candidates.append((email, received))
    return max(candidates, key=lambda item: item[1], default=('', None))


def _journal_confirmations(attention, excluded_tasks):
    seen = set()
    newest = max((timestamp(m.get('received')) or EARLIEST for m in attention), default=None)
    for mail in sorted(attention, key=lambda m: timestamp(m.get('received')) or EARLIEST, reverse=True):
        subject = mail.get('subject') or ''
        if (mail.get('category') != 'editorial-action' or not EDITORIAL_CONFIRMATION.search(subject)
                or mail.get('task_id') in excluded_tasks):
            continue
        # A reply to the owner's own support request is an acknowledgement,
        # and a confirmation link older than two weeks has lapsed.
        received = timestamp(mail.get('received'))
        if re.match(r'\s*(?:re|fw|fwd)\s*:', subject, re.I) or (
                received and newest and (newest - received).days > 14):
            continue
        identity = mail.get('manuscript_id') or next(iter(manuscript_ids(subject)), None)
        if identity:
            key = identity.casefold()
            if key in seen:
                continue
            seen.add(key)
        email = mail.get('from') or ''
        journal = mail.get('journal') or mail.get('name') or email.partition('@')[2] or '期刊编辑部'
        if isinstance(journal, dict):
            journal = journal.get('name') or mail.get('name') or '期刊编辑部'
        if re.search(r'\b(?:co[- ]?author(?:ship)?|authorship)\b|作者', subject, re.I):
            action = '点开邮件中的作者身份确认链接'
        else:
            action = '点开邮件中的确认或验证链接'
        incoming = _date(timestamp(mail.get('received')))
        yield {'person': journal, 'email': email, 'incoming_date': incoming,
               'task_id': mail.get('task_id'), 'manuscript_id': identity,
               'content': subject + '：' + action,
               'notes': '来信日期：' + (incoming or '未知')}


def _owner_controlled(messages, by_email, excluded_tasks):
    groups = defaultdict(dict)
    for index, mail in enumerate(messages):
        if ((mail.get('category') != 'owner-controlled' and mail.get('intake_reason') != 'owner-controlled')
                or mail.get('task_id') in excluded_tasks):
            continue
        sender = (mail.get('from') or {}).get('emailAddress') or {}
        email = (sender.get('address') or '').strip()
        if not email:
            continue
        person = by_email.get(email.casefold())
        key = person['id'] if person else email.casefold()
        identity = mail.get('internetMessageId') or mail.get('id') or index
        groups[key][identity] = (mail, email, person, sender.get('name'))
    rows = []
    for group in groups.values():
        mail, email, person, name = max(group.values(),
                                      key=lambda item: timestamp(item[0].get('receivedDateTime')) or EARLIEST)
        received = timestamp(mail.get('receivedDateTime'))
        count = len(group)
        rows.append((received or EARLIEST, {
            'person': person['name'] if person else name or email, 'email': email,
            'task_id': None, 'incoming_date': _date(received), 'unanswered_count': count,
            'content': f'{count} 封来信待回复', 'notes': '最近来信：' + (_date(received) or '未知'),
        }))
    return [row for _, row in sorted(rows, key=lambda item: item[0], reverse=True)]


def owner_todo(work, root, now=None) -> dict:
    """Project a saved coordinator inventory and the local contact registry."""
    now = datetime.now(timezone.utc) if now is None else timestamp(now)
    if now is None:
        raise ValueError('now must be a datetime or ISO timestamp')
    persons = people(root)
    by_id = {record['id']: person for person in persons for record in person['records']}
    by_email = {email.casefold(): person for person in persons for email in person['emails']}
    tasks = {task['id']: task for task in work.get('affairs_tasks', [])}
    draft_tasks = {task['task_id']: {**tasks.get(task['task_id'], {}), **task}
                   for task in work.get('owner_drafts', [])}
    for key, task in tasks.items():
        if task.get('state') == 'owner-draft':
            draft_tasks.setdefault(key, task)
    excluded = {key for key, task in draft_tasks.items() if task.get('draft_status') in {'sent', 'missing'}}

    def row_for(task, *, decision=False):
        request = task.get('request') or {}
        inputs = request.get('inputs') or {}
        observation = task.get('mailbox_observation') or inputs.get('mailbox_observation') or {}
        packet = task.get('packet') or task.get('packet_override') or (task.get('result') or {}).get('packet') or {}
        recipient = next(iter(packet.get('to') or []), '')
        account = (work.get('mailbox_account') or packet.get('sender') or '').casefold()
        sender, received = _latest_inbound(inputs, account,
                                           inputs.get('thread_id') or packet.get('thread_id'))
        observed_email = observation.get('email') or sender
        email = (observed_email or recipient) if decision else (recipient or observed_email)
        owner = task.get('owner') or request.get('owner') or ''
        person = (by_id.get(owner) or by_email.get(owner.casefold()) or
                  by_email.get(email.strip().casefold()) or by_email.get(observed_email.strip().casefold()))
        if not email:
            email = person['email'] if person else owner if '@' in owner else ''
        incoming = timestamp(observation.get('at')) or received
        registered = decision and not incoming and person and timestamp(activity(person)['last_in'])
        incoming = incoming or registered or None
        return {'task_id': task.get('task_id') or task.get('id'),
                'person': person['name'] if person else email or '未登记', 'email': email,
                'incoming_date': _date(incoming),
                **({'incoming_date_source': 'registry'} if registered else {})}, incoming, person

    view = {key: [] for key, _ in SECTIONS}
    view['operator_check'] = {'title': OPERATOR_TITLE, 'rows': []}
    view['confirmations'] = list(_journal_confirmations(work.get('mailbox_attention', []), excluded))

    def waiting(task, field):
        opened = timestamp(task.get(field))
        return max(0, int((now - opened).total_seconds())) if opened else None

    def age_note(age):
        if age is None:
            return '等待时间未知'
        days, hours = divmod(age // 3600, 24)
        return f'已等待 {days} 天 {hours} 小时'

    items = sorted(work.get('user_questions', []),
                   key=lambda item: timestamp(item.get('opened_at')) or EARLIEST)
    for item in items:
        task = {**tasks.get(item['task_id'], {}), **item}
        result = task.get('result') or {}
        questions = item.get('questions') or (result.get('routing') or {}).get('questions', [])
        row, _, _ = row_for(task, decision=True)
        age = waiting(item, 'opened_at')
        for question in questions:
            quote = question.get('source_quote')
            content = question['question'] + ('\n“' + quote + '”' if quote else '')
            reason = _decision_reason(question)
            date_label = '登记的最近来信日期' if row.get('incoming_date_source') == 'registry' else '来信日期'
            view['questions'].append({**row, 'question_id': question.get('id'),
                'content': content, 'source_quote': quote, 'age_seconds': age, 'reason': reason,
                'notes': age_note(age) + '，' + reason + '，' + date_label + '：' + (row['incoming_date'] or '未知')})

    holds = sorted(work.get('mailbox_retry_exhausted', []), key=lambda item: item['since'])
    for item in holds:
        task = {**tasks.get(item['task_id'], {}), **item}
        row, _, _ = row_for(task, decision=True)
        age = waiting(item, 'since')
        view['mailbox_holds'].append({**row, 'age_seconds': age, 'attempts': item['attempts'],
                                      'content': item['reason'], 'notes': age_note(age)})

    drafts, old_drafts = [], []
    for task in draft_tasks.values():
        receipt = task.get('draft_receipt')
        if task.get('state', 'owner-draft') != 'owner-draft' or not receipt:
            continue
        status = task.get('draft_status')
        if status not in {'present', 'sent', 'missing'} or (task.get('sent_receipt') and status == 'present'):
            continue
        row, incoming, _ = row_for(task)
        result = task.get('result') or {}
        brief = result.get('owner_brief') or {}
        row.update(content=brief.get('summary_zh') or result.get('headline') or '暂无摘要',
                   draft_status=status, draft_checked_at=task.get('draft_checked_at'),
                   draft_stale=bool(task.get('draft_stale')))
        stale = '草稿箱状态待复查' if row['draft_stale'] else None
        if status != 'present':
            label = '已在已发送邮件找到记录' if status == 'sent' else '草稿箱中未找到'
            row['notes'] = '；'.join(filter(None, (label, stale)))
            row['later_sent_at'] = task.get('later_sent_at')
            view['operator_check']['rows'].append(row)
            continue
        notes = []
        if (task.get('routing_override') or {}).get('reason') == HOME_RECIPIENT_REASON:
            notes.append('收件人在本校，需你过目后发送')
        if task.get('has_placeholder'):
            notes.append('需要你补内容')
        if task.get('owner_edited'):
            notes.append('你已改过')
        count = task.get('attachment_count')
        packet = task.get('packet') or task.get('packet_override') or result.get('packet') or {}
        if count is None and task.get('draft_body_checked') is None:
            count = len(packet.get('attachments') or [])
        notes.append(f'{count} 个附件' if count is not None else '附件数量待核对')
        if stale:
            notes.append(stale)
        if task.get('draft_body_checked') is False:
            notes.append('草稿正文待复查')
        saved = timestamp(task.get('draft_saved_at') or receipt.get('at') or receipt.get('saved_at'))
        brief_text = '\n'.join([brief.get('summary_zh') or '',
                                *(brief.get('decisions_zh') or []), *(brief.get('cautions_zh') or [])])
        body = (packet.get('body') or '') if task.get('draft_body_checked') is not True else ''
        event = task.get('event_date') or event_date(
            '\n'.join((brief_text, packet.get('subject') or '', body)), saved or incoming or now)
        if event:
            notes.append('约定日期：' + event)
        cautions = [c for c in brief.get('cautions_zh') or [] if not re.search(PROCESS_NOISE, c)]
        if cautions:
            notes.append(_shorten(cautions[0]))
        notes.extend(brief.get('decisions_zh') or [])
        notes.append('来信日期：' + (row['incoming_date'] or '未知'))
        row.update(notes='；'.join(notes), event_date=event, attachment_count=count,
                   has_placeholder=task.get('has_placeholder'), owner_edited=task.get('owner_edited'))
        if incoming and now - incoming > timedelta(days=21):
            old_drafts.append((incoming, row))
        else:
            drafts.append((saved or EARLIEST, row))
    drafts.sort(key=lambda item: item[0], reverse=True)
    drafts.sort(key=lambda item: (item[1]['event_date'] is None, item[1]['event_date'] or ''))
    view['drafts'] = [row for _, row in drafts]
    view['owner_controlled'] = _owner_controlled(work.get('mailbox_needs_classification', []), by_email, excluded)
    view['old_drafts'] = [row for _, row in sorted(old_drafts, key=lambda item: item[0], reverse=True)]
    return view


def _cell(value):
    return (str(value or '').replace('\\', '\\\\').replace('|', r'\|')
            .replace('\r\n', '\n').replace('\r', '\n').replace('\n', '<br>'))


def render_markdown(view):
    """Render the nonempty owner sections in the four-column Chinese format."""
    lines = []
    for key, title in SECTIONS:
        if not view.get(key):
            continue
        lines.extend(['## ' + title, '', '| 人 | 邮箱 | 内容 | 注意事项 |', '| --- | --- | --- | --- |'])
        for row in view[key]:
            lines.append('| ' + ' | '.join(_cell(row[field]) for field in ('person', 'email', 'content', 'notes')) + ' |')
        lines.append('')
    return '\n'.join(lines)
