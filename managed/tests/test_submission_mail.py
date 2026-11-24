import copy
import unittest
from urllib.error import HTTPError
import xml.etree.ElementTree as ET

from managed import submission_mail as mail


def message(body, subject='[arXiv] submission announcement 2611.00992v1'):
    return {
        'from': {'emailAddress': {'address': ('@'.join(('no-reply', 'arxiv.org')))}},
        'subject': subject,
        'body': {'contentType': 'text', 'content': body},
        'receivedDateTime': '2026-11-01T02:00:00Z',
        'internetMessageId': '<receipt@example>',
    }


class SubmissionMailTests(unittest.TestCase):
    def test_all_public_links_are_checked_and_duplicate_versions_share_one_lookup(self):
        incoming = message('Reference: https://arxiv.org/abs/2611.99101v1\n'
                           'Our paper: https://arxiv.org/abs/2611.99102v1\n'
                           'PDF: https://arxiv.org/pdf/2611.99102v2.pdf', subject='New papers')
        repeated = message('https://arxiv.org/abs/2611.99102v1', subject='Follow-up')
        calls = []
        def verify(receipt):
            calls.append((receipt['arxiv_id'], receipt['arxiv_version']))
            return {'arxiv_id': receipt['arxiv_id'], 'arxiv_version': 'v2',
                    'arxiv_url': receipt['arxiv_url'], 'title': 'Public paper',
                    'authors': (['Blaire Mercer', 'Avery Reed']
                                if receipt['arxiv_id']=='2611.99102' else ['Another Author']),
                    'date': '2026-11-01'}
        report = mail.reconcile([], [incoming], verify=verify)
        self.assertEqual(calls, [('2611.99101', 'v1'), ('2611.99102', 'v2')])
        calls.clear()
        report = mail.reconcile([], [incoming, repeated], verify=verify)
        self.assertEqual(calls, [('2611.99101', 'v1'), ('2611.99102', 'v2')])
        self.assertEqual([p['arxiv_id'] for p in report['publications']], ['2611.99102'])

    def test_one_unavailable_or_unverified_record_does_not_discard_other_papers(self):
        bad = message('https://arxiv.org/abs/2611.99101v1', subject='First paper')
        good = message('https://arxiv.org/abs/2611.99102v1', subject='Second paper')
        for error in (HTTPError('https://export.arxiv.org/api/query', 429, 'Rate limit', {}, None),
                      TimeoutError('offline'), ET.ParseError('incomplete Atom response'),
                      ValueError('No public record yet')):
            with self.subTest(error=type(error).__name__):
                def verify(receipt):
                    if receipt['arxiv_id']=='2611.99101':
                        raise error
                    return {'arxiv_id': '2611.99102', 'arxiv_version': 'v1',
                            'arxiv_url': 'https://arxiv.org/abs/2611.99102v1',
                            'title': 'Public paper', 'authors': ['Blaire Mercer', 'Avery Reed'],
                            'date': '2026-11-01'}
                report = mail.reconcile([], [bad, good], verify=verify)
                self.assertEqual([p['arxiv_id'] for p in report['publications']], ['2611.99102'])
                self.assertEqual([p['arxiv_id'] for p in report['deferred']], ['2611.99101'])

    def test_unique_body_excludes_quoted_links_and_unlabelled_numbers(self):
        incoming = message('Quoted: https://arxiv.org/abs/2611.99101v1', subject='A public result')
        incoming['from']['emailAddress']['address']='coauthor@example.edu'
        incoming['uniqueBody']={'content': 'Our paper: https://arxiv.org/abs/2611.99102v1'}
        self.assertEqual(mail.parse_message(incoming)['arxiv_id'], '2611.99102')
        incoming['uniqueBody']={'content': 'Please check invoice 2611.99102.'}
        self.assertIsNone(mail.parse_message(incoming))

    def test_public_identifier_is_distinct_from_submit_request(self):
        receipt = mail.parse_message(message('Your submission submit/9000007 is public at https://arxiv.org/abs/2611.00992v1.'))
        self.assertEqual(receipt['submit_id'], '9000007')
        self.assertEqual(receipt['arxiv_id'], '2611.00992')
        self.assertEqual(receipt['arxiv_version'], 'v1')

        pending = mail.parse_message(message('Your submission submit/9000007 was received.', subject='arXiv submission received'))
        self.assertEqual(pending['submit_id'], '9000007')
        self.assertIsNone(pending['arxiv_id'])

    def test_unique_title_author_match_is_applied_without_network(self):
        papers = [{
            'id': 'sorrell-deletion-channel-input-entropy',
            'title': 'Extremal input entropy for deletion and insertion channels',
            'authors': ['Blaire Mercer', 'Avery Reed'],
        }]
        incoming = message('Title: Maximal input entropy for binary two-deletion and two-insertion channels\n'
                           'Authors: Blaire Mercer, Avery Reed\n'
                           'Public: https://arxiv.org/abs/2611.00992v1')
        record = {'arxiv_id': '2611.00992', 'arxiv_version': 'v1',
                  'arxiv_url': 'https://arxiv.org/abs/2611.00992',
                  'title': 'Maximal input entropy for binary two-deletion and two-insertion channels',
                  'authors': ['Blaire Mercer', 'Avery Reed'], 'date': '2026-10-31'}
        report = mail.reconcile(papers, [incoming], verify=lambda receipt: record)
        self.assertEqual([row['paper_id'] for row in report['updates']], ['sorrell-deletion-channel-input-entropy'])
        self.assertFalse(report['ambiguous'])
        data = {'papers': [copy.deepcopy(papers[0])], 'updated_on': '2026-11-04'}
        updated = mail.apply_updates(data, report)
        self.assertEqual(updated['papers'][0]['arxiv_id'], '2611.00992')
        self.assertEqual(updated['papers'][0]['arxiv_status'], 'public')

    def test_ambiguous_match_is_fail_closed(self):
        papers = [
            {'id': 'one', 'title': 'Entropy deletion channels', 'authors': ['Blaire Mercer', 'Avery Reed']},
            {'id': 'two', 'title': 'Entropy deletion channels', 'authors': ['Blaire Mercer', 'Avery Reed']},
        ]
        incoming = message('Title: Entropy deletion channels\nAuthors: Blaire Mercer, Avery Reed')
        record = {'arxiv_id': '2611.00992', 'arxiv_version': 'v1',
                  'arxiv_url': 'https://arxiv.org/abs/2611.00992', 'title': 'Entropy deletion channels',
                  'authors': ['Blaire Mercer', 'Avery Reed'], 'date': '2026-10-31'}
        report = mail.reconcile(papers, [incoming], verify=lambda receipt: record)
        self.assertEqual(report['updates'], [])
        self.assertEqual(report['ambiguous'][0]['candidates'], ['one', 'two'])

    def test_api_response_is_checked_against_public_identity(self):
        xml = b'''<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
          <entry><id>http://arxiv.org/abs/2611.00992v1</id>
          <title>Maximal input entropy</title><published>2026-10-31T00:00:00Z</published>
          <author><name>Blaire Mercer</name></author><author><name>Avery Reed</name></author></entry>
        </feed>'''
        self.assertEqual(mail.parse_api_response(xml)['arxiv_version'], 'v1')


if __name__ == '__main__':
    unittest.main()
