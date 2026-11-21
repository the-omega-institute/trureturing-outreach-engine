import copy
import unittest

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
