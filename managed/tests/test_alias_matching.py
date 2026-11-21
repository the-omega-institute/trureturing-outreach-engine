"""Sender identity matching must respect names and public suffix boundaries."""
import unittest

from managed.mailbox import _alias_names_match, _alias_recipient, _registrable_domain


class AliasMatchingTests(unittest.TestCase):
    def test_names_ignore_case_accents_and_optional_middle_initials(self):
        for left, right in [
                ('RODERICK LANGFORD', 'Roderick Langford'),
                ('Roderick J. Langford', 'Roderick Langford'),
                ('Roderick Langford', 'Professor Roderick J. Langford'),
                ('Roderick J. Langford', 'Roderick John Langford'),
                ('Roderick Jo\u0308rg Langford', 'Roderick Jörg Langford')]:
            with self.subTest(left=left, right=right):
                self.assertTrue(_alias_names_match(left, right))
                self.assertTrue(_alias_names_match(right, left))

    def test_names_require_matching_first_last_and_nonconflicting_middle(self):
        for name in ('', 'Langford', 'Norman Langford', 'Roderick Stroud',
                     'Roderick K. Langford', 'Roderick James Langford'):
            with self.subTest(name=name):
                self.assertFalse(_alias_names_match(name, 'Roderick John Langford'))

    def test_registrable_domains_include_multilabel_and_private_suffixes(self):
        for email, domain in [
                ('alias@' + 'connect.fictional.co.uk', 'fictional.co.uk'),
                ('alias@' + 'dept.university.ac.uk', 'university.ac.uk'),
                ('alias@' + 'dept.example.co.uk', 'example.co.uk'),
                ('alias@' + 'dept.other.co.uk', 'other.co.uk'),
                ('alias@' + 'sub.author.github.io', 'author.github.io'),
                ('alias@' + 'sub.other.github.io', 'other.github.io'),
                ('alias@' + 'a.b.ck', 'a.b.ck'),
                ('alias@' + 'sub.www.ck', 'www.ck'),
                ('alias@' + 'dept.bücher.de', 'xn--bcher-kva.de'),
                ('alias@' + 'CO.UK', None), ('alias@localhost', None), ('', None)]:
            with self.subTest(email=email):
                self.assertEqual(_registrable_domain(email), domain)

    def test_domain_matching_does_not_merge_different_co_uk_institutions(self):
        def addr(email):
            return {'emailAddress': {'address': email}}
        people=[{'id':'a','name':'Scholar A','emails':['a@' + 'first.co.uk']},
                {'id':'b','name':'Scholar B','emails':['b@' + 'second.co.uk']}]
        sent={'toRecipients':[addr('a@' + 'first.co.uk')], 'ccRecipients':[addr('b@' + 'second.co.uk')]}
        self.assertEqual(_alias_recipient({'from':addr('alias@' + 'dept.second.co.uk')},
                                         sent, people, 'me@sample.example.invalid')['id'], 'b')
        self.assertIsNone(_alias_recipient({'from':addr('alias@' + 'third.co.uk')},
                                          sent, people, 'me@sample.example.invalid'))

    def test_duplicate_name_matches_do_not_silently_choose_first_recipient(self):
        def addr(email):
            return {'emailAddress': {'address': email, 'name': 'Roderick Langford'}}
        people=[{'id':'a','name':'Roderick Langford','emails':['a@example.invalid']},
                {'id':'b','name':'Roderick Langford','emails':['b@example.invalid']}]
        sent={'toRecipients':[addr('a@example.invalid')], 'ccRecipients':[addr('b@example.invalid')]}
        self.assertIsNone(_alias_recipient({'from':addr('alias@example.invalid')},
                                          sent, people, 'me@sample.example.invalid'))
