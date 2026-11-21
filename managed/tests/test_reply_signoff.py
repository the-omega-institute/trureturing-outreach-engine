"""Reply sign-offs carry the names only; first contacts keep the affiliation."""
import unittest

from fixture_config import use_example_config
from managed.control import reply_signoff_gate


class ReplySignoffTests(unittest.TestCase):
    def test_names_only_pass(self):
        reply_signoff_gate('Dear Professor X,\n\nResult.\n\nBest regards,\nAvery and Blaire\n')

    def test_affiliation_after_closing_rejected(self):
        for line in ('Example University; Example Research Group', 'The Omega Institute'):
            with self.assertRaisesRegex(Exception, 'names only'):
                reply_signoff_gate('Dear X,\n\nResult.\n\nBest regards,\nAvery and Blaire\n' + line + '\n')

    def test_affiliation_in_body_is_not_a_signoff(self):
        reply_signoff_gate('Dear X,\n\nOur colleague at the Example University checked it.\n\n'
                           'Best wishes,\nAvery and Blaire')


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
