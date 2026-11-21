"""A deleted Exchange draft can stay readable by id; only Drafts counts."""
import unittest
from urllib.error import HTTPError

from managed.delivery import _read_live_draft


class Box:
    def __init__(self, message=None, error=None):
        self.value, self.error = message, error

    def message(self, remote_id):
        if self.error:
            raise HTTPError('x', self.error, 'err', {}, None)
        return self.value

    def drafts_folder_id(self):
        return 'drafts-id'


class DraftPresenceTests(unittest.TestCase):
    def test_draft_in_drafts_is_present(self):
        draft = {'id': 'd', 'parentFolderId': 'drafts-id', 'isDraft': True}
        self.assertEqual(_read_live_draft(Box(draft), 'd'), draft)

    def test_deleted_draft_in_recoverable_items_is_absent(self):
        self.assertIsNone(_read_live_draft(Box({'id': 'd', 'parentFolderId': 'recoverable'}), 'd'))

    def test_forbidden_after_delete_is_absent_but_not_before(self):
        self.assertIsNone(_read_live_draft(Box(error=403), 'd', after_delete=True))
        with self.assertRaises(HTTPError):
            _read_live_draft(Box(error=403), 'd')

    def test_not_found_is_absent(self):
        self.assertIsNone(_read_live_draft(Box(error=404), 'd'))


if __name__ == '__main__':
    unittest.main()
