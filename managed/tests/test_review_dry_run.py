import unittest
from unittest.mock import MagicMock, patch

from managed import control


class ReviewDryRunTests(unittest.TestCase):
    def ledger(self, error=None):
        ledger = MagicMock()
        if error:
            ledger.tasks.verdict.side_effect = error
        return ledger

    def test_rejected_verdict_fails_before_staging(self):
        ledger = self.ledger(ValueError('An agent-held result accepts only BOUNCE or HOLD, never PASS'))
        with patch.object(control, 'Ledger', return_value=ledger), \
                patch.object(control, 'review_key', return_value='secret'):
            with self.assertRaisesRegex(ValueError, 'would be rejected by the coordinator: An agent-held'):
                control.review_dry_run({'verdict': 'PASS'}, object())
        ledger.tasks.close.assert_called_once()
        ledger.save.assert_not_called()

    def test_accepted_verdict_is_not_saved(self):
        ledger = self.ledger()
        with patch.object(control, 'Ledger', return_value=ledger), \
                patch.object(control, 'review_key', return_value='secret'):
            control.review_dry_run({'verdict': 'HOLD'}, object())
        ledger.tasks.verdict.assert_called_once_with({'verdict': 'HOLD'}, 'secret')
        ledger.save.assert_not_called()
        ledger.tasks.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
