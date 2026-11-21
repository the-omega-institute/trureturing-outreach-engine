from pathlib import Path
import unittest


class OutreachWorkflowTests(unittest.TestCase):
    def test_tick_reconciles_receipts_nonfatally_without_running_author_watch(self):
        workflow = (Path(__file__).resolve().parents[2] / '.github' / 'workflows' /
                    'outreach-control.yml').read_text()
        self.assertIn('name: Reconcile receipts', workflow)
        receipt_step = workflow.split('name: Reconcile receipts', 1)[1]
        receipt_step = receipt_step.split('name: Synchronize feed', 1)[0]
        self.assertIn('continue-on-error: true', receipt_step)
        self.assertNotIn('managed.arxiv_watch', workflow)


if __name__ == '__main__':
    unittest.main()
