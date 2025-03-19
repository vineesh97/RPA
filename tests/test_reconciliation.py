import unittest
from src.reconciliation import run_reconciliation

class TestReconciliation(unittest.TestCase):
    def test_reconciliation(self):
        result = run_reconciliation("2024-01-01", "2024-01-31")
        self.assertIsInstance(result, dict)

if __name__ == "__main__":
    unittest.main()
    #szxdfx
