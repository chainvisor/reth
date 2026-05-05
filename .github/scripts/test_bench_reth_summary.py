import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("bench-reth-summary.py")
SPEC = importlib.util.spec_from_file_location("bench_reth_summary", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class FmtMetricValueTests(unittest.TestCase):
    def test_keeps_tiny_non_zero_values_visible(self) -> None:
        self.assertEqual(MODULE.fmt_metric_value(2.2294021808143198e-06), "2.229e-06")
        self.assertEqual(MODULE.fmt_metric_value(3.0513028693573048e-05), "3.051e-05")

    def test_preserves_zero_and_integer_like_values(self) -> None:
        self.assertEqual(MODULE.fmt_metric_value(0.0), "0")
        self.assertEqual(MODULE.fmt_metric_value(12.00001), "12")


if __name__ == "__main__":
    unittest.main()
