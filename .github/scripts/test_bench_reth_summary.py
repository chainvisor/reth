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


class TargetMetricSignificanceTests(unittest.TestCase):
    def test_abba_histograms_need_three_pairs_for_significance(self) -> None:
        baseline_runs = [
            {"value": 1.0, "_values": [1.0, 1.0, 1.0]},
            {"value": 2.0, "_values": [2.0, 2.0, 2.0]},
        ]
        feature_runs = [
            {"value": 2.0, "_values": [2.0, 2.0, 2.0]},
            {"value": 1.5, "_values": [1.5, 1.5, 1.5]},
        ]

        change = MODULE.compute_histogram_target_metric_change(
            baseline_runs,
            feature_runs,
            "test_histogram",
            "decrease",
            "p50",
        )

        self.assertEqual(change["method"], "abba-paired-run-bootstrap")
        self.assertEqual(change["sig"], "neutral")
        self.assertIn("requires at least", change["significance_reason"])

    def test_abba_histograms_report_consistent_three_pair_changes(self) -> None:
        baseline_runs = [
            {"value": 1.0, "_values": [1.0, 1.0, 1.0]},
            {"value": 1.0, "_values": [1.0, 1.0, 1.0]},
            {"value": 1.0, "_values": [1.0, 1.0, 1.0]},
        ]
        feature_runs = [
            {"value": 2.0, "_values": [2.0, 2.0, 2.0]},
            {"value": 2.0, "_values": [2.0, 2.0, 2.0]},
            {"value": 2.0, "_values": [2.0, 2.0, 2.0]},
        ]

        change = MODULE.compute_histogram_target_metric_change(
            baseline_runs,
            feature_runs,
            "test_histogram",
            "decrease",
            "p50",
        )

        self.assertEqual(change["method"], "abba-paired-run-bootstrap")
        self.assertEqual(change["sig"], "bad")
        self.assertEqual(change["pct"], 100.0)
        self.assertEqual(change["ci_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
