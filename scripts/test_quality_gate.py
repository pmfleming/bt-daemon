import json
from pathlib import Path
import tempfile
import unittest

from quality_gate import evaluate
from quality_metrics import compare, measurements


class QualityGateTests(unittest.TestCase):
    def setUp(self):
        self.metrics = {"line_coverage_percent": 50.0, "high_crap_functions": 73, "duplicate_lines": 312}
        self.config = {"schema_version": 1, "metrics": {
            "line_coverage_percent": {"min": 50},
            "high_crap_functions": {"max": 73},
            "duplicate_lines": {"max": 312},
        }}

    def test_boundaries_pass_and_each_regression_fails_independently(self):
        self.assertEqual(evaluate(self.metrics, self.config), [])
        for metric, value in [("line_coverage_percent", 49.99), ("high_crap_functions", 74), ("duplicate_lines", 313)]:
            with self.subTest(metric=metric):
                failures = evaluate({**self.metrics, metric: value}, self.config)
                self.assertEqual(len(failures), 1)
                self.assertIn(metric, failures[0])

    def test_unknown_and_non_finite_evidence_never_passes(self):
        for value in [None, float("nan"), float("inf"), -float("inf"), True, "51"]:
            with self.subTest(value=value):
                failures = evaluate({**self.metrics, "line_coverage_percent": value}, self.config)
                self.assertEqual(len(failures), 1)
        report = compare({"branch_coverage_percent": None}, {"branch_coverage_percent": None})
        self.assertEqual(report["branch_coverage_percent"]["direction"], "unavailable")
        self.assertIsNone(report["branch_coverage_percent"]["current"])

    def test_invalid_configuration_never_disables_the_gate(self):
        configs = [None, [], {}, {"schema_version": True, "metrics": {}}, {"schema_version": 1, "metrics": {}},
                   {"schema_version": 1, "metrics": {"typo": {"min": 0}}}]
        configs.extend({"schema_version": 1, "metrics": {"line_coverage_percent": bounds}}
                       for bounds in [{}, {"minimum": 5}, {"min": True}, {"max": float("nan")}, {"min": 100, "max": 50}])
        for config in configs:
            with self.subTest(config=config), self.assertRaises(ValueError):
                evaluate(self.metrics, config)

    def test_incomplete_missing_and_malformed_artifacts_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with self.assertRaises(FileNotFoundError):
                measurements(path)
            api = path / "api_health.json"
            api.write_text(json.dumps({"measurement_confidence": {"complete": False}, "records": []}))
            with self.assertRaisesRegex(ValueError, "incomplete evidence"):
                measurements(path)
            api.write_text("{malformed")
            with self.assertRaises(json.JSONDecodeError):
                measurements(path)


if __name__ == "__main__":
    unittest.main()
