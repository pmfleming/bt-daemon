#!/usr/bin/env python3
"""Fail closed on missing, non-finite, incomplete or out-of-bounds evidence."""
import argparse
import json
import math
from pathlib import Path

from quality_metrics import measurements


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def evaluate(metrics, config):
    if not isinstance(config, dict) or type(config.get("schema_version")) is not int or config["schema_version"] != 1:
        raise ValueError("unsupported quality gate configuration version")
    gates = config.get("metrics")
    if not isinstance(gates, dict) or not gates:
        raise ValueError("quality gates must configure at least one metric")
    failures = []
    for metric, bounds in gates.items():
        if metric not in metrics:
            raise ValueError(f"unknown quality metric: {metric}")
        if not isinstance(bounds, dict) or not bounds or set(bounds) - {"min", "max"}:
            raise ValueError(f"invalid bounds for {metric}")
        if not all(number(bound) for bound in bounds.values()):
            raise ValueError(f"non-numeric bounds for {metric}")
        if "min" in bounds and "max" in bounds and bounds["min"] > bounds["max"]:
            raise ValueError(f"inverted bounds for {metric}")
        value = metrics[metric]
        if not number(value):
            failures.append(f"{metric}: evidence is missing or non-finite")
        elif "min" in bounds and value < bounds["min"]:
            failures.append(f"{metric}: {value} < minimum {bounds['min']}")
        elif "max" in bounds and value > bounds["max"]:
            failures.append(f"{metric}: {value} > maximum {bounds['max']}")
    return failures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts", type=Path, default=Path("target/analysis"))
    parser.add_argument("--config", type=Path, default=Path("quality-gates.json"))
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text())
        metrics = measurements(args.artifacts)
        failures = evaluate(metrics, config)
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemExit(f"quality evidence/configuration error: {error}") from error
    for name, bounds in config["metrics"].items():
        print(f"{name}: {metrics[name]} (bounds {bounds})")
    if failures:
        raise SystemExit("quality regression:\n" + "\n".join(failures))


if __name__ == "__main__":
    main()
