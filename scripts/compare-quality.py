#!/usr/bin/env python3
"""Compare rqlens artifacts without treating historical risk scores as CI gates."""
import argparse
import json
from pathlib import Path


def measurements(directory):
    def document(name):
        value = json.loads((directory / f"{name}.json").read_text())
        if value["measurement_confidence"]["complete"] is not True:
            raise ValueError(f"{directory}/{name}.json contains incomplete evidence")
        return value

    def payload(name):
        value = document(name)
        return value.get("data", value.get("records", value))

    def mean(rows, field):
        values = [row[field] for row in rows]
        return sum(values) / len(values) if values else None

    api = payload("api_health")
    nodes = [node["data"] for node in payload("map")["graph"]["nodes"]]
    hotspots = [row["score"] for row in payload("hotspots") if row["kind"] == "function"]
    public = sum(row["public_item_count"] for row in api)
    return {
        "tests": payload("correctness_review")["summary"]["test_count"],
        "failed_tests": payload("correctness_review")["summary"]["failed"],
        "line_coverage_percent": payload("coverage")["summary"]["lines"]["percent"],
        "function_coverage_percent": payload("coverage")["summary"]["functions"]["percent"],
        "region_coverage_percent": payload("coverage")["summary"]["regions"]["percent"],
        "branch_coverage_percent": payload("coverage")["summary"]["branches"]["percent"],
        "high_crap_functions": payload("function_risk")["summary"]["high_crap_count"],
        "worst_function_hotspot": max(hotspots, default=None),
        "duplicate_lines": document("clones")["summary"]["duplicated_line_count"],
        "duplication_percent": document("clones")["summary"]["duplication_percent"],
        "escape_hatches": sum(row["total_count"] for row in payload("rust_escape_hatches")),
        "production_reliability_findings": sum(row["scope"] == "production" for row in payload("reliability_findings")),
        "api_documentation_percent": 100 * sum(row["documented_public_item_count"] for row in api) / public if public else None,
        "worst_type_risk": max((row["structural_risk"] for row in payload("type_health")), default=None),
        "cyclic_modules": sum(node["cycle_member"] for node in nodes),
        "worst_module_risk": max((node["total_score"] for node in nodes if node["total_score"] is not None), default=None),
        "mean_locality_risk": mean(payload("locality_metrics"), "locality_risk"),
        "mean_leverage_pressure": mean(payload("leverage_metrics"), "pressure_score"),
        "mean_responsibility_focus": mean(payload("module_cohesion")["records"], "responsibility_focus"),
        "practice_errors": payload("rust_practices")["summary"]["failed_errors"],
        "practice_warnings": payload("rust_practices")["summary"]["failed_warnings"],
        "test_quality_findings": payload("test_quality")["summary"]["finding_count"],
        "architecture_violations": payload("architecture_rules")["summary"]["violation_count"],
    }


def compare(before, after):
    higher_is_better = {
        "tests", "api_documentation_percent", "line_coverage_percent",
        "function_coverage_percent", "region_coverage_percent", "branch_coverage_percent",
        "mean_responsibility_focus",
    }
    rows = {}
    for metric, baseline in before.items():
        current = after[metric]
        if baseline is None or current is None:
            direction = "unavailable"
        elif abs(current - baseline) < 0.0001:
            direction = "unchanged"
        elif (current > baseline) == (metric in higher_is_better):
            direction = "improved"
        else:
            direction = "regressed"
        rows[metric] = {
            "baseline": round(baseline, 4) if baseline is not None else None,
            "current": round(current, 4) if current is not None else None,
            "direction": direction,
        }
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--current", type=Path, default=Path("target/analysis"))
    parser.add_argument("--output", type=Path, default=Path("target/quality-comparison.json"))
    args = parser.parse_args()
    versions = [json.loads((path / "map.json").read_text())["risk_model_version"]
                for path in (args.baseline, args.current)]
    if versions[0] != versions[1]:
        raise SystemExit("cannot compare different risk model versions")
    report = {
        "baseline": str(args.baseline), "current": str(args.current),
        "metrics": compare(measurements(args.baseline), measurements(args.current)),
        "cautions": [
            "Raw duplication includes test code; its percentage can fall while duplicated lines increase.",
            "Risk, locality, leverage and cohesion scores change with Git history and module topology.",
            "More tests do not by themselves establish test quality; compare coverage and findings too.",
            "Unavailable branch coverage is unknown, not zero risk.",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    for name, row in report["metrics"].items():
        print(f"{name}: {row['baseline']} -> {row['current']} ({row['direction']})")


if __name__ == "__main__":
    main()
