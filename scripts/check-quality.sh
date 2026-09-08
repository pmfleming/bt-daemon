#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python3 -m unittest discover -s scripts -p 'test_quality_gate.py'

lens="${RQLENS:-rqlens}"
"$lens" measure all --config rqlens.toml
"$lens" verify --config rqlens.toml
"$lens" check --config rqlens.toml \
    --fail-on partial \
    --fail-on test-failure \
    --fail-on practice-failure \
    --fail-on reliability-finding

# Gate stable, actionable measurements, not historical churn or composite risk.
# Keep all source modules in the coverage and duplication denominators.
python3 scripts/quality_gate.py
