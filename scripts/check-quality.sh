#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

lens="${RQLENS:-rqlens}"
"$lens" measure all --config rqlens.toml
"$lens" verify --config rqlens.toml
"$lens" check --config rqlens.toml \
    --fail-on partial \
    --fail-on test-failure \
    --fail-on practice-failure \
    --fail-on reliability-finding

# Keep the artifact-based gate consistent with `just coverage`, without executing
# hardware probes or discarding poorly covered modules from the denominator.
python3 - <<'PY'
import json
from pathlib import Path

artifact = json.loads(Path("target/analysis/coverage.json").read_text())
if artifact["measurement_confidence"]["complete"] is not True:
    raise SystemExit("coverage evidence is incomplete")
coverage = artifact["data"]["summary"]["lines"]["percent"]
if not isinstance(coverage, (float, int)) or coverage < 40:
    raise SystemExit(f"line coverage {coverage!r} is below 40%")
print(f"Line coverage: {coverage:.2f}% (minimum 40%)")
PY
