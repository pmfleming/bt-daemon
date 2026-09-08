set shell := ["bash", "-euo", "pipefail", "-c"]

default: check

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all-features
    python3 -m unittest discover -s scripts -p 'test_quality_gate.py'

audit:
    cargo audit

coverage:
    cargo llvm-cov --all-features --fail-under-lines "$(python3 -c 'import json; print(json.load(open("quality-gates.json"))["metrics"]["line_coverage_percent"]["min"])')"

quality:
    bash scripts/check-quality.sh

mutations:
    bash scripts/check-mutations.sh

check: fmt-check lint test audit coverage

ci: check quality mutations

probe:
    cargo run -- probe-bluez

hardware-smoke:
    cargo build
    bash scripts/hardware-smoke.sh target/debug/bt-daemon

nix-check:
    nix flake check --show-trace
