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

audit:
    cargo audit

coverage:
    cargo llvm-cov --all-features --fail-under-lines 34

check: fmt-check lint test audit coverage

probe:
    cargo run -- probe-bluez

hardware-smoke:
    cargo build
    bash scripts/hardware-smoke.sh target/debug/bt-daemon

nix-check:
    nix flake check --show-trace
