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

check: fmt-check lint test

probe:
    cargo run -- --probe-bluez

nix-check:
    nix flake check --show-trace
