#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Mutate pure policy decisions, not live Bluetooth transports. cargo-mutants uses
# scratch source copies; never use --in-place with an operator's working tree.
cargo mutants --no-config --all-features \
    --file src/bluez/operations.rs \
    --file src/bluez/snapshot.rs \
    --file src/fast_pair/capabilities.rs \
    --re 'audio_route_required|cache_entry_is_fresh|should_include_device|provisioning_reason' \
    --jobs 1 --timeout 120 --build-timeout 300 \
    --output target/mutation-policy
