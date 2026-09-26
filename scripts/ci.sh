#!/usr/bin/env bash
# Lint and test every section below, or only the named ones: scripts/ci.sh [SECTION]...
# Needs Rust with clippy, rustfmt, and the wasm32-unknown-unknown target; protoc; and Python 3.10+ with pip.
set -euo pipefail
cd "$(dirname "$0")/.."

root() {
    cargo fmt --all -- --check
    cargo clippy --locked --workspace --all-targets -- -D warnings
    cargo test --locked --workspace
    cargo test --locked --package kymo-frontend --features local-runtime
}

wasm() {
    cargo clippy --locked --package kymo-frontend --target wasm32-unknown-unknown --features local-runtime -- -D warnings
    cargo check --locked --package kymo-frontend --target wasm32-unknown-unknown
}

local-runtime() {
    cd local-runtime
    cargo fmt --all -- --check
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
    cargo test --locked --workspace --all-targets --all-features
}

qualification() {
    cd tools/local-transport-qualification
    cargo fmt --all -- --check
    cargo clippy --locked --all-targets -- -D warnings
    cargo test --locked --workspace --all-targets
    python -m unittest test_download_clickhouse test_fences_common
}

client() {
    python -m pip install --quiet -e 'python_client[dev]'
    cd python_client
    python generate_proto.py --check
    python -m unittest test_env test_generate_proto test_local_runtime test_local_integration \
        test_pipelined_upload test_spool_replay test_graceful_shutdown test_manual_workloads
}

[ $# -gt 0 ] || set -- root wasm local-runtime qualification client
for section in "$@"; do
    declare -F "$section" >/dev/null || { echo "ci.sh: unknown section $section" >&2; exit 2; }
    ("$section")
done
