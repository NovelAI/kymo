#!/usr/bin/env bash
# Release build steps, run by .github/workflows/release.yml:
#   scripts/build.sh frontend OUT   offline dashboard bundle for kymo-local-runtime (OUT/public)
#   scripts/build.sh notices        local-runtime/THIRD-PARTY-NOTICES; fails on a license scripts/about.toml does not accept
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

case "${1:-}" in
frontend)
    out=$(mkdir -p "$2" && cd "$2" && pwd)
    # A private target dir keeps earlier hosted or local bundles from leaking stale assets into this one.
    (cd kymo-frontend && CARGO_TARGET_DIR="$out/target" dx bundle --package kymo-frontend --release \
        --features local-runtime --debug-symbols=false --out-dir "$out" --cargo-args=--locked)
    ;;
notices)
    {
        printf 'Third-party software in kymo-local-runtime\n\nThe kymo and kymo-server binaries and the embedded dashboard include the following crates, under the licenses below. Their source is available from crates.io.\n'
        cargo about generate --locked -c scripts/about.toml -m local-runtime/Cargo.toml scripts/about.hbs
        cargo about generate --locked -c scripts/about.toml -m kymo-frontend/Cargo.toml scripts/about.hbs
        for notice in kymo-frontend/assets/vendor/LICENSE-*; do
            printf '\n================================================================================\n%s\n================================================================================\n\n' "${notice##*/}"
            cat "$notice"
        done
    } >local-runtime/THIRD-PARTY-NOTICES
    ;;
*)
    sed -n '2,4p' "$0" >&2
    exit 2
    ;;
esac
