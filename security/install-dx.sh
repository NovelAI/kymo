#!/bin/sh
# Build CLI 0.7.9 with patched TLS dependencies; keep the application framework unchanged.
# Refresh OS trust and the active rustup before invoking this script.
set -eu

here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
prefix=${1:?usage: install-dx.sh INSTALL_PREFIX}
for tool in rustup curl sha256sum tar patch cargo awk; do
    command -v "$tool" >/dev/null || { echo "required build tool missing: $tool" >&2; exit 1; }
done

# Reject unrefreshed builder or override bundles before fetching the source archive.
sh "$here/verify-ca-bundle.sh" /etc/ssl/certs/ca-certificates.crt
for bundle in "${SSL_CERT_FILE:-}" "${CURL_CA_BUNDLE:-}" "${CARGO_HTTP_CAINFO:-}"; do
    if [ -n "$bundle" ]; then
        sh "$here/verify-ca-bundle.sh" "$bundle"
    fi
done
rustup --version | awk '
    $1 == "rustup" && $2 ~ /^[0-9]+\.[0-9]+\.[0-9]+$/ {
        split($2, v, ".")
        ok = v[1] > 1 || (v[1] == 1 && (v[2] > 29 || (v[2] == 29 && v[3] >= 2)))
    }
    END { exit !ok }'

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
source_dir=$work/source
mkdir "$source_dir"
curl --proto '=https' --tlsv1.2 -fsSL \
    https://github.com/DioxusLabs/dioxus/archive/3e43ffa6cf1488d5388c888eb5f2494a006ae6de.tar.gz \
    -o "$work/source.tar.gz"
printf '%s  %s\n' fe46f105f4ade27c6dad76bfc19475190a3724600e3cc0bf46aaaccaadb1b406 "$work/source.tar.gz" | sha256sum -c -
tar -xzf "$work/source.tar.gz" --strip-components=1 --no-same-owner -C "$source_dir"
patch --batch --forward -p1 -d "$source_dir" < "$here/dx-0.7.9-tls.patch"

# Preserve CLI 0.7.9 and the reviewed patched dependency lock.
cargo install --locked --path "$source_dir/packages/cli" --root "$prefix" --force
"$prefix/bin/dx" --version
