#!/bin/sh
# Repair AlmaLinux manylinux_2_28 x86_64 trust before Maturin's tool downloads.
set -eu
here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
# The image presets certifi's private bundle; select our repaired OS store instead.
# Other caller-supplied paths remain subject to refresh-trust's strict checks.
case "${SSL_CERT_FILE:-}" in
    "/opt/_internal/certs.pem") unset SSL_CERT_FILE ;;
esac
sh "$here/refresh-trust.sh" --rustup
export SSL_CERT_FILE="${SSL_CERT_FILE:-/etc/pki/tls/certs/ca-bundle.crt}"
export CURL_CA_BUNDLE="${CURL_CA_BUNDLE:-$SSL_CERT_FILE}"
export CARGO_HTTP_CAINFO="${CARGO_HTTP_CAINFO:-$SSL_CERT_FILE}"
export GIT_SSL_CAINFO="${GIT_SSL_CAINFO:-$SSL_CERT_FILE}"
exec "$@"
