#!/bin/sh
# Refresh OS roots before any build downloads; Rustup carries its own TLS roots.
set -eu
here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
case "${1:-}" in
    ''|--rustup) ;;
    *) echo 'usage: refresh-trust.sh [--rustup]' >&2; exit 1 ;;
esac

as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

case "$(uname -s)" in
    Linux)
        if command -v apt-get >/dev/null; then
            as_root apt-get update
            as_root apt-get install -y --no-install-recommends ca-certificates openssl
            as_root sed -i \
                -e 's|^mozilla/emSign_ECC_Root_CA_-_G3.crt$|!&|' \
                -e 's|^mozilla/Comodo_AAA_Services_root.crt$|!&|' \
                /etc/ca-certificates.conf
            as_root update-ca-certificates --fresh
            bundle=/etc/ssl/certs/ca-certificates.crt
            if [ "$(id -u)" -eq 0 ]; then
                rm -rf /var/lib/apt/lists/*
            fi
        elif command -v dnf >/dev/null; then
            # Patched RPM bundles must already omit the roots; verify below.
            as_root dnf install -y ca-certificates openssl
            as_root update-ca-trust
            bundle=/etc/pki/tls/certs/ca-bundle.crt
        else
            echo 'unsupported Linux trust-store manager' >&2; exit 1
        fi
        ;;
    Darwin)
        # macOS runners must provide a patched bundle; do not change their keychain.
        bundle=/etc/ssl/cert.pem
        ;;
    *) echo 'unsupported build host' >&2; exit 1 ;;
esac

sh "$here/verify-ca-bundle.sh" "$bundle"
if [ -n "${SSL_CERT_FILE:-}" ]; then
    sh "$here/verify-ca-bundle.sh" "$SSL_CERT_FILE"
    bundle=$SSL_CERT_FILE
fi
if [ -n "${CURL_CA_BUNDLE:-}" ]; then
    sh "$here/verify-ca-bundle.sh" "$CURL_CA_BUNDLE"
fi
if [ -n "${CARGO_HTTP_CAINFO:-}" ]; then
    sh "$here/verify-ca-bundle.sh" "$CARGO_HTTP_CAINFO"
fi

if [ "${1:-}" = "--rustup" ]; then
    rustup_installer=$(mktemp)
    trap 'rm -f "$rustup_installer"' EXIT HUP INT TERM
    curl --proto '=https' --tlsv1.2 --cacert "$bundle" -fsSL https://sh.rustup.rs -o "$rustup_installer"
    sh "$rustup_installer" -y --no-modify-path --default-toolchain none
    rm "$rustup_installer"
    trap - EXIT HUP INT TERM
    rustup --version | awk '
        $1 == "rustup" && $2 ~ /^[0-9]+\.[0-9]+\.[0-9]+$/ {
            split($2, v, ".")
            ok = v[1] > 1 || (v[1] == 1 && (v[2] > 29 || (v[2] == 29 && v[3] >= 2)))
        }
        END { exit !ok }'
fi
