#!/bin/sh
# Refresh OS roots before any build downloads; Rustup carries its own TLS roots.
set -eu
here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
case "${1:-}" in
    ''|--rustup) ;;
    *) echo 'usage: refresh-trust.sh [--rustup]' >&2; exit 1 ;;
esac
work=$(mktemp -d)
trap 'rm -rf "$work"' 0
trap 'exit 1' HUP INT TERM

verify_overrides() {
    for override in "${SSL_CERT_FILE:-}" "${CURL_CA_BUNDLE:-}" "${CARGO_HTTP_CAINFO:-}" "${GIT_SSL_CAINFO:-}"; do
        if [ -n "$override" ]; then
            sh "$here/verify-ca-bundle.sh" "$override"
        fi
    done
}

# Check caller input before repairing any bundle it might alias.
verify_overrides

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
                "/etc/ca-certificates.conf"
            as_root update-ca-certificates --fresh
            bundle="/etc/ssl/certs/ca-certificates.crt"
            if [ "$(id -u)" -eq 0 ]; then
                rm -rf "/var/lib/apt/lists/"*
            fi
        elif command -v dnf >/dev/null; then
            bundle="/etc/pki/tls/certs/ca-bundle.crt"
            # EL8 calls this blacklist; newer p11-kit releases use blocklist.
            # Persist the distrust policy so package upgrades cannot restore roots.
            for policy in blacklist blocklist; do
                trust_dir="/etc/pki/ca-trust/source/$policy"
                as_root mkdir -p "$trust_dir"
                as_root cp "$here/blocked-roots/emsign-ecc-root-ca-g3.pem" \
                    "$here/blocked-roots/aaa-certificate-services.pem" "$trust_dir/"
            done
            as_root update-ca-trust
            # Verify before the first package download as well as after it.
            sh "$here/verify-ca-bundle.sh" "$bundle"
            as_root dnf install -y ca-certificates openssl
            as_root update-ca-trust
        else
            echo 'unsupported Linux trust-store manager' >&2; exit 1
        fi
        ;;
    Darwin)
        # Repair the PEM used by build clients without changing the macOS keychain.
        bundle="/etc/ssl/cert.pem"
        sh "$here/verify-ca-bundle.sh" --filter "$work/bundle.pem" "$bundle"
        as_root cp "$work/bundle.pem" "$bundle"
        ;;
    *) echo 'unsupported build host' >&2; exit 1 ;;
esac

sh "$here/verify-ca-bundle.sh" "$bundle"
verify_overrides
bundle=${SSL_CERT_FILE:-$bundle}

export SSL_CERT_FILE="$bundle"
export CURL_CA_BUNDLE="${CURL_CA_BUNDLE:-$bundle}"
export CARGO_HTTP_CAINFO="${CARGO_HTTP_CAINFO:-$bundle}"
export GIT_SSL_CAINFO="${GIT_SSL_CAINFO:-$bundle}"

if [ "$(uname -s)" = Darwin ] && [ -n "${GITHUB_ENV:-}" ]; then
    # Later Actions steps must use the repaired PEM instead of native keychain roots.
    # Linux host paths must not reach Maturin's different container trust store.
    {
        printf 'SSL_CERT_FILE=%s\n' "$SSL_CERT_FILE"
        printf 'CURL_CA_BUNDLE=%s\n' "$CURL_CA_BUNDLE"
        printf 'CARGO_HTTP_CAINFO=%s\n' "$CARGO_HTTP_CAINFO"
        printf 'GIT_SSL_CAINFO=%s\n' "$GIT_SSL_CAINFO"
    } >> "$GITHUB_ENV"
fi

if [ "${1:-}" = "--rustup" ]; then
    rustup_installer=$work/rustup-init.sh
    curl --proto '=https' --tlsv1.2 --cacert "$CURL_CA_BUNDLE" -fsSL https://sh.rustup.rs -o "$rustup_installer"
    sh "$rustup_installer" -y --no-modify-path --default-toolchain none --profile minimal
    export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
    rustup --version | awk '
        $1 == "rustup" && $2 ~ /^[0-9]+\.[0-9]+\.[0-9]+$/ {
            split($2, v, ".")
            ok = v[1] > 1 || (v[1] == 1 && (v[2] > 29 || (v[2] == 29 && v[3] >= 2)))
        }
        END { exit !ok }'
fi
