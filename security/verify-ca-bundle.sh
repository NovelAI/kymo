#!/bin/sh
# Inspect DER fingerprints; --filter prepares a clean PEM for OS repair.
set -eu
output=''
if [ "${1:-}" = '--filter' ]; then
    if [ "$#" -ne 3 ] || [ -z "$2" ]; then
        echo 'usage: verify-ca-bundle.sh --filter NEW_OUTPUT_FILE BUNDLE' >&2
        exit 1
    fi
    output=$2
    shift 2
fi
if [ "$#" -eq 0 ]; then
    set -- "/etc/ssl/certs/ca-certificates.crt"
fi
cert_dir=$(mktemp -d)
trap 'rm -rf "$cert_dir"' 0
trap 'exit 1' HUP INT TERM
for bundle do
    awk -v directory="$cert_dir" '
        /-----BEGIN / && $0 !~ /^-----BEGIN CERTIFICATE-----\r?$/ { invalid=1; exit 1 }
        /-----END / && $0 !~ /^-----END CERTIFICATE-----\r?$/ { invalid=1; exit 1 }
        /^-----BEGIN CERTIFICATE-----\r?$/ {
            if (file != "") { invalid=1; exit 1 }
            count++; file=sprintf("%s/certificate-%06d.pem", directory, count)
        }
        file != "" { print > file }
        /^-----END CERTIFICATE-----\r?$/ {
            if (file == "") { invalid=1; exit 1 }
            close(file); file=""
        }
        END { if (invalid || count == 0 || file != "") exit 1 }
    ' "$bundle"
    for certificate in "$cert_dir"/certificate-*.pem; do
        fingerprint=$(openssl x509 -in "$certificate" -noout -sha256 -fingerprint)
        fingerprint=${fingerprint#*=}
        fingerprint=$(printf '%s' "$fingerprint" | tr -d ':' | tr '[:upper:]' '[:lower:]')
        case "$fingerprint" in
            *[!0-9a-f]*|'') echo "Invalid CA fingerprint in $bundle" >&2; exit 1 ;;
        esac
        test "${#fingerprint}" -eq 64
        case "$fingerprint" in
            86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b|d7a7a0fb5d7e2731d771e9484ebcdef71d5f0c3e0a2948782bc83ee0ea699ef4)
                if [ -z "$output" ]; then
                    echo "Affected CA fingerprint remains in $bundle" >&2
                    exit 1
                fi
                continue ;;
        esac
        if [ -n "$output" ]; then
            cat "$certificate" >> "$cert_dir/bundle.pem"
        fi
    done
    if [ -z "$output" ]; then
        printf 'Verified CA bundle: %s\n' "$bundle"
    else
        if [ ! -s "$cert_dir/bundle.pem" ]; then
            echo "No trusted certificates remain in $bundle" >&2
            exit 1
        fi
        # Publish only after every certificate has parsed successfully.
        if [ -e "$output" ] || [ -L "$output" ]; then
            echo "Output already exists: $output" >&2
            exit 1
        fi
        mv "$cert_dir/bundle.pem" "$output"
    fi
    rm -f "$cert_dir"/certificate-*.pem
done
