#!/bin/sh
# Inspect actual DER fingerprints, independent of certificate names or filenames.
set -eu
if [ "$#" -eq 0 ]; then
    set -- /etc/ssl/certs/ca-certificates.crt
fi
cert_dir=$(mktemp -d)
trap 'rm -rf "$cert_dir"' 0
for bundle do
    awk -v directory="$cert_dir" '
        /-----BEGIN CERTIFICATE-----/ { count++; file=directory "/certificate-" count ".pem" }
        file != "" { print > file }
        /-----END CERTIFICATE-----/ { close(file); file="" }
        END { if (count == 0) exit 1 }
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
                echo "Affected CA fingerprint remains in $bundle" >&2
                exit 1 ;;
        esac
    done
    printf 'Verified CA bundle: %s\n' "$bundle"
    rm -f "$cert_dir"/certificate-*.pem
done
