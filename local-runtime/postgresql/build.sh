#!/usr/bin/env bash
# Build the PostgreSQL archive local mode installs (the catalog's `postgresql` artifacts): upstream source without optional libraries, relocatable, so it needs only the C library and /usr/share/zoneinfo from the host.
# Usage: build.sh <linux-x86_64|macos-arm64> <output directory>. Linux builds run in quay.io/pypa/manylinux_2_28_x86_64, so the result needs glibc 2.28 like the wheel. Building needs a C compiler, make, bison, flex and Perl (release tarballs no longer ship the generated parsers), plus patchelf on Linux.
set -euo pipefail

POSTGRESQL_VERSION=17.11
SOURCE_SHA256=5367f6fb2ec97efe1eb2e0c7926bb33438e51b0bd3a9733b88498056a7dc9a7e
# The catalog version is the release plus this build number; bump it when the recipe changes an unchanged release's bytes, since artifact directories are keyed by version.
BUILD=0

target=$1
output=$(cd "$2" && pwd)
name=postgresql-$POSTGRESQL_VERSION.$BUILD-$target
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"

curl --fail --silent --show-error --location --output source.tar.gz \
    "https://ftp.postgresql.org/pub/source/v$POSTGRESQL_VERSION/postgresql-$POSTGRESQL_VERSION.tar.gz"
if command -v sha256sum >/dev/null; then
    digest=$(sha256sum <source.tar.gz)
else
    digest=$(shasum -a 256 <source.tar.gz)
fi
if [ "${digest%% *}" != "$SOURCE_SHA256" ]; then
    echo "PostgreSQL source SHA-256 is ${digest%% *}, expected $SOURCE_SHA256" >&2
    exit 1
fi
tar -xzf source.tar.gz
cd "postgresql-$POSTGRESQL_VERSION"
# Read only on macOS; the wheel's floor.
export MACOSX_DEPLOYMENT_TARGET=11.0
# A prefix containing "postgresql" installs modules and data flat into lib/ and share/, the layout the launcher runs; PostgreSQL locates them relative to its binaries wherever the tree is extracted.
# Leaving out ICU, readline and zlib (the defaults that need host libraries) leaves the core server; kymo's local profile uses none of them. Timezones keep resolving from the host, as in the builds installations were created with.
prefix=$work/$name
./configure --prefix="$prefix" --disable-rpath --without-icu --without-readline --without-zlib \
    --with-system-tzdata=/usr/share/zoneinfo
make -j"$(getconf _NPROCESSORS_ONLN)"
make install
# Extension-building support, whose test executables would still name the build prefix.
rm -rf "$prefix/lib/pgxs"

# Point every binary and library at its own tree instead of the build prefix.
case $target in
linux-x86_64)
    for file in "$prefix"/bin/* "$prefix"/lib/*.so*; do
        [ -L "$file" ] || patchelf --set-rpath '$ORIGIN/../lib' "$file"
    done
    ;;
macos-arm64)
    for file in "$prefix"/bin/* "$prefix"/lib/*.dylib; do
        [ -L "$file" ] && continue
        case $file in
        */lib/lib*.dylib) install_name_tool -id "@loader_path/../lib/$(basename "$file")" "$file" ;;
        esac
        otool -L "$file" | awk -v lib="$prefix/lib/" 'NR > 1 && index($1, lib) == 1 { print $1 }' | while read -r dependency; do
            install_name_tool -change "$dependency" "@loader_path/../lib/$(basename "$dependency")" "$file"
        done
        # install_name_tool invalidates the linker's ad-hoc signature, and arm64 refuses to run unsigned code.
        codesign --force --sign - "$file"
    done
    ;;
*)
    echo "unsupported target $target" >&2
    exit 1
    ;;
esac

# macOS tar would otherwise add AppleDouble "._" entries for extended attributes, including a second root beside the tree, which the launcher's single-root extraction rejects.
COPYFILE_DISABLE=1 tar --no-xattrs -czf "$output/$name.tar.gz" -C "$work" "$name"
echo "$output/$name.tar.gz"
