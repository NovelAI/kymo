#!/bin/sh
# Preload the bundled wasm from index.html, so its download starts with the page instead of after the JS glue has loaded and run.
# WebKit downloads a preloaded wasm twice (its preload never matches the glue's fetch; dx omits this preload for that reason), so only Chromium and Gecko user agents get the tag.
# The tag opens <head>: an inline script runs only after the style sheets and blocking scripts before it.
# Usage: preload_wasm.sh <dx bundle public dir>
set -eu
cd "$1"
set -- assets/*.wasm
if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
    echo "preload_wasm: expected exactly one wasm under $(pwd)/assets" >&2
    exit 1
fi
TAG='<script>/(Chrome|Firefox)\//.test(navigator.userAgent)&&document.head.appendChild(Object.assign(document.createElement("link"),{rel:"preload",as:"fetch",href:"/'"$1"'",crossOrigin:"anonymous"}))</script>' \
    perl -0pi -e 's#(<head(?:\s[^>]*)?>)#$1$ENV{TAG}# or die "preload_wasm: index.html has no <head>\n"' index.html
grep -q 'rel:"preload"' index.html
