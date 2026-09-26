#!/usr/bin/env bash
# Clean-install smoke test: install the built wheels, start local mode, log a point, and fetch the run's dashboard page.
#   scripts/smoke.sh DIST...   directories holding the kymo and kymo-local-runtime wheels
set -euo pipefail
work=$(mktemp -d)
export KYMO_LOCAL_ROOT="$work/root"
# The client runs the `kymo` launcher from PATH.
export PATH="$work/venv/bin:$PATH"
trap 'kymo stop || true; rm -rf "$work"' EXIT
python -m venv "$work/venv"
"$work/venv/bin/pip" install --quiet $(find "$@" -name '*.whl')
# A real file, not stdin: kymo's upload process re-imports the script on macOS.
cat >"$work/smoke.py" <<'PY'
import urllib.request

import kymo

if __name__ == "__main__":
    kymo.init(mode="local", project_id="smoke", run_name="smoke", system_metrics=False)
    kymo.log({"loss": 1.0}, step=0)
    url = kymo.run_url()
    page = urllib.request.urlopen(url, timeout=60).read()
    assert b"kymo-runtime-config" in page, page[:300]
    kymo.finish()
    print("smoke: dashboard served at", url)
PY
"$work/venv/bin/python" "$work/smoke.py"
