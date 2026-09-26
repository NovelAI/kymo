# kymo local runtime

For installing and using local mode, see the [kymo README](https://github.com/NovelAI/kymo#readme). This file is for developers of the runtime.

This platform wheel owns the native `kymo` and `kymo-server` commands plus the frozen PostgreSQL and ClickHouse artifacts used by local deployment. It provides durable installation identity, verified database installation, read-only diagnostics, and a per-user supervisor for the local stack.

For isolated development and tests, set `KYMO_LOCAL_ROOT` to put every managed directory below one private root. `MKDB2_LOCAL_ROOT` is rejected after the cutover.

```text
kymo status
kymo install
kymo ensure --json
kymo open [--no-browser] [<project-id> [<run-id>]]
kymo start
kymo doctor
kymo stop [--hold]
kymo ports [--dashboard <port> --cdn <port>]
```

`ensure` installs missing artifacts unless `KYMO_LOCAL_NO_INSTALL=1`, starts one stack when stopped, and returns the current installation UUID, endpoint generation, private client sockets, loopback browser origins, protocol range, and ephemeral server bearer. `MKDB2_LOCAL_NO_INSTALL` is rejected. Its JSON output is machine-oriented secret material and must not be logged. `start` is `ensure` without the JSON. Concurrent or repeated calls reuse the running supervisor.

The supervisor starts PostgreSQL 17 on a private Unix socket, ClickHouse on pinned HTTPS, and `kymo-server` with separate native, upload, dashboard, and CDN transports. Lifecycle, recovery, `stop`/`--hold`, ports, idle shutdown, and the trust boundary are specified in [local deployment](https://github.com/NovelAI/kymo/blob/main/docs/local-deployment.md#runtime-identity-ports-and-lifecycle).

Generation credentials are distinct and private. ClickHouse intentionally binds `127.0.0.1`, while the server connects to `localhost`: the generated certificate contains `subjectAltName=DNS:localhost`, which rustls/webpki requires and does not infer from the common name.

On macOS, short-lived sockets live below the canonical per-user temporary directory returned by the standard library's `_CS_DARWIN_USER_TEMP_DIR` path. Linux uses a validated private `XDG_RUNTIME_DIR`, with a private `/tmp/kymo-<uid>` fallback. Persistent database data and generation secrets remain in the managed data and state roots.

The artifact catalog is `../shared/local-runtime-artifacts.json`. Both this installer and the transport qualification harness consume it. The launcher also owns the shared laptop database profiles through `core`; changes to versions, exact sizes, hashes, listeners, or resource settings therefore re-run both workflows.

The official macOS ClickHouse asset is a self-extracting executable: invoking `--version` expands the downloaded 107 MB stub in place to roughly 560 MB. The catalog's `installed_sha256` is the digest after that required expansion, so preparation must happen before installed-file validation.

Manifest format 1 is still pre-release. Its compatibility rule lives beside [`MANIFEST_FORMAT_VERSION`](https://github.com/NovelAI/kymo/blob/main/local-runtime/core/src/manifest.rs). A pre-release installation with an incompatible protocol is not migrated: delete it and reinstall.

## Removing data, backup, and restore

There is no uninstall, reset, backup, or restore command. Use `kymo doctor --json` to record the resolved managed roots before deleting anything. The runtime creates:

- macOS data and state below `~/Library/Application Support/ai.NovelAI.kymo-local-runtime/`, and cache at `~/Library/Caches/ai.NovelAI.kymo-local-runtime`;
- Linux data below `${XDG_DATA_HOME:-~/.local/share}/kymo-local-runtime/data`, state below `${XDG_STATE_HOME:-~/.local/state}/kymo-local-runtime` (falling back to the local-data directory's `state` child), and cache below `${XDG_CACHE_HOME:-~/.cache}/kymo-local-runtime`;
- with `KYMO_LOCAL_ROOT=/absolute/path`, exactly `/absolute/path/data`, `/absolute/path/state`, and `/absolute/path/cache` instead of those platform roots;
- Python delivery spools at `$KYMO_SPOOL_DIR` or `~/.cache/kymo/spool`; no-argument `kymo-sync` also scans the pre-cutover `~/.cache/pymkdb2/spool` backlog;
- macOS sockets below `_CS_DARWIN_USER_TEMP_DIR/kymo-<uid>/<state-digest>`, or Linux sockets below `$XDG_RUNTIME_DIR/kymo-<uid>/<state-digest>` with canonical `/tmp/kymo-<uid>/<state-digest>` as the fallback, where `<state-digest>` is the first 12 hex digits of the state root path's SHA-256 (a copied installation shares the UUID, so the UUID cannot key it).

Component logs live in the state root and keep one current file plus three 8 MiB rotations, capped at 32 MiB per component.

Uninstall, cold backup, and restore procedures are in [local deployment](https://github.com/NovelAI/kymo/blob/main/docs/local-deployment.md#data-locations-deletion-and-backup).
