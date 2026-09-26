# Local transport qualification

This standalone harness gates the transport assumptions in the AI-1433 local-deployment plan before they become launcher architecture. It is intentionally outside the main Cargo workspace: `postgresql_embedded` currently depends on SQLx 0.9, while `kymo-server` uses SQLx 0.8, and a release qualification tool should not add that duplicate dependency or database downloads to normal server builds.

Artifact versions and hashes come from `../../shared/local-runtime-artifacts.json`, while database configuration comes from `../../local-runtime/core`. The qualification harness and launcher cannot silently drift to different release inputs or laptop profiles.

The browser fixtures share `fences_common.py` for the frozen WebSocket envelope and a two-animation-frame barrier. It has no protobuf or Playwright imports: each fixture owns its generated messages, response behavior, and RPC waiting rules. New fixtures should reuse this module rather than introducing another framing implementation. Run its golden wire vectors with `python -m unittest -v test_fences_common`.

## Frozen inputs

- `postgresql_embedded` 0.21.0.
- PostgreSQL archive 17.10.0 from `theseus-rs/postgresql-binaries`.
- ClickHouse 25.3.14.14 LTS, matching the exact production server version when this harness was introduced.
- ClickHouse crate 0.13.3, matching `kymo-server`.
- grpcio and httpx from the supported `kymo` dependency range.

`download_clickhouse.py` reads the frozen official GitHub release asset, exact byte size, and SHA-256 for each target from the shared catalog. The Rust lockfile freezes the harness dependencies.

## What it proves

1. `postgresql_embedded` installs PostgreSQL 17, starts it with `listen_addresses=''`, connects through its generated Unix-socket URL, exposes no TCP listener, creates a mode-private socket, stops, restarts the same data directory, and reads a persisted marker.
2. The exact ClickHouse binary starts with only one loopback HTTPS listener, a disabled default user, and a generated certificate trusted as the sole Rustls root by a custom Hyper client passed to `clickhouse::Client::with_http_client`. Missing credentials, a different certificate, and plaintext HTTP all fail. `SYSTEM SHUTDOWN` exits the directly supervised process group.
3. tonic and grpcio exchange unary and bidi messages over a mode-0600 Unix socket. Axum and httpx exchange bounded and streaming bodies over a second socket. Oversized messages are rejected, cancellation reaches the tonic handler, and an overlong socket path fails.

The test sets `CLICKHOUSE_WATCHDOG_ENABLE=0`. Without it, ClickHouse forks a watchdog arrangement and the spawned process handle does not prove ownership of the surviving server. Standalone ClickHouse configuration omits disabled listener elements entirely; merge-only `remove="1"` elements are invalid in a standalone file.

grpcio uses `unix:///absolute/path` plus `grpc.default_authority=localhost`. The explicit authority is required for interoperable HTTP/2 headers with tonic on the tested macOS grpcio build.

## Run locally

Install the `kymo` Python dependencies, ensure `protoc` and `lsof` are available, then run:

```sh
python download_clickhouse.py --output /tmp/kymo-clickhouse-25.3.14.14
cargo run --locked -- \
  --clickhouse-binary /tmp/kymo-clickhouse-25.3.14.14 \
  --python "$(command -v python3)"
```

Pass `--state-dir PATH` to retain the downloaded PostgreSQL installation between runs. Use a short path because filesystem Unix sockets have a platform-specific path-length limit.

## Browser fences

The editor dialog checks run locally with Playwright and a project containing a numeric chart, at least two sections, and an active sidebar run for the colour-picker case:

```sh
python editor_dialog_fences.py <dashboard-project-url>
```

Run them after editor, focus, or dialog changes. They cover native modality, dismissal, focus return, label activation, option inheritance and persistence, and smoothing hints at wide and narrow widths. They are excluded from CI to keep its runtime down. Run `user_settings_fences.py <dashboard-origin>` as well when changing the shared dialog or focus helpers.

## Target status

| Target | Status | Evidence |
|---|---|---|
| macOS arm64 | Qualified | CI run 31419177340 and a local macOS 26.5.2 run, 2026-08-10. |
| Linux x86_64 | Qualified | CI run 31419177340, 2026-08-10. |

Linux x86_64 runs on every relevant push and pull request. macOS arm64 remains available through the `macos_arm64` workflow-dispatch input and the local command above, but does not run automatically. Every artifact-catalog pin change must be followed by a successful opt-in macOS arm64 dispatch before the recorded macOS qualification is considered current. A target is not supported by the launcher until its qualification passes with the frozen database artifacts. Do not replace a failing private transport with unauthenticated loopback.

## Frontend checks

See the CI workflows for coverage. Pointer and focus changes require Kevin's hand test on `dx serve` before deployment.

`--no-browser` is a hidden diagnostic flag that prints the URL without opening a browser.

From `kymo/`, use an isolated local runtime (`KYMO_LOCAL_ROOT` and `KYMO_SPOOL_DIR` set to disposable directories), seed and open its fixture pages:

```sh
python tools/local-transport-qualification/seed_browser_fixture.py
python tools/local-transport-qualification/seed_zoom_fixture.py
kymo open browser-e2e browser-e2e --no-browser
kymo open zoom-e2e zoom-fixture --no-browser
```

Use the printed local origin for these commands; the gesture URL is a project page:

```sh
python tools/local-transport-qualification/gesture_fences.py http://127.0.0.1:PORT/browser-e2e
python tools/local-transport-qualification/gesture_fences.py http://127.0.0.1:PORT/browser-e2e --browser webkit
python tools/local-transport-qualification/user_settings_fences.py http://127.0.0.1:PORT/
python tools/local-transport-qualification/legacy_storage_fences.py http://127.0.0.1:PORT/
python tools/local-transport-qualification/zoom_fences.py http://127.0.0.1:PORT/zoom-e2e/zoom-fixture
```

The gesture, user settings, legacy storage, zoom, editor dialog, first-mount, and text scroll fences accept `--browser chromium|firefox|webkit` (default: `chromium`).

WebKit exercises native checkbox activation on auxiliary clicks that Chromium does not produce. The gesture fence suppresses native context menus to keep test input flowing; Kevin must check the actual macOS Ctrl-click menu by hand.
