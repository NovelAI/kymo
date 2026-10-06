# Local transport qualification

This standalone harness gates the transport assumptions in the AI-1433 local-deployment plan before they become launcher architecture. It is intentionally outside the main Cargo workspace, so its dependencies stay out of normal server builds. PostgreSQL is qualified through the launcher itself, by `runtime_qualification.py`.

The ClickHouse version and hashes come from `../../shared/local-runtime-artifacts.json`, and its configuration from `../../local-runtime/core`, so the harness and launcher cannot silently drift to different release inputs or laptop profiles.

The browser fixtures share `fences_common.py` for the frozen WebSocket envelope, a two-animation-frame barrier, a handle drag and a wait for a chart to take its container's width. It has no protobuf or Playwright imports: each fixture owns its generated messages, response behavior, and RPC waiting rules. New fixtures should reuse this module rather than introducing another framing implementation. Run its golden wire vectors with `python -m unittest -v test_fences_common`.

## Frozen inputs

- ClickHouse 25.3.14.14 LTS, matching the exact production server version when this harness was introduced.
- ClickHouse crate 0.13.3, matching `kymo-server`.
- grpcio and httpx from the supported `kymo` dependency range.

`download_clickhouse.py` reads the frozen official GitHub release asset, exact byte size, and SHA-256 for each target from the shared catalog. The Rust lockfile freezes the harness dependencies.

## What it proves

1. The exact ClickHouse binary starts with only one loopback HTTPS listener, a disabled default user, and a generated certificate trusted as the sole Rustls root by a custom Hyper client passed to `clickhouse::Client::with_http_client`. Missing credentials, a different certificate, and plaintext HTTP all fail. `SYSTEM SHUTDOWN` exits the directly supervised process group.
2. tonic and grpcio exchange unary and bidi messages over a mode-0600 Unix socket. Axum and httpx exchange bounded and streaming bodies over a second socket. Oversized messages are rejected, cancellation reaches the tonic handler, and an overlong socket path fails.

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

## Runtime qualification

`runtime_qualification.py` qualifies an installed `kymo-local-runtime` wheel end to end; its docstring lists the phases, and the wheel must be built with the CI-only `test-idle-timeout` feature.

## Browser fences

The options panel checks run locally with Playwright against the `browser-e2e` fixture project from `seed_browser_fixture.py` (see Frontend checks below): it has the numeric, `info/run_info` metadata and `sample` gallery charts and the two active runs the checks use.

```sh
python options_panel_fences.py <dashboard-project-url>
```

Run them after editor, focus, or panel changes; they are excluded from CI to keep its runtime down. Run `user_settings_fences.py <dashboard-origin> --dashboard-path /<project>` as well when changing the shared panel or focus helpers; without `--dashboard-path` it skips its dashboard checks. They pass in Chromium, WebKit and Firefox (Firefox needs `CFFIXED_USER_HOME` set to a writable directory on macOS); in WebKit a label click doesn't focus a checkbox and Tab skips buttons, as in Safari, so those two checks adapt there.

## Target status

| Target | Status | Evidence |
|---|---|---|
| macOS arm64 | Qualified | CI run 31419177340 and a local macOS 26.5.2 run, 2026-08-10; a local macOS 27.0 run, 2026-09-30. |
| Linux x86_64 | Qualified | CI run 31419177340, 2026-08-10. |

Linux x86_64 runs in the internal CI on every relevant push. macOS arm64 runs in the public repository's release workflow on every exported snapshot, and a release publishes only after it passes. A target is not supported by the launcher until its qualification passes with the frozen database artifacts. Do not replace a failing private transport with unauthenticated loopback.

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
python tools/local-transport-qualification/user_settings_fences.py http://127.0.0.1:PORT/ --dashboard-path /browser-e2e
python tools/local-transport-qualification/legacy_storage_fences.py http://127.0.0.1:PORT/
python tools/local-transport-qualification/zoom_fences.py http://127.0.0.1:PORT/zoom-e2e/zoom-fixture
```

The gesture, user settings, legacy storage, zoom, options panel, first-mount, and text scroll fences accept `--browser chromium|firefox|webkit` (default: `chromium`).

WebKit exercises native checkbox activation on auxiliary clicks that Chromium does not produce. The gesture fence suppresses native context menus to keep test input flowing; Kevin must check the actual macOS Ctrl-click menu by hand.
