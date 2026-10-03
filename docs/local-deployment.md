# Local deployment for end users

Status: In progress

Linear: AI-1433

## Decision summary

Local mode runs the existing PostgreSQL 17, ClickHouse, and `kymo-server` implementations under one native launcher. It does not add SQLite, Docker Desktop, or a second metadata path. PostgreSQL preserves the production SQL and transaction semantics; ClickHouse remains the metric store.

The launcher downloads pinned database archives on first use, starts the stack on demand, and stops it after an hour without frontend or application activity. The Python worker parks while empty and wakes the stack only for pending delivery. A data-loss response for a rich mutation quarantines the affected spool as `.rejected`, reports the loss, and advances later ordered segments rather than retrying the rejected head forever.

The supported v1 targets are exactly:

- Linux x86-64.
- macOS arm64.

Linux arm64, Intel macOS, native Windows, and peer-reachable multi-user local mode are out of scope.

## Trust boundary

Local mode has the same trust philosophy as hosted mode: filesystem and network position are the boundary. Anyone who can read the installation's files or reach its loopback listeners is trusted to read the local dashboard data. The intended deployments are a personal laptop or an individual VPN server account. Cross-account defense on a shared workstation is not a product requirement.

Consequences:

- Dashboard HTTP and its WebSocket are unauthenticated on a loopback-only port.
- CDN GETs are unauthenticated on a second loopback-only origin, so passive resources can use ordinary `<img src>` and links.
- `kymo open` passes only a stable, non-secret URL to the operating-system browser helper. It adds a timed supervisor hold; there is no bootstrap token, per-tab session, renewal protocol, or authenticated-upgrade handshake.
- An open WebSocket counts as frontend presence and prevents idle shutdown; its close restarts the idle hour. The server pings every dashboard socket and closes a local one that has sent nothing for 60 seconds, so a peer that vanished without closing (a dropped SSH tunnel, a sleeping client) stops counting as present within about a minute.

The server bearer, supervisor and lifecycle credentials, private native/upload/control Unix sockets, per-generation rotation, strict managed-root validation, and pinned ClickHouse TLS remain as defense in depth and operational isolation. They prevent accidental misuse and port conflicts, not access by another process already inside the trusted boundary. Credentials are still redacted from normal logs and diagnostics. Both browser listeners answer only `Host: 127.0.0.1:<port>` or `localhost:<port>`, which cheaply refuses DNS-rebinding requests.

## Architecture

```text
kymo.init(mode="local")              kymo open [project [run]]
              |                                      |
              +----------- kymo ensure/open --------+
                                  |
                          launcher/supervisor
                   install, lock, recover, start,
                      monitor, idle, diagnose
                      /          |           \
             PostgreSQL 17   ClickHouse       kymo-server
             private UDS    pinned HTTPS    private UDS + loopback
                                             /                 \
                                  embedded dashboard       CDN GET origin
                                  + local WebSocket         content files
```

Hosted mode remains the default. Its `/grpc-ws` framing, CDN upload/read behavior, Postgres path, and Kubernetes environment contract do not change.

Local mode uses:

- Private Unix sockets for native gRPC, CDN upload, PostgreSQL, and supervisor control.
- One generated server bearer for native gRPC and upload, a supervisor bearer for the control socket, and a lifecycle credential for the server's `ShutdownLocal` and `GetActivity`.
- A generated ClickHouse password and certificate pinned by `kymo-server`; no plaintext ClickHouse listener.
- Two distinct, high, loopback-only browser ports persisted in `runtime.json` at installation.
- A release frontend bundle embedded in the local server.
- An allowlisted supervisor environment (home, locale, XDG paths, `KYMO_LOCAL_ROOT`), inherited by every child, so the stack behaves the same whichever process woke it.
- No caller descriptors in the stack: the launcher marks every descriptor above stderr close-on-exec before it starts anything, so the stack never holds its waker's pipes open (anything waiting for them to close, such as a parent's `join` or a shell pipeline, would otherwise wait until the stack stopped).

The dashboard and CDN ports are stable across idle stops and restarts. Startup binds the exact persisted ports before spawning children. A conflict fails with a diagnostic naming `kymo ports`; it never silently changes a printable run URL. The supervisor retains duplicate listener descriptors for the life of the stack.

## Distribution

Keep the Python client `kymo` as a universal pure-Python distribution and ship the native launcher and server as the platform wheel `kymo-local-runtime`, which the client's `local` extra pins to exactly the client's version. Release builds must never enable the CI-only `test-idle-timeout` feature.

ClickHouse is downloaded rather than placed in the wheel because its installed binary is roughly 560 MB and PyPI's ordinary wheel limit is 100 MB. The artifact catalog freezes target, URL, version, archive size, and archive digest, plus ClickHouse's installed digest (its macOS asset expands itself on first run). PostgreSQL installs are validated by reported version and, on macOS, code signatures. Archives are streamed through bounded verified downloads and confined extraction. The supported runtime wheel must remain below 100 MB.

Build the local frontend with the same `dx bundle --release --cargo-args=--locked` path used by the hosted image, with the `local-runtime` feature selecting runtime endpoints. The wheel job consumes that release output and passes it to the server's `local-bundle` build feature. The bundle must contain `index.html`; local startup fails closed if it is absent.

## Offline frontend

Dashboard customization remains browser-local.

The local dashboard must make zero external requests. uPlot, Source Sans Pro, Source Code Pro, CSS, JavaScript, WASM, and generated wrapper files are vendored or produced by the release bundle. CI rejects external `src`, `href`, `url()`, and `@import` resources and rejects the hosted service hostname in the local WASM.

At runtime the server injects the local WebSocket path and persisted CDN origin as inert JSON in each embedded SPA shell response. This adds no configuration route or request. Embedded files use hosted nginx's existing exact-asset-then-SPA precedence: a project/run pair whose path exactly equals a generated asset path is the inherited route-collision edge. The local build validates the numeric-loopback CDN origin, derives the WebSocket origin from `window.location`, and never contains a hosted endpoint in its reachable configuration. Hosted builds retain their existing fixed endpoint contract. The shell is `no-store`; other embedded files carry a content-hash ETag and revalidate, so reopening the dashboard does not re-download and recompile the WASM.

CDN content stays on the second origin, matching hosted isolation. Local CDN reads are `private, no-store` and `nosniff`; hosted reads retain `public, immutable`. Local CDN upload has no browser TCP route and remains on the authenticated private Unix socket. Raw resource IDs remain hosted-compatible lowercase `SHA256.ext` values with the append-only extension allowlist.

## Installation, commands, and user flow

The user-facing quickstart is the [kymo README](https://github.com/NovelAI/kymo#readme).

Supported commands for v1:

```text
kymo install
kymo ensure --json
kymo start
kymo stop [--hold]
kymo status [--json]
kymo doctor [--json]
kymo open [--no-browser] [<project-id> [<run-id>]]
kymo ports [--dashboard <port> --cdn <port>]
```

`install` may run implicitly during `ensure`, `start`, or `open`. `KYMO_LOCAL_NO_INSTALL=1` makes those commands fail quickly with the explicit install instruction. A launch may take up to 8 minutes (first `initdb` plus each component's readiness budget), plus up to 2 minutes when it replaces an earlier PostgreSQL build; the Python client's first-use ensure timeout (10 minutes) is independent of the 30-second `InitRun` RPC.

Python usage:

```python
import kymo

kymo.init(mode="local", project_id="demo", run_name="first run")
print(kymo.run_url())  # stable and secret-free; does not wake the stack
kymo.open_run()        # wakes the stack, opens this run, returns its URL
```

`run_url()` uses the install-time dashboard port returned at initialization, so the URL remains valid after idle restarts. `open_run()` invokes `kymo open` with the expected installation UUID, which refuses to target a replacement data directory and adds a timed open hold while the browser starts. `kymo open` prints the URL before trying the browser helper, and a missing helper is only a warning.

`tools/wandb-import/import_to_kymo.py --local` imports W&B history. The supervised server always enables the bulk-import lane: it sits behind the same bearer as every other native call, so it reaches no one who could not already log runs. The importer wakes the stack before each run and reconnects if the endpoint generation changed.

On an individual VPN server, keep local mode bound to loopback and SSH-forward both persisted browser ports at the same numbers so the two-origin layout remains intact; `kymo ports` shows them, or sets memorable ones after a plain `kymo stop`; URLs printed earlier by `run_url()` then change. Open the forwarded URL as `127.0.0.1` or `localhost`. A team-shared service should use hosted mode with its own PostgreSQL and ClickHouse. Docker Compose remains a reference/fallback deployment outside this ticket, not a bind-address option in local mode.

## Runtime identity, ports, and lifecycle

The data directory owns a durable installation UUID, which is the installation's whole identity: a restored or relocated copy of the data keeps it, and another installation's data cannot adopt this manifest. `runtime.json` records:

- Manifest, protocol, launcher, database, and data-schema versions.
- Installation UUID.
- Stable dashboard and CDN ports.
- Current generation UUID, sockets, process identities, artifact paths, and secret references while running.
- The process groups of a launch in progress, and the last failure for diagnosis.

The first manifest picks two distinct free ports below every supported kernel's ephemeral range. Compatible reinstalls preserve them. All mutable identity and manifest publication uses file sync, atomic publication, and parent-directory sync. Existing acknowledged data without its identity fails closed rather than receiving a replacement UUID. Every start and every completed install records the running launcher version, which older environments then refuse; stopping records nothing.

The supervisor holds the runtime lock for its whole life, so acquiring that lock proves no supervisor is alive. Every child runs in its own process group and is published in the manifest immediately after it spawns (`initdb` included); a failed startup keeps that record unless every child is proven gone. Before any start, the lock holder proves each recorded group from a crashed stack or interrupted launch is gone, then clears the record; crash-stale database PID files are ordinary residue at that point. Only a record whose processes are still alive fails closed. A failed start or an unexpected component exit stops the remaining components and records the reason, but never blocks the next start. The supervisor itself never restarts anything; each `ensure` starts at most once, and the Python worker's backoff paces retries. A client that disconnects mid-request or a launcher that gives up before readiness only loses its own reply.

PostgreSQL stops through validated `pg_ctl`, ClickHouse through its native shutdown, and the server through its private lifecycle RPC, falling back to SIGTERM (the same bounded drain) when it does not exit. No local path uses `SIGKILL`. A supervisor that cannot reach its own server for a minute stops the stack, so a deleted runtime directory never pins it.

`kymo stop` first closes a start gate that it keeps until it exits, then returns only once it holds the runtime lock, which also waits out a launch in progress; `kymo stop --hold` keeps both until interrupted, so nothing can restart the stack while its data is copied or restored. A live supervisor it cannot reach (its socket directory removed, say) gets SIGTERM, on which it stops the stack itself. Components that outlived a crashed supervisor get their supervised-stop signal, then SIGINT, when their recorded PID and start time still match.

A hard kill between a spawn and its publication can leave one unrecorded child; PostgreSQL's pidfile and ClickHouse's status-file lock then refuse a second instance on the same data, and a leftover server keeps the pinned ports, so the next start fails rather than sharing data. A launch that outlives its whole budget is reported as stuck, not starting.

The supervisor polls an authenticated relative activity snapshot that excludes the poll itself. Server-ready, committed ingest, lifecycle mutations, serving the dashboard shell, and a frontend WebSocket closing reset the idle clock. The stack therefore outlives its last viewer by the full hour, and a page is covered from its shell until its WebSocket connects. Per-batch work, bounded requests, and CDN downloads block shutdown only while in flight, and frontend WebSockets while connected. Empty bidi streams do not count. Individually clearable init holds, timed open holds, and bare-ensure/post-wake holds contribute one effective hold deadline.

An occasional request racing the one-way self-fence may observe disconnect and retry. The Python spool and demand-driven `ensure` path preserve unacknowledged data without a cancellable two-phase shutdown protocol.

## Data safety and replay

PostgreSQL stores run metadata and monotonic rich-mutation heads. ClickHouse stores rich rows under `ReplacingMergeTree(mutation_version)`. A writer epoch plus mutation sequence is assigned before enqueue and remains immutable through retry, spill, and replay. Higher versions win; equal identical writes are idempotent; equal different writes are data loss; lower versions are explicitly superseded. Replay rebuilds manifests and re-serializes them, so idempotency also depends on byte-stable manifest serialization across client versions: the two manifest shapes are serialized in one place (`_cdn.py`) and a test freezes their exact bytes and content IDs.

`DATA_LOSS` while replaying a rich mutation is not retryable. The client:

1. Quarantines the affected spool as `.rejected`, matching installation-mismatch quarantine naming.
2. Logs the exact reason and file, and reports a distinct DATA_LOSS delivery failure to the owning process.
3. Removes that whole segment from automatic replay and advances to later segments for the run. Records after the rejected mutation in the same segment remain quarantined; later segment files remain eligible.
4. Uses a distinct mutation sequence reserved before enqueue if dropping an invalid gallery child requires publishing a reduced manifest; replay never reuses the rejected publication's version or allocates a new writer epoch.

Spool creation, retirement, and quarantine sync the affected directory entries, so on filesystems that support directory sync a power loss can neither drop a spooled segment nor resurrect a delivered one.

The registry is reconciled from ClickHouse on local boot through the existing type-upgrading, canonical-run-only registration path. Physical deletion of expired runs is always enabled locally; its first pass runs a minute after boot, then hourly, because an idle-stopped stack may not live long enough for hosted's one-interval startup delay.

## Data locations, deletion, and backup

There are no `uninstall`, `reset`, `backup`, or `restore` commands in this ticket.

The launcher creates these roots:

- Persistent data: `ProjectDirs::data_dir()/data` (PostgreSQL, ClickHouse, CDN objects, `installation.uuid`).
- Mutable state: `ProjectDirs::state_dir()` when available, otherwise `ProjectDirs::data_local_dir()/state` (manifest, artifacts, generations, locks, component logs).
- Reconstructible cache: `ProjectDirs::cache_dir()` (downloads).
- Python spool writes: `$KYMO_SPOOL_DIR`, otherwise `~/.cache/kymo/spool`; no-argument `kymo-sync` also scans the pre-cutover `~/.cache/pymkdb2/spool` backlog.
- Ephemeral sockets: on macOS, `_CS_DARWIN_USER_TEMP_DIR/kymo-<uid>/<state-digest>`; on Linux, `$XDG_RUNTIME_DIR/kymo-<uid>/<state-digest>`, falling back to canonical `/tmp/kymo-<uid>/<state-digest>`. The digest (12 hex digits of the state root path's SHA-256) keeps a copied installation, which shares the UUID, from touching the original's sockets.

Supervisor, PostgreSQL, ClickHouse, and server logs live in the mutable state root. Each component keeps one current file plus three rotations capped at 8 MiB each, bounding the four log families to 128 MiB total.

The state root must be on a local filesystem: the lifecycle lock passes from launcher to supervisor as an inherited `flock`, which NFS does not preserve, so lock files on NFS are refused. With `KYMO_LOCAL_ROOT=/absolute/path`, the first three become `<root>/data`, `<root>/state`, and `<root>/cache`. Documentation must show the resolved paths reported by the launcher before suggesting deletion.

Uninstall deletes the data, state, and cache roots after `kymo stop`. The hold cannot protect this (its locks live in the state root), so first stop processes that log in local mode, or set `KYMO_LOCAL_NO_INSTALL=1` for them; otherwise one with queued data reinstalls a fresh stack. Users delete spools separately if they also want to discard undelivered data.

Backup and restore run while `kymo stop --hold` holds the stack stopped:

- A cold backup copies the data root plus `state/runtime.json`, preserving ClickHouse's confined `Atomic` links and file modes. The manifest travels with the data because it records which database versions wrote it.
- A restore puts both back. The launcher refuses a restore whose manifest records neither its catalog's database builds nor another build of the catalog's PostgreSQL major release.

No live-copy or portable-bundle guarantee is made in v1.

The accepted installed footprint is roughly 700 MB, dominated by the irreducible ClickHouse binary. No startup-latency or time-to-first-chart target is imposed for v1.

## Validation and release gates

Required automated or release-qualification coverage:

1. Fresh install with neither database present; exact versions/digests; socket-only PostgreSQL; pinned-HTTPS ClickHouse; default user disabled.
2. Linux x86-64 wheel qualification on every relevant change; macOS arm64 qualification on every exported snapshot, which gates release. Unsupported target builds fail closed.
3. Concurrent `ensure` calls converge on one generation. Hard-kill recovery (of the supervisor, a component, or a launch in progress) and idle shutdown leave no process group or socket behind, and the next `ensure` starts a new generation.
4. The persisted dashboard/CDN ports survive stop/start and compatible reinstall. A port conflict produces an actionable error without mutating the pinned URL; `kymo ports` replaces ports only while stopped.
5. `run_url()` remains unchanged across restart and does not wake a stopped stack. `open_run()` wakes it, checks the installation UUID, returns the URL, and its timed hold prevents shutdown during browser launch.
6. An open frontend WebSocket prevents idle shutdown; once it closes and runs are inactive, the complete stack stops a full idle timeout later. Empty streams and quiet workers neither hold nor wake it. `kymo stop` waits out a launch; `kymo stop --hold` keeps a waking client out.
7. Local dashboard/CDN/server listeners are loopback-only and refuse other Host names. Browser CDN has GET/HEAD/OPTIONS but no upload or WebSocket route; upload remains authenticated on its Unix socket.
8. Local release bundle contains all fonts/scripts/styles/WASM, makes zero external requests with network disabled, and contains no hosted service endpoint. Hosted `/grpc-ws` framing and unauthenticated hosted upload remain unchanged.
9. Numeric, tagged, text, image, resource, metadata, Trash restore, purge, dashboard, spool replay, mutation ordering, and DATA_LOSS quarantine work locally.
10. Legacy hosted clients that omit the additive writer/mutation fields retain legacy behavior. Checked-in Python protobuf stubs match the source.
11. A newer compatible launcher/server/frontend cannot be downgraded by an older environment once it has started the stack (the version is recorded at start, so a running older generation keeps serving until its next start). Another build of the catalog's PostgreSQL major release is replaced at the next start that can install, with data, UUID, and ports intact; any other PostgreSQL, ClickHouse, or data-schema-generation change is refused before database startup.
12. The supported Apple Silicon path preserves valid executable signatures and requires no undocumented security override.

## Tradeoffs

### PostgreSQL child versus SQLite

The child adds memory, startup latency, binary distribution, and eventual major-version work. In return it avoids a second metadata implementation and preserves roughly 105 production query sites and their transaction/lifecycle semantics. Once ClickHouse already needs supervision, PostgreSQL is the smaller engineering path.

### Stable ports versus ephemeral ports

Ephemeral ports avoid conflicts automatically but make printed run URLs stale after every idle restart and force an endpoint-resolution layer into the browser API. Two persisted high ports make `run_url()` durable and SSH forwarding understandable. The cost is a rare explicit `kymo ports` remediation instead of silent rebinding.

### Replacing database builds at the next start

An installation may record another build of the catalog's PostgreSQL major release, which shares its data directory format; it is replaced the way upstream applies minor releases: stop, swap binaries, start. A start that finds nothing running (or `kymo install`) installs the catalog build beside the old one, checks that it opens the existing cluster (`postgres -C` reads the control file and configuration without starting a server), and only then atomically republishes the manifest, keeping the installation UUID and ports; the launch that follows runs it. The manifest publish is the only commit point, so no journal is needed. Until then a running stack keeps serving and newer launchers attach to it. A start that cannot replace the build within two minutes (offline, a slow or failed download, a build that cannot open the cluster) runs the recorded build, and the next start retries; `KYMO_LOCAL_NO_INSTALL=1` skips the attempt. Artifact directories are keyed by version, so a rebuild of an unchanged release takes a new build number. The replaced build and its cached archive stay on disk (about 50 MB). There is no automatic rollback; the cold backup is the way back.

ClickHouse pin moves (its new binary migrates data on first start) and PostgreSQL major releases (`pg_upgrade` into a new data directory) are still refused; each needs its own qualification first.

Also settled: a native launcher instead of Docker Compose (no Docker Desktop prerequisite on personal machines); unauthenticated browser access instead of per-tab sessions (sessions would defend a shared-workstation boundary the product does not claim); an embedded frontend (no server/frontend skew offline); activity-derived idle instead of client leases (existing signals suffice until measurements say otherwise; `system_metrics=False` may cost a cold restart after a long silent phase).

## References

- PostgreSQL 17 shutdown: <https://www.postgresql.org/docs/17/server-shutdown.html>
- Portable PostgreSQL binaries: <https://github.com/theseus-rs/postgresql-binaries/releases>
- ClickHouse supported platforms: <https://clickhouse.com/support/platforms>
- ClickHouse HTTP authentication: <https://clickhouse.com/docs/interfaces/http>
- PyPI storage limits: <https://docs.pypi.org/project-management/storage-limits/>

## Changes

- 2026-09-23: A recorded failure no longer circuit-breaks the stack into a `Degraded` state that only `kymo start --retry` cleared; it is diagnostic, and the next start retries once the recorded processes are proven gone (a transient crash otherwise left local mode silently offline). The mkdb2 ProjectDirs migration was dropped: nothing had been installed externally, so pre-release installations reinstall.
- 2026-09-28: The launcher stopped handing its caller's inheritable descriptors to the stack. A woken stack used to hold its waker's pipes open: a parent's `join` timed out, and a shell pipeline stayed open until the stack stopped.
- 2026-09-28: The supervised server enables the bulk-import lane, and the W&B importer's `--local` imports into local mode.
- 2026-09-30: A viewer restarts the idle hour: closing a dashboard WebSocket or serving the dashboard shell resets the idle clock, replacing a 10-second reconnect grace and a 60-second page-load grace. A dashboard closed long after the last logging used to let the stack stop about 10 seconds later, so a viewer whose SSH tunnel dropped came back to a stopped stack.
- 2026-10-02: Another build of the catalog's PostgreSQL major release is replaced at the next start instead of refusing every command, which would have stranded every installation at the first catalog change. Stopping no longer fences older launchers out.
