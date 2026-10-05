# Self-hosting kymo

Hosted mode runs one `kymo-server` against your own PostgreSQL and ClickHouse, with the dashboard served as static files. For one person on one machine, local mode sets all of this up automatically; see the [kymo README](https://github.com/NovelAI/kymo#readme) and [local deployment](local-deployment.md).

## Trust model

Nothing in hosted mode authenticates. Anyone who can reach the gRPC port can read, write, rename, and trash runs; anyone who can reach the HTTP port can read every logged image and file, upload blobs, and rename, trash, or restore runs through the dashboard WebSocket. Clients speak plaintext gRPC and HTTP. Run kymo on a private network or VPN, and treat network access as access to all of its data.

## Components

| Component | Default listener | Serves |
|---|---|---|
| `kymo-server` gRPC | `0.0.0.0:50051` | Training clients |
| `kymo-server` HTTP | `0.0.0.0:8080` | Dashboard WebSocket (`/grpc-ws`), media (`/cdn/…`, read and upload), firing alerts (`/alerts`) |
| `kymo-server` metrics | `0.0.0.0:9090` | Prometheus `/metrics` |
| Dashboard | `0.0.0.0:80` (dashboard image) | Browser UI; talks only to the HTTP port |
| PostgreSQL | | Projects, runs, and registry metadata |
| ClickHouse | | Metric, text, and media-reference rows |

## Databases

Local mode runs PostgreSQL 17 and ClickHouse 25.3 LTS, the tested versions. ClickHouse must be 25.3 or later: the media collector turns off a setting older versions reject. The server creates and upgrades its schema at startup and opens its listeners only after that finishes:

- **PostgreSQL**: tables, indexes, and constraints in the `DATABASE_URL` database, so its role must be able to create and alter them.
- **ClickHouse**: the database `mkdb2` (a fixed name), with tables and materialized views. The user needs full rights on `mkdb2.*` (create, alter, rename, drop, insert, select, truncate; the rename and drop are for one-time schema migrations), `SYSTEM FLUSH ASYNC INSERT QUEUE`, read access to `system` tables, and permission to set the `async_insert` settings, so no read-only or constrained profile. Hosted mode reaches ClickHouse over plain HTTP.

## Server settings

Settings are environment variables; an unset one takes its default.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgres://mkdb2@localhost:5432/mkdb2` | PostgreSQL URL |
| `CLICKHOUSE_URL` | `http://localhost:8123` | ClickHouse HTTP URL |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | `default`, empty | ClickHouse credentials |
| `LISTEN_ADDR`, `CDN_LISTEN_ADDR`, `METRICS_LISTEN_ADDR` | see Components | Listener addresses (`ip:port`) |
| `KYMO_ALLOWED_ORIGINS` | `http://localhost:*,http://127.0.0.1:*` | Comma-separated browser origins allowed to use the WebSocket and `/alerts`. Must include the dashboard's origin, exactly (for example `http://kymo.example`). A `:*` port wildcard works only for loopback hosts. |
| `KYMO_CDN_BACKEND` | `filesystem` | Media store: `filesystem` or `gcs` |
| `KYMO_CDN_ROOT` | `/data/cdn` | Media directory for the filesystem store |
| `KYMO_CDN_GCS_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS` | none | Bucket and Google credential file (a `service_account` key, or an `external_account` federation config without impersonation), both required for `gcs` |
| `KYMO_CDN_GC` | `delete` for `filesystem`, `report` for `gcs` | Media garbage collector: `off`, `report` (counts only), or `delete` (see Storage and backups) |
| `KYMO_CDN_GC_CREDENTIALS` | none | With `gcs`, the credential file for `delete`: an `external_account` config that impersonates a service account allowed to delete in the bucket |
| `KYMO_CDN_GC_MAX_CANDIDATES` | 10,000 plus 10% of referenced objects | With `delete`, a pass with more candidates deletes nothing; raise it for a deliberate large purge, then unset it |
| `KYMO_RUN_REAPER_ENABLED` | `false` | Permanently delete runs whose Trash retention expired (see below) |
| `KYMO_PROMETHEUS_URL` | unset | Prometheus whose firing alerts the dashboard shows; unset, it shows the media collector's own conditions |
| `KYMO_IMPORT_ENABLED` | `false` | Accept wandb imports (`tools/wandb-import/import_to_kymo.py`). Enable it only while importing: an import can backdate and terminate any run it names. |
| `RUST_LOG` | `info` | Log filter |

Capacity knobs: `KYMO_INGEST_BYTE_CAP` (bytes of buffered ingest, default 512 MiB), `KYMO_FLUSH_CONCURRENCY` (concurrent ClickHouse inserts, default 4), `KYMO_SERIES_CACHE_MB` (chart cache, default 256), `KYMO_TEXT_INDEX_CACHE_MB` (log index cache, default 64), and `KYMO_CHART_INFLIGHT_SERIES` (concurrent chart reads, default 32).

## Storage and backups

The filesystem store keeps each logged image or file once, named by its content hash, under `KYMO_CDN_ROOT`. By default a garbage collector deletes media that nothing references, which becomes eligible 30 to 31 days after its last upload, even as a duplicate:
- It deletes nothing until 31 days after the first start of a server with the collector.
- Deletion is final; there's no trash. Back up the media directory to be able to undo it.
- A pass deletes nothing if it finds more candidates than its ceiling (`KYMO_CDN_GC_MAX_CANDIDATES`), a gallery or file list it can't read, or a symlink in the media directory or its two-character shard directories (what it leads to is outside the collector's view).
- The dashboard's notice bar reports each of these, referenced media found missing, and failed passes. With `KYMO_PROMETHEUS_URL` set, alert on the `mkdb2_cdn_gc_*` gauges in Prometheus instead.
- `KYMO_CDN_GC=report` only counts; `off` stops it. Run `report` while rebuilding `metrics` or `rich_metrics` by hand.
- A duplicate upload refreshes the stored file's timestamps, or rewrites the file, so the server's user must be able to write the media directory.
- Keep the media directory on a local POSIX filesystem (ext4, XFS, btrfs, ZFS, APFS). The collector dates files by ctime, which FAT, exFAT, SMB and some FUSE mounts let a copy or restore set back.
- Use one media directory per pair of databases: a deployment sharing another's would see only its own references and delete the other's media.

With the `gcs` store, a garbage collector lists the bucket every few hours and publishes how much media nothing references. By default it only reports. With `KYMO_CDN_GC=delete` it also deletes objects that are unreferenced and weren't uploaded (even as a duplicate) in the last 30 days, and deletes nothing until its upload log covers those 30 days. A `gcs` server records each upload of already-stored media in ClickHouse before acknowledging it, so while ClickHouse is down those uploads fail and wait in the client's spool.

Back up PostgreSQL and ClickHouse together, with the server stopped, and with the filesystem store the media directory too. With `gcs`, the bucket holds the media, and its soft delete is the only undo of the collector's deletes. A database backup can reference media the collector deleted after it was taken, so keep media backups (with `gcs` and `delete`, the bucket's soft-delete retention) at least as long as the age of any database backup you might restore.

With `gcs`, restoring ClickHouse from a backup or running a server older than the collector loses upload-log entries, which a later `delete` would then trust. Afterwards, once a server with the collector is running again, run `INSERT INTO mkdb2.cdn_acks VALUES ('', now())`, so deletion waits a fresh 30 days. With the filesystem store, do the same after running a server older than its collector against the media directory, since that server doesn't record duplicate uploads, and after rolling the media directory back to a snapshot (ZFS, btrfs, LVM, a VM disk), which restores old file timestamps. With either store, do the same after correcting a server clock that ran more than 30 days slow, since everything it dated looks that much older.

## Run exactly one server

Run one `kymo-server` per pair of databases, never two, not even briefly during an upgrade. The server keeps deletion fences and a copy of the metric registry in memory, and truncates its ClickHouse registry outbox at startup, all of which assume it is the only writer. For the same reason, restart the server after editing the `run_metrics` table by hand. Upgrade by stopping the old server before starting the new one. There is no health endpoint; the listeners open once startup work is done, so a TCP check on port 50051 or 8080 serves as readiness.

## Trash

Deleting a run moves it to Trash, where it can be restored for 7 days. After that it is unreadable, but its rows stay on disk until physical deletion is enabled with `KYMO_RUN_REAPER_ENABLED=true`. The reaper then runs hourly, first one hour after startup, and skips a pass unless ClickHouse has free disk of twice the project's largest part plus 10 GiB. Once it has deleted anything, do not downgrade to an older server.

## Building and running

Build both images from the same commit, in the `kymo/` directory. The dashboard compiles in the server's HTTP origin as the browser should reach it, and its build fails without one:

```sh
docker build -f kymo-server/Dockerfile -t kymo-server .
docker build -f kymo-frontend/Dockerfile \
  --build-arg KYMO_FRONTEND_SERVER_ORIGIN=http://kymo.example:8080 \
  -t kymo-dashboard .

docker run -d --name kymo-server \
  -e DATABASE_URL=postgres://kymo:secret@db.example/kymo \
  -e CLICKHOUSE_URL=http://clickhouse.example:8123 \
  -e CLICKHOUSE_USER=kymo -e CLICKHOUSE_PASSWORD=secret \
  -e KYMO_ALLOWED_ORIGINS=http://kymo.example \
  -v /srv/kymo/media:/data/cdn \
  -p 50051:50051 -p 8080:8080 \
  kymo-server
docker run -d --name kymo-dashboard -p 80:80 kymo-dashboard
```

Serve the dashboard at the root of its host. Another web server works if, like the image's nginx, it answers unknown paths with `index.html` and makes `index.html` revalidate; only the content-hashed `/assets/*-dxh<hex>.*` files may be cached for good. A dashboard served over HTTPS needs an `https://` server origin, for example through a TLS proxy in front of port 8080. The server pings dashboard WebSockets every 20 seconds, so a proxy idle timeout longer than that (nginx's default is 60 seconds) keeps them open.

Upgrade the server and the dashboard together. A tab left open on a dashboard the server no longer supports keeps what it shows but stops updating; reload it.

## Clients

Point training jobs at the server:

```python
import kymo

kymo.init(
    server_address="kymo.example:50051",
    url_base="http://kymo.example",  # dashboard origin, for kymo.run_url()
    project_id="my-project",
    run_name="baseline",
)
```

`KYMO_SERVER` and `KYMO_URL_BASE` supply the same values from the environment. Media uploads go to port 8080 on the server's host; pass `cdn_address="http://host:port"` if the HTTP listener lives elsewhere. Points a job could not deliver are spooled locally; replay them with `python -m kymo.sync`, which reads the server from each spool.
