Install with `pip install "kymo[local]"`, or `pip install kymo` if there will be a separate server. For an intro, see the [kymo README](https://github.com/NovelAI/kymo#readme).

## Commands

```text
kymo open [project [run]]   start the stack if needed and open the dashboard, a project, or a run
kymo status                 show whether the stack is running
kymo stop                   stop the stack
kymo doctor                 check the installation and show where its files live
kymo ports                  show the dashboard ports, or change them while stopped
```

## On a remote machine

Local mode listens only on the machine itself. To view a stack running on a server you reach over SSH, forward both of its ports at the same numbers:

```sh
kymo ports    # e.g. dashboard: http://127.0.0.1:24819 and CDN: http://127.0.0.1:24820
ssh -L 24819:127.0.0.1:24819 -L 24820:127.0.0.1:24820 you@server
```

Then open the URL that `kymo open --no-browser` prints on the server in your local browser.

## If the backend is not available

Metrics that cannot be delivered are written to a spool (`~/.cache/kymo/spool`, or `$KYMO_SPOOL_DIR`) instead of being dropped. A running script delivers them once the backend is back. Run `kymo-sync` to deliver anything left behind by a script that has already exited.

## Remove or back up local data

Stop any scripts that are still logging, run `kymo stop`, and use `kymo doctor --json` to find the data, state, and cache directories. Deleting those three directories uninstalls local mode. The spool directory is separate; delete it only if you also want to discard undelivered data.

For a backup, keep the stack stopped with `kymo stop --hold` while you copy the data directory and `state/runtime.json`. Put both back the same way to restore.

## Logging to a server

Leave out `mode="local"` and pass the server's gRPC address as `server_address="host:port"`, or set `KYMO_SERVER`. Pass the dashboard's origin as `url_base` (or set `KYMO_URL_BASE`) for `kymo.run_url()`. To run the server yourself, see [self-hosting](https://github.com/NovelAI/kymo/blob/main/docs/self-hosting.md).

## Reading runs back

`kymo.Api` reads what runs logged. It takes the same `server_address`, `mode` and `cdn_address` as `kymo.init` and falls back to the same environment variables.

```python
import kymo

api = kymo.Api("kymo.example:50051")  # or kymo.Api(mode="local")
run = api.runs("my-project")[0]  # newest first
loss = api.history("my-project", run.run_id, "train/loss")[""]  # [(step, value), ...]
config = (api.run_info("my-project", run.run_id) or {}).get("config")
errors = api.logs("my-project", run.run_id, search="Traceback", limit=100).lines
```

`history` returns every point it can read, with NaN and inf as logged. A metric logged as a list has one series per index tag (`"0"`, `"1"`, ...), and an untagged metric has the tag `""`. Its docstring lists the server's limits. `media` lists a media metric's stored keys and `fetch` downloads one.

An `Api` works only in the process that created it: create one in each worker process.

If one script both reads and logs, every `kymo.init()` must come before the first read: `init()` forks its upload worker, which is unsafe once gRPC runs in the process, so after a read it refuses under the `fork` start method (Linux's default before Python 3.14). Calling `multiprocessing.set_start_method("spawn")` before the first `kymo.init()` also lifts it.

## wandb importing

Two scripts in [tools/wandb-import](https://github.com/NovelAI/kymo/tree/main/tools/wandb-import) move wandb history into kymo: `export_wandb.py` downloads a wandb entity into a local archive, and `import_to_kymo.py` replays that archive into a kymo server with each run's original times. They are not part of the package: run them from a clone after `pip install ./python_client pyarrow wandb wandb-workspaces requests`, since the importer uses the client's internals. The importer dry-runs by default. To write, pass `--execute --server host:port`, with `KYMO_IMPORT_ENABLED=1` on that server (see [self-hosting](https://github.com/NovelAI/kymo/blob/main/docs/self-hosting.md)), or `--execute --local`, which also needs the runtime of the same release (`pip install kymo-local-runtime==X` for a clone at tag vX). You'll likely need an AI agent to use them, since wandb's APIs change over time.
