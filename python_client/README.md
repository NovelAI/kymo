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

## Configs and metadata

`kymo.init(config=...)`, `kymo.update_config(...)` and `kymo.log({name: kymo.Metadata(...)})` store a dict as JSON, frozen when the call is made. `kymo.Api.run_info` and Python's `json` read it back. A value that cannot be stored raises ValueError: `init` raises before it stops a previous run, `update_config` leaves the config as it was, and `log` logs nothing from that call.

- JSON values are stored as they are, `NaN`, `Infinity` and `-Infinity` included, in Python json's dialect.
- A numpy bool, integer or floating scalar is stored as its exact bool, int or float. A longdouble that a float can't hold exactly raises.
- A numpy array or a torch tensor is stored as `.tolist()` gives it, nested lists or a 0-d array's scalar, each element by these rules; a datetime64 or timedelta64 array as the text of each element, as its scalars are stored.
- A set is stored as a list, sorted by each element's JSON text.
- Anything else is stored as `str(value)`: a Path, datetime, Enum, Decimal, bytes, dataclass, numpy datetime64, timedelta64 or complex scalar, or a mapping that isn't a dict. Pass `dataclasses.asdict(x)` or `OmegaConf.to_container(cfg, resolve=True)` to store their fields.
- Keys are JSON text. An int, float, bool or None key is stored as the text json writes for it: `1` as `"1"`, inf as `"Infinity"`, NaN as `"NaN"`. A numpy bool, integer or float key is converted first. Other key types raise.
- When keys of one dict become the same text, such as `1` and `"1"`, inf and `"Infinity"`, or two NaN keys, the call raises naming that text rather than keep only one.
- `{}` stores an empty config, and `update_config` merges top-level keys by their JSON text: `1` replaces `"1"`, but `1` and `1.0` are two keys.

Limits:

- Tuples read back as lists, and IntEnum and StrEnum as plain values. NaN keeps neither its sign nor its payload.
- Python's dict merges `0.0` and `-0.0`, and `1` and `True`, before kymo sees them: the first key's text survives with the last value.
- A repr that holds an object's address (`<object at 0x…>`) differs per process. A dataclass's repr leaves out its `repr=False` fields.
- A masked array's masked entries are stored as null, and a structured array without its field names (its datetime64 and timedelta64 fields as `.tolist()` gives them). A tensor that `.tolist()` can't read (meta, sparse, quantized, nested, DTensor) raises. A numpy key in a dict held inside an object array raises.
- A lone surrogate, as `os.fsdecode` makes for a path that isn't UTF-8, is stored as its exact `\udXXX` escape. A high surrogate directly followed by a low one reads back as the one character they encode.
- A manifest holding NaN or Infinity is not strict JSON. Read manifests with Python's `json`: jq turns those tokens into `null` and `±1.797e308`, and a lone low surrogate into U+FFFD, without a warning.
- An array or tensor costs about 10–35 bytes per element in the manifest (a float about 28), and every `update_config` uploads the whole config again. A manifest past 256 MiB (about 9M floats) is never delivered, and it holds every later gallery and metadata of the run on disk.
- The dashboard shows a string JSON-quoted when its text reads as JSON (`"5"`, `"true"`, `"NaN"`), is empty, or has leading or trailing whitespace, a double space or a control character; copying gives the raw text. It shows, copies and diffs a lone surrogate as the literal text `\udXXX`, so sibling keys that differ only there merge, the later winning. A Metadata dict nested 127 levels deep or more, or a config nested 126, shows as unreadable.

Resuming a run (`kymo.init` with an existing `run_id`) without `config` keeps the run's stored config and metadata, if any, and publishes none of its own, and `update_config` then raises RuntimeError, since this process has no copy of them to merge into. Pass `config`, `{}` included, to replace them. The first resume of a run created before writer epochs replaces them either way.

## Reading runs back

`kymo.Api` reads what runs logged. It takes the same `server_address`, `mode` and `cdn_address` as `kymo.init` and falls back to the same environment variables (`cdn_address` has none).

```python
import kymo

api = kymo.Api("kymo.example:50051")  # or kymo.Api(mode="local")
run = api.runs("my-project")[0]  # newest first
loss = api.history("my-project", run.run_id, "train/loss")[""]  # [(step, value), ...]
config = (api.run_info("my-project", run.run_id) or {}).get("config")
errors = api.logs("my-project", run.run_id, search="Traceback", limit=100).lines
```

`history` returns every point it can read, with NaN and inf as logged. A metric logged as a list has one series per index tag (`"0"`, `"1"`, ...), and an untagged metric has the tag `""`. Its docstring lists the server's limits. `media` lists a media metric's stored keys and `fetch` downloads one.

An `Api` can be shared between threads (close it only after their calls return), but works only in the process that created it: create one in each worker process. Importing `kymo` tees stdout and stderr into a bounded capture buffer, even in a script that only reads.

Under the `fork` start method (Linux's default before Python 3.14), start worker processes and call every `kymo.init()` before the first read: gRPC is unsafe in a process forked after it started, so `init()`, which forks its upload worker, refuses once an Api has opened its gRPC channel. Selecting `spawn` or `forkserver` with `multiprocessing.set_start_method()` before the first `kymo.init()` lifts this.

## wandb importing

Two scripts in [tools/wandb-import](https://github.com/NovelAI/kymo/tree/main/tools/wandb-import) move wandb history into kymo: `export_wandb.py` downloads a wandb entity into a local archive, and `import_to_kymo.py` replays that archive into a kymo server with each run's original times. They are not part of the package: run them from a clone after `pip install ./python_client pyarrow wandb wandb-workspaces requests`, since the importer uses the client's internals. The importer dry-runs by default. To write, pass `--execute --server host:port`, with `KYMO_IMPORT_ENABLED=1` on that server (see [self-hosting](https://github.com/NovelAI/kymo/blob/main/docs/self-hosting.md)), or `--execute --local`, which also needs the runtime of the same release (`pip install kymo-local-runtime==X` for a clone at tag vX). You'll likely need an AI agent to use them, since wandb's APIs change over time.
