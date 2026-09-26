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

## wandb importing

We have an importer, but it's not released yet; maybe I'll get around to fixing it for public release. You'll need an AI agent to use it, since wandb's APIs change over time.
