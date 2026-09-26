`kymo` is an experiment tracker for machine learning.

- ✅ Correct graphs, unlike the other experiment trackers
- ❌ All webpage viewers have admin access, so you can't show untrusted friends your graphs
- ❌ Vibe-coded. I can't read Rust and don't know Postgres or ClickHouse or webdev. That means I can't read your PRs
- ❌ It's internal tooling. We don't dogfood the local mode

Two options:

- **Local mode** runs the backend on your machine, started on demand and stopped when idle. Works on Linux x86-64 and Apple Silicon Mac, too bad for Windows users
- **Hosted mode** logs to your server; see [self-hosting](docs/self-hosting.md)

We don't host a server for you since I'm too lazy to make money.

## Install

```sh
pip install "kymo[local]"
```

Hosted mode needs only `pip install kymo`. `[local]` adds the backend `kymo-local-runtime`. Python 3.10 or newer. The first time local mode starts, it downloads PostgreSQL and ClickHouse, about 700 MB.

## Usage

```python
import numpy as np
import kymo

if __name__ == "__main__":
    kymo.init(mode="local", project_id="demo", run_name="first run", config={"lr": 3e-4})  # Karpathy's favored LR
    for step in range(1000):
        kymo.log({"loss": 1.0 / (step + 1)}, step=step)
    kymo.log({"sample": kymo.Image((127.5 * (1 + np.cos(np.arctan2(*np.mgrid[-1:1:512j, -1:1:512j])[..., None] - np.arange(3) * 2 * np.pi / 3))).astype(np.uint8))}, step=999)
    kymo.open_run()  # opens the dashboard in your browser
    kymo.finish()
```

Keep the `if __name__ == "__main__":` guard. kymo uploads from a helper process, and on macOS that process re-imports your script.

The local stack starts when logged to and stops itself after an hour with no logging and no open dashboard. `kymo open` starts it again.

The [client README](python_client/README.md) covers the `kymo` commands, remote machines, undelivered data, backups, and logging to a server.

This repository is a mirror of our internal repository. See [CONTRIBUTING](CONTRIBUTING.md).

"kymo" comes from [kymograph](https://en.wikipedia.org/wiki/Kymograph).

## License

Apache-2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
