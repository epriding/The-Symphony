# Poly Bot v1

A Python paper-trading framework for Polymarket US and Kalshi, with supervised
snapshot strategies, shared risk/accounting, public-data observation, local history
archival and deterministic replay. Actual trading algorithms and real-money bot
execution remain future work.

Set up Python 3.12 (required by `polymarket-apis`) and a virtual environment
from the repository root:

```bash
python3.12 -m venv venv
venv/bin/python -m pip install -r requirements.txt
```

Run the offline checks:

```bash
venv/bin/python main.py smoke --output /tmp/bot-smoke
venv/bin/python main.py replay --capture /tmp/bot-smoke --output /tmp/bot-replay
venv/bin/python -m pytest -q
```

[GitHub Actions CI](.github/workflows/ci.yml) runs on every pull request, pushes
to `main`, and manual dispatch. It installs and validates dependencies, compiles
Python sources, runs the full test suite, and checks synthetic smoke/replay without
venue credentials. JUnit test results are retained as an artifact for seven days,
including when tests fail. The workflow uses read-only repository permissions.

To block merges on failures, configure a GitHub branch ruleset for `main` requiring
the **Python 3.12 checks** status check after the workflow's first run. The workflow
file alone does not enforce branch protection. No deployment target, package build,
lint configuration, or static type checker is currently defined in this repository;
those gates can be added when their tooling is established.

Every output directory must be new. Smoke uses synthetic data for both venues and
requires no credentials. For public observation, copy
[the example configuration](examples/bot_config.example.json), replace the market
placeholders, verify its assumptions and configure venue credentials:

```bash
venv/bin/python main.py run --config local-bot.json --output /tmp/bot-observation --observe-only --duration 60
```

Read [the bot runtime guide](docs/BOT_RUNTIME.md) for configuration, factory
registration, limits, shutdown, archives and validation requirements. `run` can
load a local `.env` file; keep credentials local. Smoke/replay do not load it.
Authenticated feed validation and empirical execution calibration remain separate
from synthetic tests.

The [local control service](docs/BOT_CONTROL.md) provides an initially idle,
private Unix-socket interface with shared strategy/trade observations, diagnostic
findings and explicit paper-session controls. It runs independently of attached
clients. Attach the [Textual terminal interface](docs/BOT_TUI.md) with:

```bash
venv/bin/python main.py tui --socket "$HOME/.local/state/symphony/control.sock"
```

The separate structured agent CLI remains a subsequent module.

The [code guide](docs/CODE_GUIDE.md), [paper execution guide](docs/PAPER_TRADING.md),
[risk manager guide](docs/RISK_MANAGER.md) and [documentation index](docs/_index.md)
cover the component APIs. Existing session/demo commands remain supported:

```bash
venv/bin/python -m examples.paper_trading_demo --output /tmp/paper-demo
venv/bin/python -m src.paper_trading.session replay --capture /tmp/paper-demo --output /tmp/paper-replay
```
