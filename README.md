# Poly Bot v1

A Python paper-trading framework for Polymarket US and Kalshi, with supervised
snapshot strategies, shared risk/accounting, public-data observation, local history
archival and deterministic replay. Actual trading algorithms and real-money bot
execution remain future work.

Use the existing virtual environment from the repository root:

```bash
venv/bin/python main.py smoke --output /tmp/bot-smoke
venv/bin/python main.py replay --capture /tmp/bot-smoke --output /tmp/bot-replay
venv/bin/python -m pytest -q
```

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

The [code guide](docs/CODE_GUIDE.md), [paper execution guide](docs/PAPER_TRADING.md),
[risk manager guide](docs/RISK_MANAGER.md) and [documentation index](docs/_index.md)
cover the component APIs. Existing session/demo commands remain supported:

```bash
venv/bin/python -m examples.paper_trading_demo --output /tmp/paper-demo
venv/bin/python -m src.paper_trading.session replay --capture /tmp/paper-demo --output /tmp/paper-replay
```
