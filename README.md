# Poly Bot v1

A minimal scaffold for a Polymarket bot.

## Structure

- `main.py` - entrypoint
- `src/` - bot modules
- `.env.example` - sample environment variables

See [the code guide](docs/CODE_GUIDE.md) for the order flow, module ownership,
and suggested starting points for reading the implementation.

## Run

```bash
python3 main.py
```

If you want to use a local `.env` file, copy `.env.example` to `.env` and adjust the values.

## Risk manager

See [risk manager usage](docs/RISK_MANAGER.md) for configuration, sizing, paper/live
accounts, batches, and recovery. Run the complete offline example with:

```bash
venv/bin/python -m examples.risk_manager_demo
venv/bin/python -m pytest -q
```

The production bot loop is still a scaffold. The example uses simulated markets
and balances and makes no network requests.

## Paper trading

See [paper trading usage](docs/PAPER_TRADING.md) for Polymarket US/Kalshi public
feeds, configuration, pending submissions, execution assumptions, and replay.

```bash
venv/bin/python -m examples.paper_trading_demo --output /tmp/paper-demo
venv/bin/python -m src.paper_trading.session replay --capture /tmp/paper-demo --output /tmp/paper-replay
```

The shared simulator models depth, partial fills, conservative resting queues,
fees, latency and cancel races. Live-feed authentication and empirical execution
calibration still need validation with representative data.
