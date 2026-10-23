# Poly Bot v1

A minimal scaffold for a Polymarket bot.

## Structure

- `main.py` - entrypoint
- `src/` - bot modules
- `.env.example` - sample environment variables

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
