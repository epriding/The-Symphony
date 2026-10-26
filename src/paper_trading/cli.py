"""Explicit configuration for capture, live paper sessions, and offline replay."""
import argparse
import asyncio
import json
import os
from pathlib import Path

from .clock import RealClock
from .live import run_live
from .models import Delay, FeeSchedule, VenueRules, PaperExecutionConfig
from .replay import replay
from .session import PaperSession
from ..utilities.classes import TradingConfig, RiskConfig


def load_configuration(path):
    raw = json.loads(Path(path).read_text())
    config = TradingConfig()
    config.strategy_portfolios = raw["strategy_portfolios"]
    config.paper_starting_balances = raw["starting_balances"]
    config.market_overrides = raw.get("market_overrides", {})
    for name, settings in raw.get("risk", {"PAPER": {}}).items():
        config.risk_configs[name] = RiskConfig(**settings)
    venues = {}
    for exchange, settings in raw["execution"]["venues"].items():
        venues[exchange] = _load_venue_rules(exchange, settings)
    execution = {key: value for key, value in raw["execution"].items() if key != "venues"}
    if "processing_delay" in execution:
        execution["processing_delay"] = Delay(**execution["processing_delay"])
    config.paper_execution = PaperExecutionConfig(venues, **execution)
    return config, raw.get("feeds", {})


def _load_venue_rules(exchange, values):
    settings = dict(values)
    settings["fees"] = FeeSchedule(**settings["fees"])
    settings["market_fee_overrides"] = {
        market_id: FeeSchedule(**fees)
        for market_id, fees in settings.get("market_fee_overrides", {}).items()
    }
    for delay_name in ("outbound", "acknowledgement", "execution_report", "cancel"):
        settings[delay_name] = Delay(**settings[delay_name])
    return VenueRules(exchange=exchange, **settings)


def create_feeds(settings):
    from ..api.interfaces.kalshi import KalshiMarketData
    from ..api.kalshi.public_api import KalshiPublicAPI
    from ..api.interfaces.polymarket_us_public import PolymarketUSMarketData
    feeds = []
    for exchange, options in settings.items():
        if exchange == "KALSHI":
            key_id = os.environ.get(options.get("key_id_env", "KALSHI_API_KEY_ID"))
            private_key_path = os.environ.get(options.get("private_key_env", "KALSHI_PRIVATE_KEY_PATH"))
            if not key_id or not private_key_path:
                raise ValueError("Set Kalshi websocket credential environment variables")
            api = KalshiPublicAPI(key_id, private_key_path, read_interval=options["read_interval"])
            feeds.append(KalshiMarketData(
                options["markets"], api=api, overrides=options.get("market_rules", {}),
            ))
        elif exchange == "POLYMARKET_US":
            key_id = os.environ.get(options.get("key_id_env", "POLYMARKET_US_KEY_ID"))
            secret_key = os.environ.get(options.get("secret_key_env", "POLYMARKET_US_SECRET_KEY"))
            if not key_id or not secret_key:
                raise ValueError("Set Polymarket US websocket credential environment variables")
            feeds.append(PolymarketUSMarketData(
                options["markets"], key_id, secret_key,
                overrides=options.get("market_rules", {}), read_interval=options["read_interval"],
            ))
        else:
            raise ValueError(f"Register a public market-data adapter for {exchange}")
    if not feeds:
        raise ValueError("At least one public feed is required")
    return feeds


async def execute(args):
    if args.command == "replay":
        _, summary = await replay(args.capture, args.output)
    else:
        config, feed_settings = load_configuration(args.config)
        feeds = create_feeds(feed_settings)
        session = PaperSession(config, RealClock(), args.output)
        summary = await run_live(session, feeds, duration=args.duration,
                                 output_directory=args.output, metadata_interval=args.metadata_interval)
        seen = {exchange for (exchange, _), pool in session.engine.books.items() if pool.revision}
        missing = {feed.exchange for feed in feeds} - seen
        if missing:
            raise RuntimeError(f"No verified depth received for {sorted(missing)}; live-feed readiness remains unverified")
    print(json.dumps({key: summary[key] for key in ("session_id", "submitted", "fees", "accounts", "assumptions")}, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Paper trading and reproducible public-data replay")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("live", "capture"):
        command = commands.add_parser(name)
        command.add_argument("--config", required=True)
        command.add_argument("--output", required=True)
        command.add_argument("--duration", type=float, required=True)
        command.add_argument("--metadata-interval", type=float, default=30)
    command = commands.add_parser("replay")
    command.add_argument("--capture", required=True)
    command.add_argument("--output", required=True)
    try:
        asyncio.run(execute(parser.parse_args()))
    except KeyboardInterrupt:
        print("Paper session stopped; captured inputs and determined execution reports were finalized.")


if __name__ == "__main__":
    main()
