"""P06/P10 schema-3 name/settings and genuine fresh-decision replay seams."""
import asyncio
from copy import deepcopy
from hashlib import sha256
import importlib
from importlib import metadata
import json
from pathlib import Path
import platform

import pytest

import src.bot as bot
from src.paper_trading.clock import VirtualClock
from src.utilities.classes import Side

from bot_v1_support import (
    START, US, VENUES, ControlledFeed, eventually, strategy_config, valid_config,
)


ORIGINAL_SETTINGS = {
    "alpha": {"enter": True, "nested": {"price": .5}, "labels": ["alpha"]},
    "beta": {"enter": False, "nested": {"price": .45}, "labels": ["beta"]},
}


def configured_factory(seen, calls, inventory, *, abstain=False):
    def factory(context):
        seen[context.strategy_name] = deepcopy(context.settings)

        class Configured:
            done = False

            async def on_snapshot(self, snapshot):
                calls[context.strategy_name] = calls.get(context.strategy_name, 0) + 1
                if snapshot.portfolios[context.portfolio_id].positions:
                    inventory.set()
                if self.done:
                    return None
                self.done = True
                enter = context.settings["enter"]
                price = context.settings["nested"]["price"]
                # Mutate both nested settings and a list after using originals.
                # Manifest/fresh replay must retain the original name association.
                context.settings["enter"] = not enter
                context.settings["nested"]["price"] = .99
                context.settings["labels"].append("context-only")
                if abstain or not enter:
                    return None
                return [context.new_intent(
                    market_id="m", exchange=US, outcome="YES", side=Side.BUY,
                    price=price, estimated_probability=1,
                    resolution_date=context.now() + 3600)]

        return Configured()
    return factory


async def record_configured_capture(directory):
    config = valid_config(strategies=[
        strategy_config(name, factory="configured", settings=settings)
        for name, settings in ORIGINAL_SETTINGS.items()])
    preserved = deepcopy(config)
    clock = VirtualClock(START)
    feeds = [ControlledFeed(venue, clock=clock) for venue in VENUES]
    seen, calls, inventory = {}, {}, asyncio.Event()
    runner = bot.BotRunner(config, output_directory=directory, feeds=feeds, clock=clock,
        strategy_factories={"configured": configured_factory(seen, calls, inventory)})
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: task.done() or all(feed.started.is_set() for feed in feeds))
        if task.done():
            task.result()
        for _ in range(12):
            prior = sum(calls.values())
            clock.advance(clock() + .05)
            for feed in feeds:
                await feed.push_book()
            await eventually(lambda: sum(calls.values()) > prior or task.done())
            if task.done():
                task.result()
                pytest.fail("Capture ended before the controlled settings decision executed")
            if inventory.is_set() and set(calls) == set(ORIGINAL_SETTINGS):
                break
        assert inventory.is_set() and seen == ORIGINAL_SETTINGS
        for feed in feeds:
            await feed.end()
        summary = await asyncio.wait_for(asyncio.shield(task), 5)
        assert summary["submitted"] == summary["full_fills"] == 1
        assert config == preserved
        manifest = json.loads((directory / "manifest.json").read_text())
        assert manifest["schema_version"] == 3
        entries = manifest["runtime_config"]["strategies"]
        assert {entry["name"]: entry["settings"] for entry in entries} == ORIGINAL_SETTINGS
        assert all(entry["factory"] == "configured" for entry in entries)
        return summary
    finally:
        if not task.done():
            runner.request_stop()
            done, pending = await asyncio.wait({task}, timeout=2)
            if pending:
                task.cancel()
                done, pending = await asyncio.wait({task}, timeout=2)
                assert not pending, "Capture fixture exceeded cooperative cleanup deadline"
            await asyncio.gather(*done, return_exceptions=True)
        else:
            await asyncio.gather(task, return_exceptions=True)


def execution_rows(directory):
    with (directory / "executions.jsonl").open() as source:
        return [json.loads(line) for line in source]


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [0, .0002])
@pytest.mark.parametrize("fresh", [False, True])
async def test_schema3_recorded_and_fresh_replay_use_original_named_settings_at_each_pace(
        tmp_path, delay, fresh):
    original = await record_configured_capture(tmp_path / "capture")
    driver = importlib.import_module("src.paper_trading.replay").replay_runtime
    seen, calls, inventory, paced = {}, {}, asyncio.Event(), []

    async def pacing(record):
        paced.append((record["kind"], record["at"]))
        await asyncio.sleep(delay)

    registry = {"configured": configured_factory(seen, calls, inventory)} if fresh else None
    result = await asyncio.wait_for(driver(tmp_path / "capture", tmp_path / "replay",
        strategy_factories=registry, pacing=pacing), 5)
    assert result["accounts"] == original["accounts"]
    assert result["submitted"] == result["full_fills"] == 1
    assert execution_rows(tmp_path / "replay") == execution_rows(tmp_path / "capture")
    assert paced, "The private replay pacing seam was ignored"
    if fresh:
        assert seen == ORIGINAL_SETTINGS
        assert set(calls) == set(ORIGINAL_SETTINGS) and all(calls.values())
    else:
        assert not seen and not calls


@pytest.mark.asyncio
async def test_fresh_replay_detects_a_factory_that_disagrees_with_recorded_decisions(tmp_path):
    await record_configured_capture(tmp_path / "capture")
    driver = importlib.import_module("src.paper_trading.replay").replay_runtime
    seen, calls = {}, {}
    with pytest.raises((ValueError, RuntimeError)):
        await asyncio.wait_for(driver(tmp_path / "capture", tmp_path / "replay",
            strategy_factories={"configured": configured_factory(
                seen, calls, asyncio.Event(), abstain=True)}), 5)
    assert seen == ORIGINAL_SETTINGS and calls, "Fresh replay never invoked the supplied strategies"


@pytest.mark.asyncio
async def test_replay_final_store_cleanup_failure_cannot_leave_clean_artifacts(tmp_path, monkeypatch):
    await record_configured_capture(tmp_path / "capture")
    store_type = importlib.import_module("src.paper_trading.history").HistoryStore
    original = store_type.aclose
    output = tmp_path / "failed-replay"
    closed = []

    async def failed_owned_close(store):
        result = await original(store)
        if store.path.parent == output and not closed:
            closed.append(store)
            raise OSError("dummy-final-replay-store-cleanup-failure")
        return result

    monkeypatch.setattr(store_type, "aclose", failed_owned_close)
    with pytest.raises(OSError, match="final-replay-store-cleanup-failure"):
        await asyncio.wait_for(bot.replay_bot(tmp_path / "capture", output), 5)
    assert len(closed) == 1 and not closed[0]._thread.is_alive()
    assert json.loads((output / "manifest.json").read_text())["finalized"] is False
    assert json.loads((output / "outcome.json").read_text())["clean"] is False
    summary_path = output / "summary.json"
    if summary_path.exists():
        assert json.loads(summary_path.read_text())["runtime"]["clean"] is False


@pytest.mark.asyncio
async def test_schema3_provenance_matches_actual_product_sources_and_installed_versions(tmp_path):
    output = tmp_path / "provenance"
    await record_configured_capture(output)
    manifest = json.loads((output / "manifest.json").read_text())
    provenance = manifest["provenance"]
    assert provenance["versions"]["python"] == platform.python_version()
    for distribution in ("polymarket-us", "aiohttp", "websockets"):
        try:
            version = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            version = "unknown"
        assert provenance["versions"][distribution] == version
    fingerprint = provenance["source_fingerprint"]
    assert fingerprint["algorithm"] == "sha256"
    assert len(fingerprint["sha256"]) == 64
    assert int(fingerprint["sha256"], 16) >= 0
    sources = fingerprint["files"]
    assert 2 <= len(sources) <= 128
    assert {"src/paper_trading/runtime.py", "src/api/interfaces/polymarket_us_public.py"} <= sources.keys()
    root = Path(__file__).resolve().parents[1]
    for relative, digest in sources.items():
        path = Path(relative)
        # Validate the product-only scope before reading any named file. A
        # credential/config/host path is a provenance defect, never test input.
        assert not path.is_absolute() and ".." not in path.parts
        assert path.suffix == ".py" and (relative == "main.py" or path.parts[0] == "src")
        assert digest == sha256((root / path).read_bytes()).hexdigest()
