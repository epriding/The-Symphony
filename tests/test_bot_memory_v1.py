"""Finite workload guards; these are not long-run release memory evidence."""
from decimal import Decimal
import importlib
import json

import pytest


@pytest.mark.asyncio
async def test_increasing_real_trading_workload_bounds_retained_state_report_and_replay(tmp_path):
    benchmark = importlib.import_module("examples.bot_trading_benchmark").benchmark
    measurements = []
    for cycles in (12, 48):
        directory = tmp_path / f"workload-{cycles}"
        result = await benchmark(directory, cycles=cycles)
        assert result["cycles"] == cycles
        original, replay = result["summary"], result["replay_summary"]
        assert original["submitted"] == original["full_fills"] == 4 * cycles
        assert original["accounts"] == replay["accounts"]
        for summary in (original, replay):
            assert summary["submitted"] == summary["full_fills"] == 4 * cycles
            assert Decimal(str(summary["fees"])) == 0
            account = summary["accounts"]["PAPER"]
            assert account["open_orders"] == account["open_positions"] == account["pending_reservations"] == 0
        # The effective immutable capture configuration supplies every numeric
        # bound; this test does not invent a production memory/latency target.
        manifest = json.loads((directory / "capture/manifest.json").read_text())
        config = manifest["runtime_config"]
        limits = config["runtime"]["limits"]
        high = result["high_water"]
        for measured, configured in (("active_orders", "max_active_orders"),
                                     ("positions", "max_positions"),
                                     ("archive_cache_records", "max_archive_cache_records")):
            assert 0 <= high[measured] <= limits[configured], (measured, high[measured], configured, limits[configured])
        # Instrumentation can observe the authoritative terminal record between
        # accounting and its successful commit/eviction. The frozen retention
        # contract permits this finite active-order cleanup headroom as well.
        assert high["hot_history_records"] <= (limits["max_hot_history_records"]
                                                + limits["max_active_orders"])
        assert high["pending_events"] <= config["execution"]["max_pending_events"]
        assert high["outbox"] <= config["execution"]["max_outbox"]
        dependency_headroom = (limits["max_active_orders"] + limits["max_hot_history_records"]
                               + config["execution"]["max_outbox"])
        assert high["engine_orders"] <= dependency_headroom
        assert high["summary_bytes"] < 1024 * 1024
        assert result["detail_records"]["executions"] == 4 * cycles
        assert result["archive_bytes"] > 0 and result["elapsed_seconds"] > 0
        memory = result["memory"]
        assert memory["budget_bytes"] == limits["memory_budget_bytes"]
        assert 0 <= memory["python_peak_delta_bytes"] <= memory["budget_bytes"]
        assert memory["rss_peak_bytes"] >= memory["rss_baseline_bytes"] > 0
        assert memory["rss_peak_bytes"] - memory["rss_baseline_bytes"] <= memory["budget_bytes"]
        assert memory["rss_final_bytes"] > 0
        assert {"warmup", "trading", "report", "replay"} <= set(memory["samples"])
        measurements.append((result, limits))
    small, _ = measurements[0]
    large, limits = measurements[1]
    assert large["archive_bytes"] > small["archive_bytes"], "The increasing workload did not archive more history"
    # Capacity and short-lived dependency headroom are fixed as workload grows.
    # Linear lists/ID sets with one item per historical fill must fail this guard.
    headroom = limits["max_active_orders"] + limits["max_hot_history_records"]
    for key in ("engine_orders", "engine_executions", "processed_ids", "book_levels"):
        assert large["high_water"][key] <= small["high_water"][key] + headroom
    assert large["high_water"]["summary_bytes"] <= small["high_water"]["summary_bytes"] + 4096
