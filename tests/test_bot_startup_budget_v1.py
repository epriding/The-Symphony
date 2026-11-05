"""Configured public startup bursts must fit the admitted memory budget."""
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from src.paper_trading import runtime
from src.paper_trading.runtime_config import validate_bot_config

from bot_v1_support import US, valid_config


def configuration(markets, *, source="public"):
    config = valid_config(source=source)
    config["feeds"][US]["markets"] = ["m"] + [f"fixture-{index}" for index in range(1, markets)]
    config["runtime"]["limits"]["max_markets"] = markets + 1
    return config


def minimum_admitted_budget(config):
    candidate = deepcopy(config)
    low, high = 1, candidate["runtime"]["limits"]["memory_budget_bytes"]
    validate_bot_config(candidate, require_runtime=True)
    while low < high:
        middle = (low + high) // 2
        candidate["runtime"]["limits"]["memory_budget_bytes"] = middle
        try:
            validate_bot_config(candidate, require_runtime=True)
        except ValueError as error:
            assert "memory_budget_bytes" in str(error)
            low = middle + 1
        else:
            high = middle
    return low


def test_public_startup_burst_cannot_be_admitted_when_its_wire_bytes_exceed_budget(
        tmp_path, monkeypatch):
    config = configuration(16)
    limits = config["runtime"]["limits"]
    # One initial message for each market-data and trade subscription must fit
    # before the stream yields its first connected event and starts draining.
    startup_wire_bytes = 2 * len(config["feeds"][US]["markets"]) * limits["max_event_bytes"]
    limits["memory_budget_bytes"] = startup_wire_bytes - 1
    factory = AsyncMock(side_effect=AssertionError("Transport opened before budget admission"))
    monkeypatch.setattr(runtime, "_create_public_feeds", factory)
    output = tmp_path / "rejected"

    with pytest.raises(ValueError, match="memory_budget_bytes"):
        runtime.BotRunner(config, output_directory=output,
                          strategy_factories={"one": lambda context: object()})

    factory.assert_not_called()
    assert not output.exists()


def test_public_queue_growth_has_matching_admission_and_runtime_reserves(tmp_path):
    configs = {(source, count): configuration(count, source=source)
               for source in ("public", "synthetic") for count in (1, 3)}
    runners = {key: runtime.BotRunner(config, output_directory=tmp_path / f"{key[0]}-{key[1]}",
                                     strategy_factories={"one": lambda context: object()})
               for key, config in configs.items()}
    floors = {key: minimum_admitted_budget(config) for key, config in configs.items()}
    public_growth = (runners["public", 3]._reserved_bytes
                     - runners["public", 1]._reserved_bytes)
    # The same subscription-list overhead is present in both sources; subtract
    # it to isolate admission headroom for the actual public transport queues.
    admission_growth = ((floors["public", 3] - floors["public", 1])
                        - (floors["synthetic", 3] - floors["synthetic", 1]))

    assert public_growth > 0
    assert admission_growth == public_growth
    assert runners["synthetic", 3]._reserved_bytes == runners["synthetic", 1]._reserved_bytes
    for runner in runners.values():
        assert runner._reserved_bytes < runner.limits["memory_budget_bytes"]
