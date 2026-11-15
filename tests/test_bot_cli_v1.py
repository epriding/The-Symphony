"""Independent CLI/config/logging acceptance for confirmed bot-v1 O02/O03.

All CLI processes block socket connection attempts and dotenv file reads. No
credentials or external feeds are required, even when these tests fail red.
"""
import asyncio
import json
import logging
import os
from pathlib import Path
import runpy
import socket
import subprocess
import sys

import pytest

import src.bot as bot_module
from src.utilities.logger import setup_logger

from bot_v1_support import valid_config


ROOT = Path(__file__).resolve().parents[1]
SENTINEL = "BOT_V1_TEST_SECRET_DO_NOT_LOG_486103"


@pytest.mark.parametrize('arguments', [[],['--smoke','--config','nonexistent.json'],['--config','nonexistent.json']])
def test_serve_invalid_modes_and_config_fail_before_starting_any_session(tmp_path,arguments):
    result=run_cli('serve',*arguments,'--socket',tmp_path/'private/control.sock',
        '--output-root',tmp_path/'runs')
    assert result.returncode==2
    assert not (tmp_path/'runs').exists()
    assert not (tmp_path/'private/control.sock').exists()
_LAUNCHER = """
import runpy, socket, sys
import dotenv
dotenv.load_dotenv = lambda *args, **kwargs: False
def no_network(*args, **kwargs):
    raise AssertionError('offline bot-v1 CLI test attempted a network connection')
socket.socket.connect = no_network
socket.create_connection = no_network
sys.argv = ['main.py', *sys.argv[1:]]
runpy.run_path('main.py', run_name='__main__')
"""


def run_cli(*arguments):
    env = os.environ.copy()
    for key in ("POLYMARKET_US_KEY_ID", "POLYMARKET_US_SECRET_KEY", "KALSHI_API_KEY_ID"):
        env[key] = SENTINEL
    env["KALSHI_PRIVATE_KEY_PATH"] = "/nonexistent/offline-fixture-key.pem"
    return subprocess.run([sys.executable, "-c", _LAUNCHER, *map(str, arguments)],
                          cwd=ROOT, env=env, capture_output=True, text=True,
                          timeout=8, check=False)


def public_api(name):
    assert hasattr(bot_module, name), f"Confirmed bot-v1 API is not implemented: {name}"
    return getattr(bot_module, name)


TIMINGS = ("startup_timeout_seconds", "strategy_timeout_seconds",
           "risk_interval_seconds", "shutdown_grace_seconds")
LIMITS = ("max_strategies", "max_markets", "max_active_orders", "max_positions",
          "max_intents_per_callback", "max_hot_history_records",
          "max_archive_cache_records", "max_event_bytes", "memory_budget_bytes")


@pytest.mark.parametrize("section,key", [("runtime", key) for key in TIMINGS] +
                         [("limits", key) for key in LIMITS])
def test_loader_rejects_unbounded_or_zero_runtime_configuration(tmp_path, section, key):
    loader = public_api("load_bot_config")
    config = valid_config(strategies=[], source="public", observe_only=True)
    target = config["runtime"] if section == "runtime" else config["runtime"]["limits"]
    target[key] = 0
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        loader(path)


@pytest.mark.parametrize("invalid", [float("inf"), float("nan"), True])
def test_loader_rejects_nonfinite_or_boolean_timing(tmp_path, invalid):
    loader = public_api("load_bot_config")
    config = valid_config(strategies=[], source="public", observe_only=True)
    config["runtime"]["risk_interval_seconds"] = invalid
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        loader(path)


def test_loader_accepts_explicit_strategy_free_public_observation_without_activation(tmp_path, monkeypatch):
    loader = public_api("load_bot_config")
    config = valid_config(strategies=[], source="public", observe_only=True)
    path = tmp_path / "observation.json"
    path.write_text(json.dumps(config))

    def forbid_network(*args, **kwargs):
        raise AssertionError("configuration loading must not start transport")
    monkeypatch.setattr(socket.socket, "connect", forbid_network)
    loaded = loader(path)
    assert isinstance(loaded, dict)
    json.dumps(loaded, allow_nan=False)
    assert loaded["runtime"]["observe_only"] is True
    assert loaded["strategies"] == []
    assert loaded["feeds"] == config["feeds"]
    assert set(tmp_path.iterdir()) == {path}


@pytest.mark.parametrize("problem", ["missing_file", "invalid_json", "invalid_limits"])
def test_run_command_returns_configuration_exit_code_without_network(tmp_path, problem):
    path = tmp_path / "config.json"
    if problem == "invalid_json":
        path.write_text("{not-json")
    elif problem == "invalid_limits":
        config = valid_config(strategies=[], source="public", observe_only=True)
        config["runtime"]["limits"]["memory_budget_bytes"] = 0
        path.write_text(json.dumps(config))
    result = run_cli("run", "--config", path, "--output", tmp_path / "output",
                     "--observe-only", "--duration", .01)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert SENTINEL not in result.stdout + result.stderr


def test_smoke_command_invokes_real_public_runner(tmp_path, monkeypatch):
    runner_type = public_api("BotRunner")
    original_run = runner_type.run
    invocations = []

    async def traced_run(self, *args, **kwargs):
        invocations.append(self)
        return await asyncio.wait_for(original_run(self, *args, **kwargs), 3)
    monkeypatch.setattr(runner_type, "run", traced_run)
    monkeypatch.setattr(sys, "argv", ["main.py", "smoke", "--output", str(tmp_path / "smoke")])

    def forbid_network(*args, **kwargs):
        raise AssertionError("synthetic smoke attempted a network connection")
    monkeypatch.setattr(socket.socket, "connect", forbid_network)
    try:
        runpy.run_path(str(ROOT / "main.py"), run_name="__main__")
    except SystemExit as exit_result:
        assert exit_result.code in (0, None)
    assert len(invocations) == 1, "smoke bypassed the real BotRunner"
    summary = json.loads((tmp_path / "smoke/summary.json").read_text())
    assert summary["submitted"] > 0 and summary["accounts"]["PAPER"]


def test_smoke_and_recorded_decision_replay_reproduce_execution_and_accounts(tmp_path):
    captures = [tmp_path / "first", tmp_path / "second"]
    summaries = []
    for output in captures:
        result = run_cli("smoke", "--output", output)
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert SENTINEL not in result.stdout + result.stderr
        summaries.append(json.loads((output / "summary.json").read_text()))
        manifest = json.loads((output / "manifest.json").read_text())
        assert manifest["finalized"] is True
        assert summaries[-1]["submitted"] > 0
        assert "synthetic" in summaries[-1]["assumptions"].lower()
    replay_output = tmp_path / "replay"
    replay_result = run_cli("replay", "--capture", captures[0], "--output", replay_output)
    assert replay_result.returncode == 0, (replay_result.stdout, replay_result.stderr)
    replay_summary = json.loads((replay_output / "summary.json").read_text())
    assert summaries[0]["accounts"] == summaries[1]["accounts"] == replay_summary["accounts"]
    assert summaries[0]["submitted"] == summaries[1]["submitted"] == replay_summary["submitted"]
    api_replay_output = tmp_path / "api-replay"
    api_summary = asyncio.run(public_api("replay_bot")(captures[0], api_replay_output))
    assert api_summary["accounts"] == summaries[0]["accounts"]
    assert api_summary["submitted"] == summaries[0]["submitted"]

    def executions(directory):
        return [json.loads(line) for line in (directory / "executions.jsonl").read_text().splitlines()]
    first = executions(captures[0])
    assert first, "smoke submitted decisions but never exercised execution accounting"
    assert first == executions(captures[1]) == executions(replay_output) == executions(api_replay_output)
    for directory in [*captures, replay_output, api_replay_output]:
        for artifact in directory.rglob("*"):
            if artifact.is_file() and artifact.suffix in {".json", ".jsonl", ".log"}:
                assert SENTINEL not in artifact.read_text()


def test_smoke_refuses_existing_output_and_preserves_artifacts(tmp_path):
    output = tmp_path / "existing"
    output.mkdir()
    previous = {"manifest.json": b'{"preserve":true}', "inputs.jsonl": b'original-inputs\n'}
    for filename, content in previous.items():
        (output / filename).write_bytes(content)
    result = run_cli("smoke", "--output", output)
    assert result.returncode != 0, "smoke falsely reported success with existing capture artifacts"
    assert {filename: (output / filename).read_bytes() for filename in previous} == previous


def test_file_only_repeated_logger_setup_retains_traceback_without_duplicate_handlers(tmp_path, monkeypatch):
    monkeypatch.setenv("POLYMARKET_US_SECRET_KEY", SENTINEL)
    name = "bot-v1-file-only-" + tmp_path.name
    path = tmp_path / "bot.log"
    logger = logging.getLogger(name)
    try:
        result = setup_logger(name, log_file=str(path), console=False)
        again = setup_logger(name, log_file=str(path), console=False)
        assert result is again and len(logger.handlers) == 1
        try:
            raise ValueError("synthetic source-context error")
        except ValueError:
            logger.exception("visible offline failure")
        for handler in logger.handlers:
            handler.flush()
        text = path.read_text()
        assert "synthetic source-context error" in text
        assert Path(__file__).name in text and "ValueError" in text
        assert text.count("visible offline failure") == 1
        assert SENTINEL not in text
    finally:
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)
