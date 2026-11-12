"""Whitelisted, typed JSONL records; decimals and numeric dictionary keys survive."""
from dataclasses import fields, is_dataclass
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from importlib import metadata
from pathlib import Path
import json
import os
import platform
import subprocess

from ..utilities import classes, market_events
from . import models

SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = {1, 2, 3, 4}
MAX_HEADER_BYTES = 16 * 1024 * 1024
COMPATIBILITY_EVENT_BYTES = 262144
TYPES = {
    name: value
    for module in (classes, market_events, models)
    for name, value in vars(module).items()
    if isinstance(value, type) and (is_dataclass(value) or issubclass(value, Enum))
}

# Explicit product sources only: never inspect configuration, credential files,
# arbitrary working-tree paths or environment values for capture provenance.
PROVENANCE_PATHS = (
    'main.py', 'src/bot.py',
    'src/operator/__init__.py', 'src/operator/protocol.py',
    'src/operator/service.py', 'src/operator/observations.py',
    'src/paper_trading/runtime.py', 'src/paper_trading/runtime_config.py',
    'src/paper_trading/session.py', 'src/paper_trading/recording.py',
    'src/paper_trading/replay.py', 'src/paper_trading/history.py',
    'src/paper_trading/reporting.py', 'src/paper_trading/paper_trading_engine.py',
    'src/paper_trading/scheduler.py', 'src/paper_trading/clock.py',
    'src/paper_trading/models.py', 'src/paper_trading/fees.py',
    'src/paper_trading/orderbook.py', 'src/paper_trading/matching.py',
    'src/strategies/context.py', 'src/strategies/smoke.py',
    'src/market_decisions/order_coordinator.py', 'src/market_decisions/risk_manager.py',
    'src/utilities/classes.py', 'src/utilities/trading_state.py',
    'src/utilities/trading_rules.py', 'src/utilities/market_events.py',
    'src/utilities/logger.py', 'src/api/interfaces/bounded_public.py',
    'src/api/interfaces/public_market_data.py', 'src/api/interfaces/kalshi.py',
    'src/api/interfaces/polymarket_us_public.py', 'src/api/kalshi/public_api.py',
)


def runtime_provenance():
    versions = {'python': platform.python_version()}
    for distribution in ('polymarket-us', 'aiohttp', 'websockets'):
        try:
            versions[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            versions[distribution] = 'unknown'
    root = Path(__file__).resolve().parents[2]
    files = {}
    combined = sha256()
    for relative in PROVENANCE_PATHS:
        digest = sha256()
        with (root / relative).open('rb') as source:
            while chunk := source.read(65536):
                digest.update(chunk)
        files[relative] = digest.hexdigest()
        combined.update(relative.encode() + b'\0' + digest.digest())
    return dict(versions=versions, source_fingerprint=dict(
        algorithm='sha256', sha256=combined.hexdigest(), files=files))


def encode(value):
    if isinstance(value, Enum):
        return {"enum": type(value).__name__, "value": value.value}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if is_dataclass(value):
        return {"type": type(value).__name__, "fields": {
            field.name: encode(getattr(value, field.name)) for field in fields(value)}}
    if isinstance(value, dict):
        sorted_items = sorted(value.items(), key=lambda item: str(item[0]))
        return {"pairs": [[encode(key), encode(item)] for key, item in sorted_items]}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"Unsupported recording value {type(value).__name__}")


def decode(value):
    if isinstance(value, list):
        return [decode(v) for v in value]
    if not isinstance(value, dict):
        return value
    if "decimal" in value:
        return Decimal(value["decimal"])
    if "enum" in value:
        return TYPES[value["enum"]](value["value"])
    if "type" in value:
        record_type = TYPES[value["type"]]
        record_fields = {key: decode(item) for key, item in value["fields"].items()}
        return record_type(**record_fields)
    if "pairs" in value:
        return {decode(key): decode(item) for key, item in value["pairs"]}
    raise ValueError("Invalid typed capture object")


def trading_settings(config):
    return {key: getattr(config, key) for key in (
        "strategy_portfolios", "paper_trading", "paper_starting_balances",
        "risk_configs", "market_overrides", "paper_execution")}


class Recorder:
    def __init__(self, directory, config, started_at, *, runtime_config=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "manifest.json"
        # Refuse accidental overwrite of a prior session/capture.
        self.handle = (self.directory / "inputs.jsonl").open("x")
        try:
            try:
                revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True,
                                                   stderr=subprocess.DEVNULL).strip()
            except (OSError, subprocess.CalledProcessError):
                revision = "unknown"
            self.schema_version = 3 if runtime_config is not None else SCHEMA_VERSION
            self.max_event_bytes = (runtime_config['runtime']['limits']['max_event_bytes']
                                    if runtime_config is not None else None)
            self.manifest = dict(schema_version=self.schema_version, code_revision=revision,
                                 started_at=started_at, settings=encode(trading_settings(config)),
                                 finalized=False)
            if runtime_config is not None:
                # Serialize now: factories receive different copies of these original values.
                self.manifest['runtime_config'] = json.loads(json.dumps(runtime_config, allow_nan=False))
                self.manifest['identity_mapping'] = {}
                self.manifest['context_initialization'] = dict(
                    seed=config.paper_execution.seed, session_id=config.paper_execution.session_id,
                    derivation='sha256(seed:name); sha256(name) intent prefix v1')
                self.manifest['provenance'] = runtime_provenance()
            with self.path.open("x") as output:
                json.dump(self.manifest, output, indent=2, allow_nan=False)
        except BaseException as error:
            try:
                self.handle.close()
            except BaseException as cleanup_error:
                error.add_note(f'partial capture close: {type(cleanup_error).__name__}')
            raise
        self.digest = sha256()
        self.count = 0

    def write(self, kind, timestamp, payload, *, origin="driver", cause=None):
        row = dict(schema_version=self.schema_version, kind=kind, at=timestamp,
                   payload=encode(payload), origin=origin, cause=cause)
        line = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        if self.max_event_bytes is not None and len(line.encode()) > self.max_event_bytes:
            raise BufferError('capture_record_exceeds_max_event_bytes')
        self.handle.write(line)
        self.digest.update(line.encode())
        self.count += 1

    def close(self, ended_at, *, finalized=True):
        if self.handle.closed:
            return
        try:
            self.handle.flush()
            os.fsync(self.handle.fileno())
        finally:
            self.handle.close()
        self.manifest.update(finalized=finalized, ended_at=ended_at,
                             dataset_sha256=self.digest.hexdigest(), record_count=self.count)
        self._write_manifest()

    def _write_manifest(self):
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.manifest, indent=2, allow_nan=False) + "\n")
        temporary.replace(self.path)

    def finalize(self):
        """Publish completion only after the runtime verifies owned cleanup."""
        if not self.handle.closed or 'dataset_sha256' not in self.manifest:
            raise RuntimeError('Capture has not been successfully closed')
        self.manifest['finalized'] = True
        try:
            self._write_manifest()
        except BaseException:
            self.manifest['finalized'] = False
            raise


def read_capture(directory):
    """Verify integrity before execution; the second streaming pass yields records."""
    path = Path(directory)
    with (path / 'manifest.json').open('rb') as source:
        header = source.read(MAX_HEADER_BYTES + 1)
    if len(header) > MAX_HEADER_BYTES:
        raise ValueError('Capture manifest exceeds bounded header limit')
    manifest = json.loads(header)
    schema_version = manifest.get("schema_version")
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS or not manifest.get("finalized"):
        raise ValueError("Unsupported or incomplete capture")
    digest = sha256()
    count = 0
    row_limit = None
    if schema_version in (3, 4):
        from .runtime_config import validate_bot_config
        runtime_config = validate_bot_config(manifest.get('runtime_config'), require_runtime=True)
        row_limit = runtime_config['runtime']['limits']['max_event_bytes']
    else:
        row_limit = COMPATIBILITY_EVENT_BYTES
    with (path / "inputs.jsonl").open("rb") as source:
        while line := source.readline(row_limit + 1):
            if len(line) > row_limit or not line.endswith(b'\n'):
                raise ValueError('Oversized or truncated capture record')
            digest.update(line)
            count += 1
    if digest.hexdigest() != manifest["dataset_sha256"] or count != manifest["record_count"]:
        raise ValueError("Capture integrity check failed")
    return manifest, iter_records(path / "inputs.jsonl", schema_version, max_event_bytes=row_limit)


def iter_records(path, schema_version=SCHEMA_VERSION, *, max_event_bytes=COMPATIBILITY_EVENT_BYTES):
    previous = float("-inf")
    with Path(path).open() as source:
        while line := source.readline(max_event_bytes + 1):
            if len(line.encode()) > max_event_bytes or not line.endswith('\n'):
                raise ValueError('Oversized or truncated capture record')
            row = json.loads(line)
            import math
            if (row.get("schema_version") != schema_version or isinstance(row['at'], bool)
                    or not math.isfinite(row['at']) or row["at"] < previous):
                raise ValueError("Invalid capture schema or availability order")
            previous = row["at"]
            row["payload"] = decode(row["payload"])
            yield row
