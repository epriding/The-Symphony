"""Whitelisted, typed JSONL records; decimals and numeric dictionary keys survive."""
from dataclasses import fields, is_dataclass
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from pathlib import Path
import json
import os
import subprocess

from ..utilities import classes, market_events
from . import models

SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = {1, SCHEMA_VERSION}
TYPES = {
    name: value
    for module in (classes, market_events, models)
    for name, value in vars(module).items()
    if isinstance(value, type) and (is_dataclass(value) or issubclass(value, Enum))
}


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
    def __init__(self, directory, config, started_at):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "manifest.json"
        # Refuse accidental overwrite of a prior session/capture.
        self.handle = (self.directory / "inputs.jsonl").open("x")
        try:
            revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True,
                                               stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            revision = "unknown"
        self.manifest = dict(schema_version=SCHEMA_VERSION, code_revision=revision,
                             started_at=started_at, settings=encode(trading_settings(config)),
                             finalized=False)
        with self.path.open("x") as output:
            json.dump(self.manifest, output, indent=2, allow_nan=False)
        self.digest = sha256()
        self.count = 0

    def write(self, kind, timestamp, payload, *, origin="driver", cause=None):
        row = dict(schema_version=SCHEMA_VERSION, kind=kind, at=timestamp,
                   payload=encode(payload), origin=origin, cause=cause)
        line = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        self.handle.write(line)
        self.digest.update(line.encode())
        self.count += 1

    def close(self, ended_at):
        if self.handle.closed:
            return
        self.handle.flush()
        os.fsync(self.handle.fileno())
        self.handle.close()
        self.manifest.update(finalized=True, ended_at=ended_at,
                             dataset_sha256=self.digest.hexdigest(), record_count=self.count)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.manifest, indent=2, allow_nan=False) + "\n")
        temporary.replace(self.path)


def read_capture(directory):
    """Verify integrity before execution; the second streaming pass yields records."""
    path = Path(directory)
    manifest = json.loads((path / "manifest.json").read_text())
    schema_version = manifest.get("schema_version")
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS or not manifest.get("finalized"):
        raise ValueError("Unsupported or incomplete capture")
    digest = sha256()
    count = 0
    with (path / "inputs.jsonl").open("rb") as source:
        for line in source:
            digest.update(line)
            count += 1
    if digest.hexdigest() != manifest["dataset_sha256"] or count != manifest["record_count"]:
        raise ValueError("Capture integrity check failed")
    return manifest, iter_records(path / "inputs.jsonl", schema_version)


def iter_records(path, schema_version=SCHEMA_VERSION):
    previous = float("-inf")
    with path.open() as source:
        for line in source:
            row = json.loads(line)
            if row.get("schema_version") != schema_version or row["at"] < previous:
                raise ValueError("Invalid capture schema or availability order")
            previous = row["at"]
            row["payload"] = decode(row["payload"])
            yield row
