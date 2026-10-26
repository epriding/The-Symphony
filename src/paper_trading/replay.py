"""Streaming replay of driver steps and the decisions made within those steps."""
from .clock import VirtualClock
from .recording import read_capture, decode
from .session import PaperSession
from ..utilities.classes import TradingConfig


class _ReplayInputs:
    """One-record lookahead lets callbacks consume their recorded decisions."""
    def __init__(self, records):
        self.records = iter(records)
        self.next_record = next(self.records, None)

    def pop(self):
        record = self.next_record
        self.next_record = next(self.records, None)
        return record


async def _apply_input(session, record):
    kind = record["kind"]
    payload = record["payload"]
    if kind == "public":
        session.publish(payload)
    elif kind == "public_immediate":
        await session.apply_public_immediately(payload)
    elif kind == "market":
        await session.add_market(payload)
    elif kind == "intent":
        await session.submit(payload)
    elif kind == "batch":
        await session.submit_batch(payload)
    elif kind == "cancel":
        await session.cancel(**payload)
    elif kind == "advance":
        await session.advance(**payload)
    else:
        raise ValueError(f"Unsupported capture input {kind}")


class _RecordedDecisions:
    def __init__(self, session, inputs):
        self.session = session
        self.inputs = inputs

    async def __call__(self):
        while self.inputs.next_record is not None:
            record = self.inputs.next_record
            if record.get("origin") != "strategy" or record.get("cause") != self.session._strategy_event:
                break
            self.inputs.pop()
            self.session.clock.advance(max(self.session.clock(), record["at"]))
            await _apply_input(self.session, record)


async def replay(directory, output_directory=None, strategy=None, execution_config=None, strategy_factory=None):
    manifest, records = read_capture(directory)
    settings = decode(manifest["settings"])
    config = TradingConfig()
    for key, value in settings.items():
        setattr(config, key, value)
    if execution_config is not None:
        config.paper_execution = execution_config
    session = PaperSession(config, VirtualClock(manifest["started_at"]), audit_directory=output_directory)
    if strategy is not None and strategy_factory is not None:
        raise ValueError("Use a strategy instance or factory, not both")
    session.strategy = strategy_factory(session) if strategy_factory else strategy
    replay_decisions = session.strategy is None
    if manifest["schema_version"] == 1:
        # Older captures have no driver boundaries. Honor their recorded file
        # order instead of letting later same-time public rows overtake intents.
        for record in records:
            await session.advance(record["at"])
            if replay_decisions or record["kind"] not in {"intent", "batch", "cancel"}:
                await _apply_input(session, record)
        await session.advance(manifest["ended_at"])
    else:
        inputs = _ReplayInputs(records)
        if replay_decisions:
            session._replay_decisions = _RecordedDecisions(session, inputs)
        while inputs.next_record is not None:
            record = inputs.pop()
            if record.get("origin") == "strategy":
                if replay_decisions:
                    raise ValueError("Recorded decision has no matching public callback")
                continue  # A fresh strategy supplies its own decisions.
            session.clock.advance(max(session.clock(), record["at"]))
            await _apply_input(session, record)
        # The explicit advances already ran all determined work. Do not create
        # a final pump that could execute arrivals left queued at capture end.
        session.clock.advance(max(session.clock(), manifest["ended_at"]))
        if replay_decisions:
            session._replay_decisions = None
    summary = await session.finish(output_directory)
    return session, summary
