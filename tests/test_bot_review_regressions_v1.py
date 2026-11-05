"""Independent offline regressions for the first runtime review findings."""
import asyncio
import base64
from copy import deepcopy
import time
import traceback

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, padding, rsa

import src.bot as bot
from src.api.interfaces.kalshi import KalshiMarketData
from src.api.kalshi.public_api import KalshiPublicAPI
from src.market_decisions.order_coordinator import OrderCoordinator
from src.paper_trading.runtime import _create_public_feeds
from src.utilities.classes import Side

from bot_v1_support import ControlledFeed, US, VENUES, eventually, strategy_config, valid_config


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_late", [True, False], ids=["late-result-rejected", "ontime-observed-late"])
async def test_callback_deadline_uses_completion_time_while_actual_driver_is_delayed(
        tmp_path, monkeypatch, completed_late):
    timeout = .3
    config = valid_config(strategies=[strategy_config("candidate"), strategy_config("healthy")])
    config["runtime"].update(strategy_timeout_seconds=timeout, shutdown_grace_seconds=.8)
    entered, callback_release, returned, driver_waiting, driver_release = (
        asyncio.Event() for _ in range(5))
    submitted, callback_tasks = [], []
    observed_times = {}
    original_submit = OrderCoordinator.submit
    original_compact = bot.BotRunner._compact
    blocked_once = False

    async def observe_submit(coordinator, intent):
        submitted.append(intent)
        return await original_submit(coordinator, intent)

    async def delayed_compact(runner):
        nonlocal blocked_once
        candidate = next(slot for slot in runner.slots if slot.entry["name"] == "candidate")
        if candidate.task is not None and not blocked_once:
            blocked_once = True
            # Model an awaited bounded archive/report operation. The genuine
            # driver cannot inspect completion until this operation returns.
            driver_waiting.set()
            await driver_release.wait()
        await original_compact(runner)

    class Candidate:
        def __init__(self, context):
            self.context = context

        async def on_snapshot(self, snapshot):
            callback_tasks.append(asyncio.current_task())
            observed_times["entered"] = time.monotonic()
            entered.set()
            await callback_release.wait()
            observed_times["returned"] = time.monotonic()
            returned.set()
            return [self.context.new_intent(
                market_id="m", exchange=US, outcome="YES", side=Side.BUY,
                price=.5, estimated_probability=1,
                resolution_date=self.context.now() + 3600)]

    class Healthy:
        def __init__(self, context):
            pass

        async def on_snapshot(self, snapshot):
            return None

    monkeypatch.setattr(OrderCoordinator, "submit", observe_submit)
    monkeypatch.setattr(bot.BotRunner, "_compact", delayed_compact)
    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot.BotRunner(config, output_directory=tmp_path / "deadline",
        feeds=feeds, strategy_factories={"candidate": Candidate, "healthy": Healthy})
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: task.done() or all(feed.started.is_set() for feed in feeds))
        if task.done():
            task.result()
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: task.done() or (entered.is_set() and driver_waiting.is_set()))
        if task.done():
            task.result()
            pytest.fail("Runner finished before the delayed-driver fixture was established")
        if completed_late:
            await asyncio.sleep(timeout + .1)
        callback_release.set()
        await asyncio.wait_for(returned.wait(), 1)
        await eventually(lambda: callback_tasks[0].done())
        assert driver_waiting.is_set() and not driver_release.is_set()
        duration = observed_times["returned"] - observed_times["entered"]
        assert (duration > timeout) if completed_late else (duration < timeout)
        # Both variants are first observed by the driver after the deadline.
        if not completed_late:
            await asyncio.sleep(timeout + .1)
        assert not submitted
        driver_release.set()
        if completed_late:
            await eventually(lambda: task.done() or "deadline" in
                runner.status["strategies"]["candidate"]["reason"])
            assert not task.done(), "A healthy strategy must keep the owner available"
            candidate = runner.status["strategies"]["candidate"]
            assert not candidate["ready"] and "deadline" in candidate["reason"]
            assert runner.status["strategies"]["healthy"]["ready"]
            assert not submitted, "A callback that completed after its deadline submitted an intent"
        else:
            await eventually(lambda: task.done() or len(submitted) == 1)
            if task.done():
                task.result()
            assert runner.status["strategies"]["candidate"]["ready"]
            assert runner.status["strategies"]["candidate"]["reason"] == ""
            assert len(submitted) == 1 and submitted[0].strategy == "candidate"
        runner.request_stop()
        summary = await asyncio.wait_for(asyncio.shield(task), 3)
        assert summary["submitted"] == (0 if completed_late else 1)
        assert runner.status["phase"] == "stopped"
        assert all(feed.close_count == 1 for feed in feeds)
    finally:
        callback_release.set()
        driver_release.set()
        if not task.done():
            runner.request_stop()
            done, pending = await asyncio.wait({task}, timeout=3)
            if pending:
                task.cancel()
                done, pending = await asyncio.wait({task}, timeout=3)
                assert not pending, "Runner ignored bounded fixture cleanup"
        await asyncio.gather(task, *callback_tasks, return_exceptions=True)


class RefreshAPI:
    def __init__(self):
        self.market = {"ticker": "selected", "event_ticker": "verified-event", "status": "active"}

    async def get(self, path):
        assert path == "/markets/selected"
        return {"market": deepcopy(self.market)}


def bounded_kalshi(api):
    return KalshiMarketData(["selected"], api=api,
        overrides={"selected": {"tick_size": ".01", "quantity_step": "1", "min_quantity": "1"}},
        max_markets=1, max_event_bytes=4096, max_book_levels=4)


def fee_receipt(index):
    return dict(received_at=1_800_000_000 + index, received_monotonic_ns=index, ingest_index=index)


@pytest.mark.asyncio
async def test_repeated_kalshi_event_churn_cannot_grow_or_redirect_verified_fee_membership():
    api = RefreshAPI()
    feed = bounded_kalshi(api)
    assert (await feed.get_market("selected")).active
    baseline_members, baseline_states = deepcopy(feed.normalizer.event_members), deepcopy(feed.normalizer.states)
    for index in range(100):
        api.market.update(event_ticker=f"changed-event-{index}", status="closed")
        with pytest.raises(ValueError, match="association"):
            await feed.get_market("selected")
        assert feed.normalizer.event_members == baseline_members == {"verified-event": {"selected"}}
        assert feed.normalizer.states == baseline_states == {"selected": "active"}
    assert feed.normalizer.normalize({"type": "event_fee_update", "sid": 3, "seq": 1,
        "msg": {"event_ticker": "changed-event-99"}}, fee_receipt(1)) == []
    assert not feed.normalizer.frozen_fees
    result = feed.normalizer.normalize({"type": "event_fee_update", "sid": 3, "seq": 2,
        "msg": {"event_ticker": "verified-event"}}, fee_receipt(2))
    assert [(row.event.market_id, row.event.status) for row in result] == [("selected", "FEE_RULES_CHANGED")]
    api.market.update(event_ticker="verified-event", status="active")
    for _ in range(100):
        assert (await feed.get_market("selected")).market_id == "selected"
    assert feed.normalizer.event_members == baseline_members
    assert feed.normalizer.states == {"selected": "FEE_RULES_CHANGED"}
    assert feed.normalizer.frozen_fees == {"selected"}


@pytest.mark.asyncio
@pytest.mark.parametrize("event_ticker", [None, "", False, 7, [], {}])
@pytest.mark.parametrize("previously_verified", [False, True])
async def test_missing_or_malformed_kalshi_event_rejected_before_retained_state_changes(
        event_ticker, previously_verified):
    api = RefreshAPI()
    feed = bounded_kalshi(api)
    if previously_verified:
        await feed.get_market("selected")
    before = deepcopy((feed.normalizer.event_members, feed.normalizer.states))
    api.market.update(event_ticker=event_ticker, status="closed")
    with pytest.raises(ValueError, match="association"):
        await feed.get_market("selected")
    assert (feed.normalizer.event_members, feed.normalizer.states) == before


@pytest.fixture(scope="module", params=["rsa", "ed25519"])
def signing_key(request):
    if request.param == "rsa":
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return ed25519.Ed25519PrivateKey.generate()


def key_bytes(key, encoding=serialization.Encoding.PEM):
    return key.private_bytes(encoding, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())


def assert_valid_signature(headers, key, key_id="generated-fixture-key"):
    assert headers["KALSHI-ACCESS-KEY"] == key_id
    stamp = headers["KALSHI-ACCESS-TIMESTAMP"]
    assert stamp.isdecimal()
    message = (stamp + "GET/trade-api/ws/v2").encode("ascii")
    signature = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"], validate=True)
    public = key.public_key()
    if isinstance(public, rsa.RSAPublicKey):
        public.verify(signature, message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256().digest_size),
            hashes.SHA256())
    else:
        public.verify(signature, message)


@pytest.mark.parametrize("form", ["pem", "escaped-pem", "escaped-crlf-pem", "base64-pem", "base64-der", "str-path", "pathlike", "der-path"])
def test_generated_kalshi_material_and_legacy_paths_produce_verifiable_signatures(
        tmp_path, signing_key, form):
    pem = key_bytes(signing_key)
    if form == "pem":
        source = pem.decode("ascii")
    elif form == "escaped-pem":
        source = pem.decode("ascii").replace("\n", "\\n")
    elif form == "escaped-crlf-pem":
        source = pem.decode("ascii").replace("\n", "\\r\\n")
    elif form == "base64-pem":
        source = base64.b64encode(pem).decode("ascii")
    elif form == "base64-der":
        source = base64.b64encode(key_bytes(signing_key, serialization.Encoding.DER)).decode("ascii")
    else:
        path = tmp_path / "generated-fixture-key.pem"
        path.write_bytes(key_bytes(signing_key, serialization.Encoding.DER) if form == "der-path" else pem)
        source = str(path) if form == "str-path" else path
    api = KalshiPublicAPI("generated-fixture-key", source, max_event_bytes=4096)
    assert_valid_signature(api.websocket_headers(), signing_key)


@pytest.mark.parametrize("file_source", [False, True])
def test_valid_generated_key_larger_than_configured_byte_cap_is_rejected(
        tmp_path, signing_key, file_source):
    pem = key_bytes(signing_key)
    source = pem.decode("ascii")
    if file_source:
        source = tmp_path / "generated-size-boundary.pem"
        source.write_bytes(pem)
    with pytest.raises(ValueError):
        KalshiPublicAPI("generated-fixture-key", source, max_event_bytes=len(pem) - 1).websocket_headers()
    assert_valid_signature(KalshiPublicAPI("generated-fixture-key", source,
        max_event_bytes=len(pem)).websocket_headers(), signing_key)


@pytest.mark.asyncio
async def test_public_feed_private_key_env_accepts_generated_in_memory_der_without_network(
        monkeypatch, signing_key):
    encoded = base64.b64encode(key_bytes(signing_key, serialization.Encoding.DER)).decode("ascii")
    monkeypatch.setenv("BOT_TEST_GENERATED_KALSHI_KEY_ID", "generated-fixture-key")
    monkeypatch.setenv("BOT_TEST_GENERATED_KALSHI_MATERIAL", encoded)
    settings = {"KALSHI": {"markets": ["selected"], "read_interval": .01,
        "key_id_env": "BOT_TEST_GENERATED_KALSHI_KEY_ID",
        "private_key_env": "BOT_TEST_GENERATED_KALSHI_MATERIAL"}}
    feeds = await _create_public_feeds(settings,
        {"max_event_bytes": 4096, "max_markets": 1}, timeout=.5)
    assert len(feeds) == 1 and isinstance(feeds[0], KalshiMarketData)
    assert feeds[0].markets == ["selected"] and feeds[0].max_event_bytes == 4096
    assert_valid_signature(feeds[0].api.websocket_headers(), signing_key)


@pytest.mark.parametrize("defect", ["invalid-material", "missing-path", "oversize-material", "oversize-file", "unsupported-key"])
def test_invalid_kalshi_key_is_bounded_and_errors_do_not_expose_material_or_path(tmp_path, defect):
    marker = "DUMMY_PRIVATE_KEY_NEVER_RENDER_THIS"
    if defect == "invalid-material":
        source = "-----BEGIN PRIVATE KEY-----\n" + marker + "\n-----END PRIVATE KEY-----"
    elif defect == "missing-path":
        source = tmp_path / marker
    elif defect == "oversize-material":
        source = marker + "x" * 4096
    elif defect == "oversize-file":
        source = tmp_path / marker
        source.write_bytes((marker + "x" * 4096).encode("ascii"))
    else:
        unsupported = ec.generate_private_key(ec.SECP256R1())
        source = base64.b64encode(key_bytes(unsupported)).decode("ascii")
        marker = source
    with pytest.raises(ValueError) as caught:
        KalshiPublicAPI("dummy-public-key-id", source, max_event_bytes=4096).websocket_headers()
    error = caught.value
    rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert marker not in str(error) and marker not in rendered
    assert str(tmp_path) not in str(error) and str(tmp_path) not in rendered
    assert error.__cause__ is None and error.__context__ is None


def test_missing_kalshi_key_is_rejected_with_no_source_value_in_error():
    with pytest.raises(ValueError, match="credentials are required"):
        KalshiPublicAPI("dummy-public-key-id", None, max_event_bytes=4096).websocket_headers()
