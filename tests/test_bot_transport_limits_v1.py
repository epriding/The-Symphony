"""Public transport and accumulating normalizer bounds, entirely offline."""
import importlib
import asyncio
from unittest.mock import AsyncMock

import pytest


def transport():
    return importlib.import_module("src.api.interfaces.bounded_public")


class ChunkedResponse:
    def __init__(self, chunks, headers=None):
        self.chunks = chunks
        self.headers = headers or {}
        self.consumed = 0
        self.requested_chunk_size = None

    async def aiter_bytes(self, chunk_size):
        self.requested_chunk_size = chunk_size
        for chunk in self.chunks:
            self.consumed += 1
            yield chunk

    def json(self):
        raise AssertionError("Unbounded response.json() was used")

    async def read(self):
        raise AssertionError("Unbounded response.read() was used")


@pytest.mark.asyncio
async def test_unknown_http_length_stops_stream_before_decode_and_before_remaining_chunks(monkeypatch):
    module = transport()
    response = ChunkedResponse([b"{" + b"x" * 23, b"x" * 24, b"never-consume"])

    def forbid_decode(data):
        raise AssertionError("Oversize bytes reached JSON decoding")

    monkeypatch.setattr(module, "_finite_json", forbid_decode)
    with pytest.raises(module.PublicCapacityError):
        await module.read_json_limited(response, 32)
    assert response.consumed == 2
    assert response.requested_chunk_size <= 33


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{"Content-Length": "4096"},
                                      {"Content-Encoding": "gzip"}])
async def test_http_oversize_header_or_compression_rejected_before_body(headers):
    module = transport()
    response = ChunkedResponse([b"never-consume"], headers=headers)
    with pytest.raises(module.PublicCapacityError):
        await module.read_json_limited(response, 32)
    assert response.consumed == 0


@pytest.mark.asyncio
async def test_both_installed_transport_response_shapes_use_finite_chunks():
    module = transport()
    first = ChunkedResponse([b'{"price":', b'"0.42"}'])
    assert await module.read_json_limited(first, 32) == {"price": "0.42"}

    class AiohttpResponse:
        headers = {}

        class Content:
            async def iter_chunked(self, size):
                assert size <= 33
                yield b'{"quantity":3}'

        content = Content()

        async def json(self):
            raise AssertionError("aiohttp JSON was decoded before bounded streaming")

    assert await module.read_json_limited(AiohttpResponse(), 32) == {"quantity": 3}


@pytest.mark.asyncio
async def test_bounded_sdk_http_refuses_private_endpoints_before_transport(monkeypatch):
    module = transport()
    client = module.BoundedPublicHTTP(max_event_bytes=128)
    private = AsyncMock(side_effect=AssertionError("Private route reached HTTP transport"))
    try:
        monkeypatch.setattr(client._http, "stream", private)
        for method, path, authenticated in [("POST", "/v1/orders", False),
                                            ("GET", "/v1/account/positions", False),
                                            ("GET", "/v1/markets", True)]:
            with pytest.raises(ValueError):
                await client._request(method, path, authenticated=authenticated)
        private.assert_not_called()
    finally:
        await client.close()


def receipt(index):
    return dict(received_at=1_800_000_000 + index, received_monotonic_ns=index,
                ingest_index=index)


def test_ignored_global_lifecycle_advances_sequence_without_growing_market_state():
    from src.api.interfaces.public_market_data import KalshiNormalizer
    normalizer = KalshiNormalizer(allowed_markets={"selected"}, max_book_levels=4,
                                  max_sequences=2)
    for index in range(1, 101):
        assert not normalizer.normalize({"type": "market_lifecycle_v2", "sid": 7,
            "seq": index, "msg": {"market_ticker": f"unrelated-{index}",
                                      "event_type": "activated"}}, receipt(index))
    assert not normalizer.books and not normalizer.states and not normalizer.event_members
    assert normalizer.sequences == {7: 100}
    selected = {"type": "market_lifecycle_v2", "sid": 7, "seq": 101,
                "msg": {"market_ticker": "selected", "event_type": "activated"}}
    assert normalizer.normalize(selected, receipt(101))[0].event.market_id == "selected"
    selected["seq"] = 103
    with pytest.raises(ValueError, match="sequence"):
        normalizer.normalize(selected, receipt(103))


def test_many_small_deltas_cannot_grow_a_book_beyond_its_configured_level_cap():
    from src.api.interfaces.public_market_data import KalshiNormalizer
    module = transport()
    normalizer = KalshiNormalizer(allowed_markets={"selected"}, max_book_levels=3)
    normalizer.normalize({"type": "orderbook_snapshot", "sid": 1, "seq": 1,
        "msg": {"market_ticker": "selected", "yes_dollars_fp": [[".40", "1"]],
                "no_dollars_fp": [[".50", "1"]]}}, receipt(1))
    normalizer.normalize({"type": "orderbook_delta", "sid": 1, "seq": 2,
        "msg": {"market_ticker": "selected", "side": "yes",
                "price_dollars": ".41", "delta_fp": "1"}}, receipt(2))
    with pytest.raises(module.PublicCapacityError):
        normalizer.normalize({"type": "orderbook_delta", "sid": 1, "seq": 3,
            "msg": {"market_ticker": "selected", "side": "yes",
                    "price_dollars": ".42", "delta_fp": "1"}}, receipt(3))
    assert sum(len(side) for side in normalizer.books["selected"].values()) == 3


@pytest.mark.asyncio
async def test_pinned_sdk_websocket_receives_native_frame_and_queue_caps_before_connect(monkeypatch):
    module = transport()

    class EmptySocket:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    connect = AsyncMock(return_value=EmptySocket())
    monkeypatch.setattr(module.websockets, "connect", connect)
    monkeypatch.setattr(module, "create_auth_headers", lambda *args: {"fixture": "public"})
    client = module.BoundedMarketsWebSocket(
        max_event_bytes=128, max_queue=3, key_id="dummy", secret_key="dummy")
    await client.connect()
    await asyncio.wait_for(client._message_task, 1)
    assert connect.await_count == 1
    parameters = connect.call_args.kwargs
    assert parameters["max_size"] == 128 and parameters["max_queue"] == 3
    assert parameters["compression"] is None


def test_websocket_capacity_rejects_an_oversize_frame_before_sdk_parser(monkeypatch):
    module = transport()
    client = module.BoundedMarketsWebSocket(
        max_event_bytes=32, max_queue=2, key_id="dummy", secret_key="dummy")

    def forbid_decode(data):
        raise AssertionError("Oversize websocket frame reached JSON parser")

    monkeypatch.setattr(module, "_finite_json", forbid_decode)
    with pytest.raises(module.PublicCapacityError):
        client._handle_message("{" + "x" * 64)


@pytest.mark.asyncio
async def test_us_http_owner_closes_once_before_stream_even_if_first_waiter_is_cancelled(monkeypatch):
    from src.api.interfaces.polymarket_us_public import PolymarketUSMarketData

    entered, release = asyncio.Event(), asyncio.Event()
    instances = []

    class CountingAPI:
        def __init__(self, *args, **kwargs):
            self.client, self.calls, self.closed = self, 0, False
            instances.append(self)

        async def close(self):
            self.calls += 1
            entered.set()
            await release.wait()
            self.closed = True

    monkeypatch.setattr(transport(), "BoundedPolymarketPublicAPI", CountingAPI)
    feed = PolymarketUSMarketData(["m"], "dummy-key", "dummy-secret", max_event_bytes=128)
    first = asyncio.create_task(feed.aclose())
    waiters = []
    try:
        await asyncio.wait_for(entered.wait(), 1)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        waiters = [asyncio.create_task(feed.aclose()) for _ in range(2)]
        release.set()
        await asyncio.wait_for(asyncio.gather(*waiters), 1)
        await feed.aclose()
        assert len(instances) == 1
        assert instances[0].calls == 1 and instances[0].closed
    finally:
        release.set()
        await asyncio.gather(first, *waiters, return_exceptions=True)
        await feed.aclose()
