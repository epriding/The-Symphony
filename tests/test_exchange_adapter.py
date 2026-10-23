import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
import httpx
import pytest
from polymarket_us.errors import BadRequestError, InternalServerError

from src.api.interfaces.polymarket_US import PolyUsClient, PolyUsWebsocketClient, _account_balance
from src.api.polymarket.polyUS_api import PolymarketAPI
from src.utilities.classes import AccountConnectionEvent, OrderStatus, OrderType, TimeInForce


@pytest.fixture
def client():
    result = PolyUsClient(None, None, {"slug": "m"})
    result.polymarket_api = SimpleNamespace(create_order=AsyncMock(), get_order=AsyncMock(), get_balance=AsyncMock())
    return result


def raw_order(**changes):
    return {
        "id": "order-1", "marketSlug": "slug", "intent": "ORDER_INTENT_BUY_LONG",
        "type": "ORDER_TYPE_LIMIT", "quantity": 600, "cumQuantity": 40,
        "leavesQuantity": 560, "price": {"value": "0.5", "currency": "USD"},
        "avgPx": {"value": "0.49", "currency": "USD"},
        "state": "ORDER_STATE_PARTIALLY_FILLED", "tif": "TIME_IN_FORCE_FILL_OR_KILL",
        **changes,
    }


def test_account_balance_does_not_invent_equity():
    raw = {"currency": "USD", "currentBalance": 800, "buyingPower": 700,
           "balanceReservation": 100}
    balance = _account_balance(raw)
    assert balance.cash is None
    assert balance.current_balance == 800
    assert balance.buying_power == 700
    assert balance.reserved_cash == 100
    mapped = _account_balance(raw, lambda data: data["currentBalance"])
    assert mapped.cash == 800


@pytest.mark.asyncio
async def test_adapter_reads_execution_list_and_maps_tif(client, state, intent):
    approved = state.risk_managers["PAPER"].evaluate(intent, state._snapshot()).orders[0]
    client.polymarket_api.create_order.return_value = {"id": "order-1", "executions": [{"order": raw_order()}]}
    result = await client.place_order(approved)
    assert result.filled_quantity == 40
    assert result.average_fill_price == .49
    kwargs = client.polymarket_api.create_order.call_args.kwargs
    assert kwargs["tif"] == "TIME_IN_FORCE_FILL_OR_KILL"
    assert kwargs["quantity"] == 600
    assert kwargs["slug"] == "slug"


@pytest.mark.asyncio
async def test_execution_tie_uses_final_terminal_state(client, state, intent):
    approved = state.risk_managers["PAPER"].evaluate(intent, state._snapshot()).orders[0]
    client.polymarket_api.create_order.return_value = {"executions": [
        {"order": raw_order()}, {"order": raw_order(state="ORDER_STATE_CANCELED")},
    ]}
    assert (await client.place_order(approved)).status == OrderStatus.CANCELED


@pytest.mark.asyncio
async def test_id_only_response_fetches_order(client, state, intent):
    approved = state.risk_managers["PAPER"].evaluate(intent, state._snapshot()).orders[0]
    client.polymarket_api.create_order.return_value = {"id": "order-1"}
    client.polymarket_api.get_order.return_value = {"order": raw_order()}
    result = await client.place_order(approved)
    assert result.order_id == "order-1"
    client.polymarket_api.get_order.assert_awaited_once_with("order-1")


@pytest.mark.asyncio
async def test_400_is_definitive_but_500_remains_ambiguous(client, state, intent):
    approved = state.risk_managers["PAPER"].evaluate(intent, state._snapshot()).orders[0]
    request = httpx.Request("POST", "https://example.test")
    client.polymarket_api.create_order.side_effect = BadRequestError("invalid", response=httpx.Response(400, request=request), body={})
    assert (await client.place_order(approved)).status == OrderStatus.REJECTED
    client.polymarket_api.create_order.side_effect = InternalServerError("unknown", response=httpx.Response(500, request=request), body={})
    with pytest.raises(InternalServerError):
        await client.place_order(approved)


@pytest.mark.asyncio
async def test_raw_request_matches_installed_sdk_contract(clock):
    api = object.__new__(PolymarketAPI)
    create = AsyncMock(return_value={"id": "test"})
    api.client = SimpleNamespace(orders=SimpleNamespace(create=create))
    await api.create_order("slug", "ORDER_INTENT_BUY_LONG", "ORDER_TYPE_MARKET", .5,
                           10, "TIME_IN_FORCE_GOOD_TILL_DATE", slippage=2,
                           good_till_date=clock() + 100)
    params = create.call_args.args[0]
    assert params["marketSlug"] == "slug" and "slug" not in params
    assert params["price"] == {"value": "0.5", "currency": "USD"}
    assert params["goodTillTime"].endswith("Z")
    assert params["slippageTolerance"]["ticks"] == 2
    assert params["synchronousExecution"] is True


@pytest.mark.asyncio
async def test_balance_response_uses_currency_list(client):
    client.cash_resolver = lambda raw: raw["currentBalance"]
    client.polymarket_api.get_balance.return_value = {"balances": [
        {"currency": "USD", "currentBalance": 900, "buyingPower": 800},
    ]}
    balances = await client.get_balance()
    assert balances["USD"].cash == 900
    assert balances["USD"].buying_power == 800


@pytest.mark.asyncio
async def test_position_pagination_is_complete_and_uses_params_dict():
    api = object.__new__(PolymarketAPI)
    pages = AsyncMock(side_effect=[
        {"positions": {"a": {}}, "nextCursor": "second"},
        {"positions": {"b": {}}, "eof": True},
    ])
    api.client = SimpleNamespace(portfolio=SimpleNamespace(positions=pages))
    result = await api.get_positions(limit=10)
    assert set(result["positions"]) == {"a", "b"}
    assert pages.await_count == 2
    assert pages.call_args.args[0] == {"limit": 10, "cursor": "second"}


@pytest.mark.asyncio
async def test_position_amounts_are_usd_not_implicitly_microdollars(client):
    positions = await client._to_position({"positions": {"slug": {
        "netPositionDecimal": "10", "qtyBoughtDecimal": "10",
        "cost": {"value": "4.5", "currency": "USD"},
    }}})
    assert positions[0].entry_price == .45


@pytest.mark.asyncio
async def test_websocket_balances_preserve_cash_and_buying_power():
    ws = PolyUsWebsocketClient(None, None, {"slug": "m"})
    ws.cash_resolver = lambda raw: raw["currentBalance"]
    await ws._handle_account_balance_snapshot({"accountBalancesSnapshot": {"balances": [
        {"currency": "USD", "currentBalance": 900, "buyingPower": 800},
    ]}})
    event = await ws.queue.get()
    assert event.balances["USD"].cash == 900
    assert event.balances["USD"].buying_power == 800
    await ws._handle_account_balance_snapshot({"accountBalancesSnapshot": {"balances": [
        {"currency": "USD", "currentBalance": 900},
    ]}})
    assert ws.queue.empty()  # incomplete snapshots cannot erase valid account data
    await ws._handle_connection_state("private", True)
    assert isinstance(await ws.queue.get(), AccountConnectionEvent)


@pytest.mark.asyncio
async def test_lookup_delay_cannot_send_expired_approval(client, state, intent, clock, monkeypatch):
    from src.api.interfaces import polymarket_US
    from src.utilities.classes import OrderNotSubmittedError
    approved = state.risk_managers["PAPER"].evaluate(intent, state._snapshot()).orders[0]
    async def delayed_slug(market_id):
        clock.now += 3
        return "slug"
    client._get_slug = delayed_slug
    monkeypatch.setattr(polymarket_US.time, "time", clock)
    with pytest.raises(OrderNotSubmittedError):
        await client.place_order(approved)
    client.polymarket_api.create_order.assert_not_awaited()


def test_observed_account_freshness_is_not_last_transaction_time():
    import time
    balance = _account_balance({"currency": "USD", "buyingPower": 10,
        "currentBalance": 10, "lastUpdated": "2000-01-01T00:00:00Z"})
    assert abs(balance.timestamp - time.time()) < 1
    assert balance.source_timestamp == 946684800
