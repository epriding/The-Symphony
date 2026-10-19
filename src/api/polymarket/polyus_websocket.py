import asyncio
import inspect
from polymarket_us import PolymarketUS
from websockets.exceptions import ConnectionClosed
from typing import Dict, Optional, List, Callable, Any, Set
import json
from src.utilities.logger import setup_logger


logger = setup_logger(__name__)


class PolyUSWebsocket():

    def __init__(self, key_id: Optional[str], secret_key: Optional[str], handlers: Dict[str, Callable], market_subscriptions: Dict[str, Dict[str, List[str]]] = None):
        #PolymarketUS client information
        self.key_id: Optional[str] = key_id
        self.secret_key: Optional[str] = secret_key
        self.client: PolymarketUS = PolymarketUS(key_id=self.key_id, secret_key=self.secret_key)

        ##websockets
        self._ws: Dict[str, Any] = {'market': None, 'private': None} #once its connection function will add the instances

        ##Handlers
        self.handlers: Dict[str, Callable] = handlers

        ##events
        self.MARKET_EVENTS = {'market_data', 'market_data_lite', 'trade'}
        self.PRIVATE_EVENTS = {'order_snapshot', 'order_update', 'position_snapshot', 'position_update',
                                'account_balance_snapshot', 'account_balance_update', 'rfq_event'}

        ##Websocket status variables
        self._connected: Dict[str, bool] = {'market': False, 'private': False}
        self._disconnected: Dict[str, asyncio.Event] = {'market': asyncio.Event(), 'private': asyncio.Event()}
        self._handler_tasks: Dict[str, Set[asyncio.Task]] = {'market': set(), 'private': set()}

        ##reconnection variables
        self._attempts: Dict[str, int] = {'market': 0, 'private': 0}
        self._max_reconnect_attempts: int = 10
        self._reconnect_delay: int = 5

        ##Track inputs and outputs
        #subscriptions dictionary structure:
        self.market_subscriptions: Dict[str, Set[str]] = {
            slug: set(sub_types) for slug, sub_types in (market_subscriptions or {}).items()
        }
        self.private_subscriptions: Dict[str, str] = {
            'orders': 'SUBSCRIPTION_TYPE_ORDER',
            'order_snapshot': 'SUBSCRIPTION_TYPE_ORDER_SNAPSHOT',
            'positions': 'SUBSCRIPTION_TYPE_POSITION',
            'balance': 'SUBSCRIPTION_TYPE_ACCOUNT_BALANCE',
            'rfq': 'SUBSCRIPTION_TYPE_RFQ',
        }

        ##Misc
        #Rid map
        self.RID_MAP = {
            'SUBSCRIPTION_TYPE_MARKET_DATA': 'market_data',
            'SUBSCRIPTION_TYPE_MARKET_DATA_LITE': 'market_data_lite',
            'SUBSCRIPTION_TYPE_TRADE': 'trade'
        }

    def _on_close(self, ws_type: str, close_data: dict) -> None:
        """On websocket closure event"""
        self._connected[ws_type] = False
        self._ws[ws_type] = None

        close_data = close_data or {}
        code = close_data.get('close', close_data.get('code', 1006))
        reason = close_data.get('reason', 'Unknown Drop')

        if code == 1000:
            logger.info(f'Websocket Closure: Websocket properly closed, Code: {code} Reason: {reason}')
            return

        elif code == 1001:
            logger.error(f'Websocket Closure: Websocket server shut down, Enabling reconnect attempt, Code: {code} Reason: {reason}')
            self._disconnected[ws_type].set()
            return

        else:
            logger.info(f'Websocket Closure: Enabling reconnect attempt, Code: {code} Reason: {reason}')
            self._disconnected[ws_type].set()
            return


    def _on_error(self, ws_type: str, error_data: dict) -> None:
        logger.error(f"Websocket Error: Error info: {error_data}")

    def _register_handlers(self, ws_type: str, ws) -> None:
        allowed = self.MARKET_EVENTS if ws_type == "market" else self.PRIVATE_EVENTS
        for event, handler in self.handlers.items():
            if event in allowed:
                ws.on(event, self._async_handler_callback(ws_type, event, handler))

        ws.on('close', lambda *args: self._on_close(ws_type=ws_type, close_data=args[0] if args else {}))
        ws.on('error', lambda data: self._on_error(ws_type=ws_type, error_data=data))

    def _async_handler_callback(self, ws_type: str, event: str, handler: Callable) -> Callable:
        """Bridge the SDK's synchronous emitter to async event handlers."""
        def callback(*args):
            result = handler(*args)
            if not inspect.isawaitable(result):
                return
            task = asyncio.create_task(result, name=f"{ws_type}-ws:{event}")
            tasks = self._handler_tasks[ws_type]
            tasks.add(task)
            task.add_done_callback(lambda done: self._handler_done(ws_type, event, done))
        return callback

    def _handler_done(self, ws_type: str, event: str, task: asyncio.Task) -> None:
        self._handler_tasks[ws_type].discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "%s websocket %s handler failed: %s",
                ws_type.upper(), event, error,
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _load_subscriptions(self, ws_type: str, ws) -> None:
        if ws_type == 'market':
            if not self.market_subscriptions:
                return

            for slug, sub_types in self.market_subscriptions.items():
                for sub_type in sub_types:
                    rid = self._get_rid(sub_type=sub_type, slug=slug)
                    await ws.subscribe(rid, sub_type, [slug])

        else:
            for rid, sub_type in self.private_subscriptions.items():
                await ws.subscribe(rid, sub_type)

    def _get_rid(self, sub_type: str, slug: str) -> str:
        """creates the rid for subscriptions"""
        suffix = self.RID_MAP[sub_type]
        return f'{slug}:{suffix}'


    async def add_market_subscription(self, sub_types: Set[str], slug: str) -> None:
        """Adds a market subscription to the websocket subscription list"""
        for sub_type in sub_types:
            rid = self._get_rid(sub_type=sub_type, slug=slug)
            if self._ws['market'] is not None:
                await self._ws['market'].subscribe(rid, sub_type, [slug])

        check_status = self.market_subscriptions.get(slug, None)

        if check_status is None:
            self.market_subscriptions[slug] = sub_types

        else:
            combined_types = self.market_subscriptions[slug].union(sub_types)
            self.market_subscriptions[slug] = combined_types


    async def remove_market_subscription(self, slug: str) -> None:
        """Removes a market subscription"""
        sub_types = self.market_subscriptions.pop(slug, None)

        if sub_types is None:
            logger.error('Error: failed to fetch market subscription data.')
            return

        for sub_type in sub_types:
            rid = self._get_rid(sub_type=sub_type, slug=slug)
            if self._ws['market'] is not None:
                await self._ws['market'].unsubscribe(rid)


    async def _close_websocket(self, ws_type: str) -> None:
        """Closes websocket"""
        tasks = list(self._handler_tasks[ws_type])
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._ws[ws_type] is not None:
            await self._ws[ws_type].close()
            self._ws[ws_type] = None
        else:
            logger.error('Websocket Closure Attempt Error: failed to close websocket already closed or does not exist')
            return

    async def _connect(self, ws_type: str) -> None:
        """Connects the websocket to Polymarket US"""
        ws = self.client.ws.markets() if ws_type == 'market' else self.client.ws.private()
        self._register_handlers(ws_type=ws_type, ws=ws)

        await ws.connect()

        #load past subscriptions
        await self._load_subscriptions(ws_type=ws_type, ws=ws)

        #adds websockets, and reset variables to the base amount
        self._ws[ws_type] = ws
        self._connected[ws_type] = True
        self._attempts[ws_type] = 0
        self._disconnected[ws_type].clear()

    async def _start_stream(self, ws_type: str) -> None:
        """Streams market or private data depending on websocket type: ws_type"""
        try:
            if self._ws[ws_type] is None:
                await self._connect(ws_type=ws_type)

            while True:
                await self._disconnected[ws_type].wait()

                while True:
                    if self._attempts[ws_type] >= self._max_reconnect_attempts:
                        logger.error(f"{ws_type.upper()} WS: max reconnection attempts reached.")
                        raise RuntimeError(f"{ws_type.upper()} WS: max reconnection attempts reached.")

                    delay = min(self._reconnect_delay * 2 ** self._attempts[ws_type], 60)
                    self._attempts[ws_type] += 1
                    await asyncio.sleep(delay)

                    try:
                        await self._connect(ws_type=ws_type)
                        break

                    except (ConnectionClosed, asyncio.TimeoutError, OSError) as e:
                        logger.error(f"{ws_type.upper()} WS: reconnect failed: {e}")

        except asyncio.CancelledError:
            await self._close_websocket(ws_type=ws_type)
            raise


    async def start(self) -> None:
        await self._connect('market')
        await self._connect('private')

    async def run(self) -> None:
        await asyncio.gather(self._start_stream('market'), self._start_stream('private'))
