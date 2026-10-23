"""Thin async SDK wrapper. Exchange normalization lives in polymarket_US.py."""
import asyncio
from datetime import datetime, timezone
import os
from typing import Dict

import dotenv
from polymarket_us import AsyncPolymarketUS
from polymarket_us.errors import NotFoundError

from ...utilities.logger import setup_logger

logger = setup_logger(__name__)
dotenv.load_dotenv()


class PolymarketAPI:
    def __init__(self, key_id: str, secret_key: str):
        self.client = AsyncPolymarketUS(key_id=key_id, secret_key=secret_key)

    async def get_markets(self, active: bool = True, limit: int = 100, offset: int = 0) -> Dict:
        params = {"limit": limit, "active": active, "closed": not active, "offset": offset}
        response = await self.client.markets.list(params)
        return {
            "markets": response["markets"],
            "offset": offset + len(response["markets"]),
        }

    async def get_market(self, market_id: str) -> Dict:
        response = await self.client.markets.retrieve(market_id)
        return response.get("market", response) if isinstance(response, dict) else response

    async def get_all_markets(self, limit: int = None, active: bool = True) -> Dict:
        offset = 0
        all_markets = []
        while True:
            response = await self.get_markets(active=active, limit=1000, offset=offset)
            all_markets.extend(response["markets"])
            if limit and len(all_markets) >= limit:
                all_markets = all_markets[:limit]
                break
            offset += len(response["markets"])
        return {"markets": all_markets}

    def _invert_book(self, yes_book: Dict) -> Dict:
        """A YES ask becomes a NO bid at the complementary price, and vice versa."""
        def invert_level(level):
            price = level["px"]
            return {
                "px": {"value": 1.0 - float(price["value"]), "currency": price["currency"]},
                "qty": level["qty"],
            }

        return {
            **yes_book,
            "bids": [invert_level(ask) for ask in yes_book.get("offers", [])],
            "offers": [invert_level(bid) for bid in yes_book.get("bids", [])],
        }

    async def get_orderbook(self, slug: str, outcome: str = None) -> Dict:
        response = await self.client.markets.book(slug)
        yes_book = response.get("marketData", response)
        if outcome == "YES":
            return yes_book
        if outcome == "NO":
            return self._invert_book(yes_book)
        return {"YES": yes_book, "NO": self._invert_book(yes_book)}

    async def get_bbo(self, slug: str) -> Dict:
        return await self.client.markets.bbo(slug)

    async def create_order(
        self, slug, intent, order_type, price, quantity, tif,
        slippage=None, currency="USD", good_till_date=None,
    ):
        """Serialize the installed SDK's CreateOrderParams contract."""
        params = {
            "marketSlug": slug, "intent": intent, "type": order_type,
            "price": {"value": str(price), "currency": currency},
            "quantity": quantity, "tif": tif, "synchronousExecution": True,
        }
        if good_till_date is not None:
            expiry = datetime.fromtimestamp(good_till_date, timezone.utc)
            params["goodTillTime"] = expiry.isoformat().replace("+00:00", "Z")
        if order_type == "ORDER_TYPE_MARKET":
            if type(slippage) is not int or slippage < 0:
                raise ValueError("Market orders require a nonnegative tick bound")
            params["slippageTolerance"] = {
                "currentPrice": {"value": str(price), "currency": currency},
                "ticks": slippage,
            }
        return await self.client.orders.create(params)

    async def cancel_order(self, order_id: str, slug: str) -> Dict:
        return await self.client.orders.cancel(order_id, {"marketSlug": slug})

    async def cancel_all_orders(self, slug: str = None) -> Dict:
        if slug is None:
            return await self.client.orders.cancel_all()
        return await self.client.orders.cancel_all({"marketSlug": slug})

    async def get_preview(
        self, slug: str, intent: str, order_type: str, price: str,
        quantity: int, currency: str,
    ) -> Dict:
        params = {
            "slug": slug, "intent": intent, "type": order_type,
            "price": {"value": price, "currency": currency}, "quantity": quantity,
        }
        return await self.client.orders.preview(params)

    async def close_position(
        self, slug: str, price: str = None, currency: str = "USD", slippage: float = None,
    ) -> Dict:
        params = {"marketSlug": slug}
        if price is not None and slippage is not None:
            params["slippageTolerance"] = {
                "currentPrice": {"value": price, "currency": currency}, "ticks": slippage,
            }
        return await self.client.orders.close_position(params)

    async def get_orders(self):
        return await self.client.orders.list()

    async def get_order(self, order_id: str):
        """Fetch one order, including its final state when it is no longer open."""
        try:
            return await self.client.orders.retrieve(order_id)
        except NotFoundError:
            return None

    async def get_positions(self, limit=None, cursor=None):
        """Load every page: a partial inventory is unsafe for risk approval."""
        params = {}
        if limit is not None:
            params["limit"] = limit
        if cursor:
            params["cursor"] = cursor
        positions = {}
        seen_cursors = set()
        while True:
            page = await self.client.portfolio.positions(params or None)
            positions.update(page.get("positions", {}))
            next_cursor = page.get("nextCursor")
            if page.get("eof") is True or not next_cursor:
                return {"positions": positions}
            if next_cursor in seen_cursors:
                raise ValueError("Position pagination repeated a cursor")
            seen_cursors.add(next_cursor)
            params["cursor"] = next_cursor

    async def get_activities(
        self, limit: int = None, cursor: str = None, types: list[str] = None,
        marketslug: str = None, sortorder: str = "SORT_ORDER_DESCENDING",
    ):
        params = {
            "limit": limit, "cursor": cursor, "types": types,
            "marketSlug": marketslug, "sortOrder": sortorder,
        }
        return await self.client.portfolio.activities(params)

    async def get_balance(self):
        return await self.client.account.balances()

    async def get_market_by_slug(self, slug: str) -> Dict:
        response = await self.client.markets.retrieve_by_slug(slug)
        return response.get("market", response) if isinstance(response, dict) else response


if __name__ == "__main__":
    async def main():
        client = PolymarketAPI(key_id=os.getenv("KEY_ID"), secret_key=os.getenv("SECRET_KEY"))
        print(await client.get_markets(active=True, limit=1))

    asyncio.run(main())
