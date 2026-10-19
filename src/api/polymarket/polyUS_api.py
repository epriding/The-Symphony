from polymarket_us import AsyncPolymarketUS
from polymarket_us.errors import NotFoundError
from typing import Dict
from ...utilities.logger import setup_logger
import dotenv
import os
import asyncio


logger = setup_logger(__name__)


dotenv.load_dotenv()

class PolymarketAPI():
    def __init__(self, key_id: str, secret_key: str):
        self.client = AsyncPolymarketUS(key_id=key_id, secret_key=secret_key)

    async def get_markets(self, active: bool = True, limit: int = 100, offset: int = 0) -> Dict:
        params = {
            'limit': limit,
            'active': active,
            'closed': False if active else True,
            'offset': offset
        }

        response =  await self.client.markets.list(params)


        values = {
            'markets': response['markets'],
            'offset': offset + len(response['markets']),
        }    

        return values

    async def get_market(self, market_id: str) -> Dict:
        response = await self.client.markets.retrieve(market_id)
        return response

    async def get_all_markets(self, limit: int = None, active: bool = True) -> Dict:
        offset = 0
        all_markets = []

        while True:
            response = await self.get_markets(active=active, limit=1000, offset=offset)
            all_markets.extend(response['markets'])

            if limit and len(all_markets) >= limit:
                all_markets = all_markets[:limit]
                break
            offset += len(response['markets'])

        return {'markets': all_markets}

    def _invert_book(self, yes_book: Dict) -> Dict:
        """Derive the NO-side book from a YES-denominated book."""
        return {
            **yes_book,
            "bids": [
                {"px": {"value": 1.0 - ask["px"]["value"], "currency": ask["px"]["currency"]}, "qty": ask["qty"]}
                for ask in yes_book.get("offers", [])
            ],
            "offers": [
                {"px": {"value": 1.0 - bid["px"]["value"], "currency": bid["px"]["currency"]}, "qty": bid["qty"]}
                for bid in yes_book.get("bids", [])
            ],
        }

    async def get_orderbook(self, slug: str, outcome: str = None) -> Dict:
        response = await self.client.markets.book(slug)
        yes_book = response["marketData"]

        if outcome == "YES":
            return yes_book
        elif outcome == "NO":
            return self._invert_book(yes_book)
        else:
            return {"YES": yes_book, "NO": self._invert_book(yes_book)}

    async def get_bbo(self, slug: str) -> Dict:
        response = await self.client.markets.bbo(slug)
        return response
   
    async def create_order(self, slug: str, intent: str, order_type: str, price: str, quantity: int, tif: str, slippage: float = None, currency: str = "USD") -> Dict:
        """This Creates an order, can be buy, sell, or limits, etc. changed through the intent"""

        if order_type == "ORDER_TYPE_LIMIT":
            params = {
                'slug': slug,
                'intent': intent,
                'type': order_type,
                'price': price,
                'quantity': quantity,
                'tif': tif
            }
            response = await self.client.orders.create(params)
            return response 

        elif slippage is not None:
            params = {
                'slug': slug,
                'intent': intent,
                'type': order_type,
                'quantity': quantity,
                'tif': tif,
                'slippageTolerance': {
                    'currentPrice': {'value': price, 'currency': currency},
                    'ticks': slippage
                }
            }

            response = await self.client.orders.create(params)
            return response
        else:
            logger.warning("No slippage tolerance specified for order creation.")
            return {'error': 'No slippage tolerance specified for order creation.'}

    async def cancel_order(self, order_id: str, slug: str) -> Dict:
        response = await self.client.orders.cancel(order_id, {"marketSlug": slug})
        return response

    async def cancel_all_orders(self, slug: str = None) -> Dict:
            if slug is None:
                #cancels all orders
                response = await self.client.orders.cancel_all()
                return response

            #cancel all order for a specific market
            response = await self.client.orders.cancel_all({"marketSlug": slug})
            return response

    async def get_preview(self, slug: str, intent: str, order_type: str, price: str, quantity: int, currency: str) -> Dict:
        """Primarily going to be used for paper trading"""
        params = {
                'slug': slug,
                'intent': intent,
                'type': order_type,
                'price': {'value': price, 'currency': currency},
                'quantity': quantity,
            }
        response = await self.client.orders.preview(params)
        return response

    async def close_position(self, slug: str, price: str = None, currency: str = "USD", slippage: float = None) -> Dict:
        """Closes all positions for a given market, at a given price if stated"""
        if price is not None and slippage is not None:
            params = {
                'marketSlug': slug,
                'slippageTolerance': {
                    'currentPrice': {'value': price, 'currency': currency},
                    'ticks': slippage
                }
            }
            response = await self.client.orders.close_position(params)
            return response

        else:
            params = {
                'marketSlug': slug
            }
            response = await self.client.orders.close_position(params)
            return response

    async def get_orders(self):
        """Gets all open orders"""
        response = await self.client.orders.list()
        return response

    async def get_order(self, order_id: str):
        """Fetch one order, including its final state when it is no longer open."""
        try:
            return await self.client.orders.retrieve(order_id)
        except NotFoundError:
            return None

    async def get_positions(self, limit: int = None, cursor: str = None):
        if limit is not None and cursor is not None:
            positions = await self.client.portfolio.positions(limit=limit, cursor=cursor)
            return positions

        elif cursor is not None:
            positions = await self.client.portfolio.positions(cursor=cursor)
            return positions

        elif limit is not None:
            positions = await self.client.portfolio.positions(limit=limit)
            return positions

        else:
            positions = await self.client.portfolio.positions()
            return positions

    async def get_activities(self, limit: int = None, cursor: str = None, types: list[str] = None, marketslug: str = None, sortorder: str = 'SORT_ORDER_DESCENDING'):
        raw_params = {
            'limit': limit,
            'cursor': cursor,
            'types' : types,
            'marketSlug': marketslug,
            'sortOrder': sortorder
        }

        params = {k: v for k, v in raw_params.items()}

        activities = await self.client.portfolio.activities(params)
        return activities

    async def get_balance(self):
        balances = await self.client.account.balances()
        return balances

    async def get_market_by_slug(self, slug: str) -> Dict:
        response = await self.client.markets.retrieve_by_slug(slug)
        return response

if __name__ == "__main__":
    async def main():
        client = PolymarketAPI(key_id=os.getenv("KEY_ID"), secret_key=os.getenv("SECRET_KEY"))
        response = await client.get_markets(active=True, limit=1)

        print(response)

    asyncio.run(main())
