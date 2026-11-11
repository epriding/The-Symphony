"""Copied, allowlisted observations; no account references cross this boundary."""
from collections import deque
from decimal import Decimal
import json
import time
import unicodedata

from ..utilities.logger import redact


def safe_text(value, limit=2048):
    text = redact(str(value))
    text = ''.join(c for c in text if unicodedata.category(c) not in ('Cc', 'Cs'))
    return text.encode('utf-8')[:limit].decode('utf-8', errors='ignore')


def amount(value):
    if value is None:
        return None
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError('Nonfinite observed amount')
    return str(number)


class Observations:
    def __init__(self, config):
        self.config = config
        self.fills = deque(maxlen=256)
        self.fill_sequence = 0
        self.market_time = None
        self.snapshot_at = None
        self.last_refresh = -float('inf')
        self.orders = []
        self.positions = []
        self.position_count = 0
        self.venues = {}
        self.activities = {}
        self.errors = {}
        self.strategies = self._strategies({})

    def _strategies(self, readiness):
        summaries = {}
        for entry in self.config['strategies']:
            name = entry['name']
            status = readiness.get(name, {})
            route = self.config['strategy_portfolios'].get(name)
            summaries[name] = dict(id=name, name=name, factory=entry['factory'],
                enabled=entry['enabled'], portfolio_id=route,
                execution_mode='PAPER' if route == 'PAPER' else None,
                ready=bool(status.get('ready',False)),
                reason=safe_text(status.get('reason', 'awaiting startup' if entry['enabled'] else 'disabled')),
                operator_state='running', active_order_count=0, position_count=0,
                last_activity=self.activities.get(name), last_error=self.errors.get(name))
        return summaries

    def observe(self, runner, kind, payload):
        if kind == 'control':
            name = payload.get('strategy')
            if name in self.strategies:
                self.activities[name] = dict(action=safe_text(payload['action']), timestamp=runner.clock())
                if payload['action'] == 'disabled':
                    self.errors[name] = safe_text(payload.get('reason',''))
        if kind == 'execution' and payload['event'] == 'execution':
            order = runner._session.engine.orders[payload['order_id']]
            approved = order.approved
            self.fill_sequence += 1
            self.fills.append(dict(id=f'fill-{self.fill_sequence:012d}',
                execution_id=payload['execution_id'], order_id=payload['order_id'],
                sequence=self.fill_sequence, strategy=approved.strategy,
                attribution='strategy' if approved.strategy in self.strategies else 'system',
                portfolio_id=approved.portfolio_id, exchange=approved.exchange,
                market_id=approved.market_id, outcome=approved.outcome,
                side=approved.side.value, quantity=amount(payload['quantity']),
                price=amount(payload['price']), timestamp=payload['timestamp'],status='FILLED'))
        if kind in ('tick','final') and (kind=='final' or time.monotonic()-self.last_refresh >= .1):
            self.refresh(runner)
            return True
        return False

    def refresh(self, runner):
        status = runner.status
        summaries = self._strategies(status.get('strategies',{}))
        orders, positions = [], []
        session = runner._session
        if session:
            for order in session.engine.orders.values():
                if order.phase == 'terminal':
                    continue
                record = order.record
                row = dict(id=record.order_id, order_id=record.order_id, strategy=record.strategy,
                    attribution='strategy' if record.strategy in summaries else 'system',
                    portfolio_id=record.portfolio_id, exchange=record.exchange,
                    market_id=record.market_id, outcome=record.outcome, side=record.side.value,
                    quantity=amount(record.size), filled_quantity=amount(record.filled_quantity),
                    remaining_quantity=amount(record.remaining_quantity), price=amount(record.price),
                    status=record.status.value, timestamp=order.approved.intent.created_at)
                orders.append(row)
                if record.strategy in summaries:
                    summaries[record.strategy]['active_order_count'] += 1
            self.position_count = sum(len(p.positions) for p in session.state.portfolios.values() if p.paper)
            for portfolio in session.state.portfolios.values():
                if not portfolio.paper:
                    continue
                # Netted aggregate positions cannot establish strategy ownership.
                for name, holdings in portfolio.strategy_positions.items():
                    for position in holdings.values():
                        identity=json.dumps([portfolio.portfolio_id,name,position.exchange,
                            position.market_id,position.outcome],separators=(',',':'))
                        row=dict(id=identity,strategy=name,
                            attribution='strategy' if name in summaries else 'system',
                            portfolio_id=portfolio.portfolio_id,exchange=position.exchange,
                            market_id=position.market_id,outcome=position.outcome,side=position.side.value,
                            quantity=amount(position.size),price=amount(position.entry_price),
                            status='OPEN',timestamp=None)
                        positions.append(row)
                        if name in summaries:
                            summaries[name]['position_count'] += 1
        for summary in summaries.values():
            summary['operator_state']=runner.operator_state
        self.strategies,self.orders,self.positions=summaries,orders,positions
        self.venues=status.get('venues',{})
        self.market_time=runner.clock()
        self.snapshot_at=time.time()
        self.last_refresh=time.monotonic()

    @staticmethod
    def page(rows, *, scope='active'):
        rows=sorted(rows,key=lambda row:row['id'])
        return dict(items=rows,next_cursor=None,has_more=False,scope=scope)

    def read(self,name,collection=None):
        if collection is None:
            return self.strategies[name]
        rows={'orders':self.orders,'positions':self.positions,'fills':self.fills,'events':[]}[collection]
        return self.page([row for row in rows if row['strategy']==name],
            scope='recent' if collection in ('fills','events') else 'active')
