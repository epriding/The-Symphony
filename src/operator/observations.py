"""Copied, allowlisted observations; no account references cross this boundary."""
from collections import deque
from decimal import Decimal
import base64
import binascii
import hashlib
import hmac
import json
import secrets
import time
import unicodedata

from ..utilities.logger import redact
from ..paper_trading.runtime_config import retained_bytes
from .protocol import ProtocolError, bounded_text

MODEL_LIMIT = 4 * 1024 * 1024


def validate_projection(config):
    identifiers = [(config['execution'].get('session_id','paper'),128)]
    for entry in config['strategies']:
        identifiers.extend(((entry['name'],256),(entry['factory'],256)))
    for venue,feed in config['feeds'].items():
        identifiers.append((venue,256))
        identifiers.extend((market,256) for market in feed['markets'])
    for value,limit in identifiers:
        try:
            bounded_text(value,limit)
            if any(unicodedata.category(c) in ('Cc','Cs') for c in value):
                raise ValueError('Control character')
        except (ValueError,UnicodeError):
            raise ValueError('Control identifier exceeds projection limits or contains control characters') from None
    if any(route != 'PAPER' for route in config['strategy_portfolios'].values()):
        raise ValueError('Control projections support PAPER routes only')
    limits=config['runtime']['limits']
    estimate=(2*retained_bytes(config)
        + limits['max_active_orders']*4096
        + limits['max_positions']*limits['max_strategies']*2048
        + limits['max_strategies']*4096 + limits['max_markets']*2048
        + 256*3072*2 + 128*6144)
    if estimate>MODEL_LIMIT:
        raise ValueError('Configured control projection exceeds service memory budget')


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
    if len(str(number))>128:
        raise ValueError('Observed amount exceeds projection limit')
    return str(number)


class Observations:
    def __init__(self, config):
        self.config = config
        self.fills = deque(maxlen=256)
        self.fill_sequence = 0
        self.events=deque(maxlen=256)
        self.event_sequence=0
        self.findings=deque(maxlen=128)
        self.finding_sequence=0
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
        self._cursor_key=secrets.token_bytes(32)

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
        if kind in ('control','execution'):
            self.event_sequence+=1
            action=payload.get('action',payload.get('event','activity'))
            name=payload.get('strategy')
            order=runner._session.engine.orders.get(payload.get('order_id'))
            if name is None and order is not None:
                name=order.approved.strategy
            name=name if name in self.strategies else None
            self.events.append(dict(id=f'event-{self.event_sequence:012d}',
                sequence=self.event_sequence,action=safe_text(action,256),strategy=name,
                attribution='strategy' if name else 'system',
                severity='error' if action=='disabled' or payload.get('failed') else 'info',
                message=safe_text(payload.get('reason') or action),
                timestamp=payload['timestamp'] if 'timestamp' in payload else runner.clock()))
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
        prospective=dict(config=self.config,strategies=summaries,orders=orders,
            positions=positions,fills=self.fills,events=self.events,findings=self.findings,
            activities=self.activities,errors=self.errors)
        if retained_bytes(prospective)>MODEL_LIMIT:
            raise ProtocolError('limit_exceeded','Current projection exceeds memory budget')
        self.strategies,self.orders,self.positions=summaries,orders,positions
        self.venues=status.get('venues',{})
        self.market_time=runner.clock()
        self.snapshot_at=time.time()
        self.last_refresh=time.monotonic()

    def storage_bytes(self):
        return retained_bytes(dict(config=self.config,strategies=self.strategies,
            orders=self.orders,positions=self.positions,fills=self.fills,
            events=self.events,findings=self.findings,
            venues=self.venues,activities=self.activities,errors=self.errors))

    @staticmethod
    def window(rows,total):
        return (rows[0]['sequence'] if rows else None,rows[-1]['sequence'] if rows else None,total)

    @classmethod
    def window_info(cls,rows,total):
        first,last,total=cls.window(rows,total)
        return dict(retained_start=first,retained_end=last,total_observed=total,
            evicted=total>0 and (first is None or first>1))

    def finding(self,params,request_id):
        author=safe_text(params['author'],128).strip()
        message=safe_text(params['message']).strip()
        if not author or not message:
            raise ProtocolError('invalid_request','Finding author and message must contain plain text')
        sequence=self.finding_sequence+1
        return dict(id=f'finding-{sequence:012d}',sequence=sequence,
            timestamp=time.time(),strategy=params.get('name'),author=author,
            severity=params['severity'],message=message,
            evidence=safe_text(params['evidence']) if 'evidence' in params else None,
            source_request_id=safe_text(request_id,128))

    def accept_finding(self,row):
        self.finding_sequence=row['sequence']
        self.findings.append(row)

    def _encode_cursor(self,tag,last):
        raw=json.dumps(dict(tag=tag,after=last),separators=(',',':')).encode()
        signature=hmac.new(self._cursor_key,raw,hashlib.sha256).hexdigest()
        return base64.urlsafe_b64encode(raw).decode().rstrip('=')+'.'+signature

    def _decode_cursor(self,value,tag):
        bounded_text(value,2048)
        try:
            encoded,signature=value.split('.')
            raw=base64.b64decode(encoded+'='*(-len(encoded)%4),altchars=b'-_',validate=True)
            expected=hmac.new(self._cursor_key,raw,hashlib.sha256).hexdigest()
            if not hmac.compare_digest(signature,expected):
                raise ProtocolError('conflict','Cursor belongs to another session or service')
            data=json.loads(raw)
            if not isinstance(data,dict) or set(data)!={'tag','after'}:
                raise ValueError('Invalid cursor fields')
            if data['tag']!=tag:
                raise ProtocolError('conflict','Cursor collection or filter changed')
            bounded_text(data['after'],1536)
            return data['after']
        except ProtocolError:
            raise
        except (ValueError,UnicodeError,binascii.Error,TypeError):
            raise ProtocolError('invalid_request','Malformed collection cursor') from None

    def page(self,rows, *, scope='active',limit=50,cursor=None,tag=None,window=None):
        if type(limit) is not int or not 1<=limit<=100:
            raise ProtocolError('invalid_request','Page limit must be an integer from 1 to 100')
        after=self._decode_cursor(cursor,tag) if cursor is not None else None
        rows=sorted(rows,key=lambda row:row['id'])
        rows=[row for row in rows if after is None or row['id']>after]
        selected=rows[:limit]
        more=len(rows)>limit
        result=dict(items=selected,next_cursor=self._encode_cursor(tag,selected[-1]['id'])
            if more else None,has_more=more,scope=scope)
        if window is not None:
            first,last,total=window
            result.update(retained_start=first,retained_end=last,evicted=total>0 and (first is None or first>1))
        return result

    def read(self,name,collection=None,**paging):
        if collection is None:
            return self.strategies[name]
        rows={'orders':self.orders,'positions':self.positions,'fills':self.fills,'events':self.events}[collection]
        window=None
        if collection=='fills':
            window=self.window(self.fills,self.fill_sequence)
        if collection=='events':
            window=self.window(self.events,self.event_sequence)
        return self.page([row for row in rows if row['strategy']==name],
            scope='recent' if collection in ('fills','events') else 'active',
            tag=['strategy',name,collection],window=window,**paging)
