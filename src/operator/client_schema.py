"""Version-one observation shapes, checked before a client applies any reply."""
from decimal import Decimal, InvalidOperation
import math

from .protocol import bounded_text

PHASES=('idle','starting','ready','degraded','stopping','stopped','failed')
OPERATOR_STATES=('running','pausing','paused')
ROWS={
    'strategies': 'id name factory enabled portfolio_id execution_mode ready reason operator_state active_order_count position_count last_activity last_error',
    'orders': 'id order_id strategy attribution portfolio_id exchange market_id outcome side quantity filled_quantity remaining_quantity price status timestamp',
    'positions': 'id strategy attribution portfolio_id exchange market_id outcome side quantity price status timestamp',
    'fills': 'id execution_id order_id sequence strategy attribution portfolio_id exchange market_id outcome side quantity price timestamp status',
    'events': 'id sequence action strategy attribution severity message timestamp',
    'findings': 'id sequence timestamp strategy author severity message evidence source_request_id session_id',
}


def _count(value, *, nullable=False):
    if not (nullable and value is None) and (type(value) is not int or value<0):
        raise ValueError('Invalid count')


def _time(value, *, nullable=False):
    try:
        valid=type(value) in (int,float) and math.isfinite(value)
    except OverflowError:
        valid=False
    if not (nullable and value is None) and not valid:
        raise ValueError('Invalid time')


def _text(value, limit=2048, *, nullable=False):
    if nullable and value is None:return
    if not isinstance(value,str) or len(value.encode('utf-8'))>limit:
        raise ValueError('Invalid text')


def _row(row, kind, session):
    if not isinstance(row,dict) or set(row)!=set(ROWS[kind].split()):
        raise ValueError('Invalid row fields')
    for key,value in row.items():
        if key in ('enabled','ready'):
            if type(value) is not bool:raise ValueError('Invalid flag')
        elif key in ('sequence','active_order_count','position_count'):
            _count(value)
        elif key=='timestamp':
            _time(value,nullable=True)
        elif key=='last_activity':
            if value is not None:
                if not isinstance(value,dict) or set(value)!={'action','timestamp'}:raise ValueError()
                _text(value['action']);_time(value['timestamp'])
        elif key in ('quantity','price','filled_quantity','remaining_quantity'):
            if value is not None:
                _text(value)
                try:
                    if not Decimal(value).is_finite():raise ValueError()
                except InvalidOperation:raise ValueError() from None
        else:
            _text(value,128 if key in ('author','source_request_id') else 2048,
                nullable=key in ('strategy','portfolio_id','execution_mode','last_error','evidence'))
    bounded_text(row['id'],256,identifier=True)
    if kind=='strategies':
        for key in ('name','factory'):bounded_text(row[key],256,identifier=True)
        if (row['id']!=row['name'] or row['portfolio_id'] not in (None,'PAPER')
                or row['execution_mode']!=row['portfolio_id'] or row['operator_state'] not in OPERATOR_STATES):
            raise ValueError('Invalid strategy identity or mode')
    elif kind=='findings':
        if row['session_id']!=session or row['severity'] not in ('info','warning','error'):
            raise ValueError('Invalid finding target')
    elif row.get('portfolio_id','PAPER')!='PAPER':
        raise ValueError('Invalid execution mode')


def _window(value, *, total=False):
    keys={'retained_start','retained_end','evicted'}|({'total_observed'} if total else set())
    if not isinstance(value,dict) or set(value)!=keys or type(value['evicted']) is not bool:
        raise ValueError('Invalid retention window')
    first,last=value['retained_start'],value['retained_end']
    _count(first,nullable=True);_count(last,nullable=True)
    if (first is None)!=(last is None) or first is not None and last<first:raise ValueError()
    if total:_count(value['total_observed'])


def validate_data(data: dict, request: dict, session: str | None) -> None:
    op=request['op']
    if op in ('start','stop','pause','resume'):
        if (set(data)!={'accepted','lifecycle','operator_state'} or data['accepted'] is not True
                or data['lifecycle'] not in PHASES or data['operator_state'] not in OPERATOR_STATES
                or op=='start' and session is None):raise ValueError('Invalid admission')
        return
    if op=='overview':
        expected=set(('lifecycle reason operator_state source observe_only live_execution_supported '
            'cleanup_unresolved strategy_count strategy_ready_count venue_count venue_ready_count '
            'active_order_count position_count market_time snapshot_at observation_available '
            'recent_events event_window recent_findings finding_window finding_publication_available '
            'finding_append_pending service_limits capture_id capture_directory').split())
        if set(data)!=expected:raise ValueError('Invalid overview fields')
        if data['lifecycle'] not in PHASES or data['operator_state'] not in OPERATOR_STATES:
            raise ValueError('Invalid lifecycle')
        if data['source'] not in ('public','synthetic') or data['live_execution_supported'] is not False:
            raise ValueError('Unsupported execution mode')
        for key in ('observe_only','cleanup_unresolved','observation_available',
                    'finding_publication_available','finding_append_pending'):
            if type(data[key]) is not bool:raise ValueError('Invalid flag')
        for key in ('strategy_count','strategy_ready_count','venue_count','venue_ready_count',
                    'active_order_count','position_count'):_count(data[key],nullable=True)
        for key in ('market_time','snapshot_at'):_time(data[key],nullable=True)
        _text(data['reason']);_text(data['capture_id'],nullable=True)
        _text(data['capture_directory'],4096,nullable=True)
        keys=set(('memory_budget_bytes reserved_bytes memory_highwater_bytes projection_bytes '
                  'clients client_highwater queued queue_highwater').split())
        if not isinstance(data['service_limits'],dict) or set(data['service_limits'])!=keys:
            raise ValueError('Invalid service limits')
        for value in data['service_limits'].values():_count(value)
        for kind,window in [('events','event_window'),('findings','finding_window')]:
            rows=data['recent_'+kind]
            if not isinstance(rows,list) or len(rows)>8:raise ValueError('Invalid recent rows')
            for row in rows:_row(row,kind,session)
            _window(data[window],total=True)
        return
    if op=='publish_finding':
        _row(data,'findings',session)
        return
    if op=='strategy' and not request['params'].get('collection'):
        _row(data,'strategies',session)
        if data['name']!=request['params']['name']:raise ValueError('Foreign strategy')
        return
    kind=request['params'].get('collection',op)
    recent=kind in ('fills','events','findings')
    expected={'items','scope','has_more','next_cursor'}
    if recent:expected|={'retained_start','retained_end','evicted'}
    if (set(data)!=expected or not isinstance(data['items'],list) or len(data['items'])>100
            or data['scope']!=('recent' if recent else 'active')
            or type(data['has_more']) is not bool):raise ValueError('Invalid page')
    bounded_text(data['next_cursor'],2048,nullable=True)
    if data['has_more']!=(data['next_cursor'] is not None):raise ValueError('Invalid cursor state')
    for row in data['items']:
        _row(row,kind,session)
        name=request['params'].get('name')
        if name is not None and row.get('strategy')!=name:raise ValueError('Foreign row')
    if recent:_window({key:data[key] for key in ('retained_start','retained_end','evicted')})
