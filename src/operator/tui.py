"""Human terminal client. The independent service owns all trading lifetime."""
from pathlib import Path
import time

from textual import work
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.widgets import DataTable, Footer, Static

from .client import ClientError, RuntimeClient
from .tui_state import ViewState, label
from .tui_dialogs import Confirmation, Help
from .protocol import CONTROL_OPS

COLLECTIONS=('orders','positions','fills','events','findings')
COLUMNS={
    'strategies':('name','execution_mode','enabled','ready','reason','active_order_count','position_count'),
    'orders':('exchange','market_id','outcome','side','status','remaining_quantity','price'),
    'positions':('exchange','market_id','outcome','side','quantity','price'),
    'fills':('exchange','market_id','outcome','side','quantity','price','timestamp'),
    'events':('sequence','action','strategy','timestamp'),
    'findings':('severity','author','strategy','message'),
}


class BotApp(App[int]):
    TITLE='SYMPHONY'
    CSS_PATH='tui.tcss'
    BINDINGS=[Binding('q','detach','Detach',priority=True),
              Binding('ctrl+c','interrupt','Detach',show=False,priority=True),
              Binding('tab','next_pane','Next tab',priority=True),
              Binding('shift+tab','previous_pane','Previous tab',priority=True),
              ('n','next_page','Next page'),('b','first_page','First page'),
              ('escape','back','Overview'),('j','down','Down'),('k','up','Up'),
              ('s','start','Start'),('p','pause','Pause'),('r','resume','Resume'),
              ('x','stop','Stop'),('?','help','Help')]

    def __init__(self, socket_path: str | Path, *, clock=time.monotonic):
        super().__init__()
        self.animation_level='none'
        self.client=RuntimeClient(socket_path)
        self.state=ViewState()
        self.clock=clock
        self._read_worker=None
        self._next_attempt=0
        self._retry_delay=1
        self._visible_rows=[]
        self._table_key=None
        self._command_worker=None
        self.confirmation=None
        self._last_bindings=None

    @property
    def overview(self):
        return self.state.overview

    def compose(self) -> ComposeResult:
        yield Static('SYMPHONY  /  PAPER BOT',id='banner',markup=False)
        yield Static('Connecting to local service...',id='connection',markup=False)
        with Vertical(id='body'):
            yield Static('SESSION  /  unknown',id='session',markup=False)
            yield Static('EXECUTION  /  PAPER ONLY  |  LIVE unsupported',id='mode',markup=False)
            yield Static('Service / unknown',id='identity',markup=False)
            yield Static('Waiting for observations',id='health',markup=False)
            yield Static(self.state.notice,id='notice',markup=False)
            yield Static('Pause gates strategy decisions; existing orders and risk actions can continue.',
                id='control_note',markup=False)
            yield Static('STRATEGIES / loading',id='collection',markup=False)
            yield DataTable(id='records',cursor_type='row',zebra_stripes=True)
            with VerticalScroll(id='detail_scroll',can_focus=True):
                yield Static('Waiting for a collection',id='record_details',markup=False)
        yield Footer()

    def on_mount(self) -> None:
        self.set_interval(1,self.poll)
        self.set_interval(.2,self.render_status)
        self.poll()
        self.query_one('#records',DataTable).focus()

    def poll(self) -> None:
        if self._read_worker is not None and not self._read_worker.is_finished:
            return
        if self.clock()>=self._next_attempt:
            self._read_worker=self.load_overview()

    @work(group='observations')
    async def load_overview(self) -> None:
        try:
            reply=await self.client.request('overview')
        except ClientError:
            self.state.connected=False
            self.state.connection='Disconnected'
            self._next_attempt=self.clock()+self._retry_delay
            self._retry_delay=min(5,self._retry_delay*2)
            self.render_status()
            return
        previous=self.state.generation
        self.state.accept_overview(reply,self.clock())
        if previous!=self.state.generation:
            if self.confirmation is not None:
                self.confirmation.dismiss(False)
            self.render_records()
        self.reconcile_control()
        # The timer supplies cadence; only failed discovery adds backoff.
        # Waiting another second after a reply would skip the next timer tick.
        self._next_attempt=0
        self._retry_delay=1
        self.render_status()
        await self.load_collection()

    async def load_collection(self) -> None:
        key=(self.state.generation,self.state.view_generation)
        name=self.state.strategy
        params={'limit':50}
        if self.state.cursor is not None:
            params['cursor']=self.state.cursor
        op='strategies'
        summary=None
        try:
            if name is not None:
                summary=(await self.client.request('strategy',target=self.state.target,
                    params={'name':name}))['data']
                op='findings' if self.state.collection=='findings' else 'strategy'
                params['name']=name
                if op=='strategy':params['collection']=self.state.collection
            reply=await self.client.request(op,target=self.state.target,params=params)
        except ClientError as error:
            if key==(self.state.generation,self.state.view_generation):
                self.state.page=None
                self.state.notice='View unavailable / '+error.code
                self.render_records()
            return
        if key!=(self.state.generation,self.state.view_generation):
            return
        self.state.summary=summary
        self.state.page=reply['data']
        self.state.page_revision=reply['revision']
        self.render_records()

    def update_text(self, selector: str, content: str) -> None:
        widget=self.query_one(selector,Static)
        if widget.content!=content:
            widget.update(content)

    def render_status(self) -> None:
        age=self.state.age(self.clock())
        status=self.state.connection
        if not self.state.fresh(self.clock()):
            status+=' / STALE'
        status+=f' / last response {int(age)}s ago' if age!=float('inf') else ' / no observations yet'
        self.update_text('#connection',status)
        self.update_text('#notice',self.state.unknown or self.state.notice)
        bindings=tuple(self.control_allowed(op) for op in sorted(CONTROL_OPS))
        if bindings!=self._last_bindings:
            self._last_bindings=bindings
            self.refresh_bindings()
        view=self.overview
        if view is None:
            return
        self.update_text('#session',
            f"SESSION  /  {label(view['lifecycle'])}  |  operator {label(view.get('operator_state'))}")
        pause=('Strategy decisions paused' if view['operator_state']=='paused' else
            'Pausing strategy decisions' if view['operator_state']=='pausing' else 'Pause gates strategy decisions')
        self.update_text('#control_note',pause+'; existing orders and risk actions can continue.')
        self.update_text('#mode','EXECUTION  /  PAPER ONLY  |  LIVE unsupported'
            f"  |  {label(view.get('source'))} data  |  observe-only {label(view.get('observe_only'))}")
        self.update_text('#identity',
            f'Service {self.state.target[0]}\nSession {self.state.target[1] or "none (idle)"}')
        self.update_text('#health',
            f"Strategies ready {label(view.get('strategy_ready_count'))}/{label(view.get('strategy_count'))}"
            f"  |  venues {label(view.get('venue_ready_count'))}/{label(view.get('venue_count'))}"
            f"  |  orders {label(view.get('active_order_count'))}  |  positions {label(view.get('position_count'))}\n"
            f"Observation available {label(view.get('observation_available'))}  |  {label(view.get('reason'))}\n"
            f"Snapshot {label(view.get('snapshot_at'))}  |  bot clock {label(view.get('market_time'))}")

    def action_detach(self) -> None:
        self.exit(0)

    def render_records(self) -> None:
        table=self.query_one('#records',DataTable)
        old_row=table.cursor_row
        selected=self._visible_rows[old_row].get('id') if old_row<len(self._visible_rows) else None
        kind=self.state.collection if self.state.strategy is not None else 'strategies'
        key=(self.state.generation,self.state.view_generation)
        table.clear(columns=True)
        table.add_columns(*(field.replace('_',' ').title() for field in COLUMNS[kind]))
        self._visible_rows=self.state.page['items'] if self.state.page is not None else []
        index=0
        for i,row in enumerate(self._visible_rows):
            table.add_row(*(Text('unrouted' if kind=='strategies' and field=='execution_mode'
                and row.get(field) is None else label(row.get(field))) for field in COLUMNS[kind]),key=str(i))
            if key==self._table_key and row.get('id')==selected:index=i
        self._table_key=key
        if self._visible_rows:table.move_cursor(row=index,animate=False)
        page=self.state.page
        title=f'{self.state.strategy or "ALL STRATEGIES"} / {kind}'
        if page is None:
            title+=' / unavailable or loading'
        else:
            title+=f" / {page['scope']} / revision {self.state.page_revision} / {len(self._visible_rows)} rows"
            title+=' / next page: '+('yes' if page['has_more'] else 'no')
            if page['scope']=='recent':
                title+=f" / retained {page.get('retained_start')}..{page.get('retained_end')} / evicted {page.get('evicted')}"
        self.query_one('#collection',Static).update(title)
        self.render_details()

    def render_details(self) -> None:
        lines=[]
        if self.state.strategy is None and self.overview:
            for key in ('recent_findings','recent_events'):
                rows=self.overview.get(key,[])
                lines.append(key.replace('_',' ').upper()+f' / {len(rows)} retained overview entries')
                for row in rows:
                    lines.append(' | '.join(f'{label(k)}: {label(v)}' for k,v in row.items()))
            lines.append('Connection freshness does not prove fresh quotes. Recent windows are not full history.')
        index=self.query_one('#records',DataTable).cursor_row
        if index<len(self._visible_rows):
            lines.append('SELECTED RECORD / Enter focuses this scrollable pane')
            lines.extend(f'{label(k)}: {label(v)}' for k,v in self._visible_rows[index].items())
        if self.state.summary:
            lines.append('STRATEGY SUMMARY')
            lines.extend(f'{label(k)}: {label(v)}' for k,v in self.state.summary.items())
        if self.overview:
            lines.append('OVERVIEW FIELDS')
            lines.extend(f'{label(k)}: {label(v)}' for k,v in self.overview.items()
                if k not in ('recent_events','recent_findings'))
        self.query_one('#record_details',Static).update('\n'.join(lines) or 'No rows in this collection scope.')

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        self.render_details()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if self.state.strategy is not None:
            self.query_one('#detail_scroll').focus()
        elif event.cursor_row<len(self._visible_rows):
            self.state.strategy=self._visible_rows[event.cursor_row]['name']
            self.state.collection='orders'
            self.navigate()

    def navigate(self, cursor=None) -> None:
        self.state.cursor=cursor
        self.state.page=None
        self.state.view_generation+=1
        self.render_records()
        self._next_attempt=0
        self.poll()

    def action_next_pane(self) -> None:
        if len(self.screen_stack)>1 or self.state.strategy is None:
            self.screen.focus_next()
        else:
            self.state.collection=COLLECTIONS[(COLLECTIONS.index(self.state.collection)+1)%len(COLLECTIONS)]
            self.navigate()

    def action_previous_pane(self) -> None:
        if len(self.screen_stack)>1 or self.state.strategy is None:
            self.screen.focus_previous()
        else:
            self.state.collection=COLLECTIONS[(COLLECTIONS.index(self.state.collection)-1)%len(COLLECTIONS)]
            self.navigate()

    def action_next_page(self) -> None:
        if self.state.page is not None and self.state.page['next_cursor']:
            self.navigate(self.state.page['next_cursor'])

    def action_first_page(self) -> None:
        self.navigate()

    def action_back(self) -> None:
        if len(self.screen_stack)>1:
            self.screen.dismiss(False)
            return
        if self.state.unknown:
            self.state.unknown=''
            self.state.notice='Unknown outcome acknowledged; inspect observations before acting.'
            self.render_status()
            return
        self.state.strategy=None
        self.state.summary=None
        self.navigate()

    def action_down(self) -> None:
        if isinstance(self.focused,DataTable):self.focused.action_cursor_down()
        else:self.query_one('#detail_scroll').scroll_relative(y=1,animate=False)

    def action_up(self) -> None:
        if isinstance(self.focused,DataTable):self.focused.action_cursor_up()
        else:self.query_one('#detail_scroll').scroll_relative(y=-1,animate=False)

    def action_interrupt(self) -> None:
        self.exit(130,return_code=130)

    def control_allowed(self, action: str) -> bool:
        if (not self.state.fresh(self.clock()) or self.overview is None
                or len(self.screen_stack)>1
                or self._command_worker is not None and not self._command_worker.is_finished):
            return False
        if self.state.pending is not None and self.state.pending[0]==action:
            return False
        if self.state.unknown and action!='stop':
            return False
        phase=self.overview['lifecycle']
        operator=self.overview['operator_state']
        if action=='start':
            return (phase in ('idle','stopped','failed')
                and not self.overview.get('cleanup_unresolved')
                and not self.overview.get('finding_append_pending'))
        active=self.state.target[1] is not None and phase in ('starting','ready','degraded','stopping')
        return active and (action=='stop' or phase!='stopping'
            and (action=='pause' and operator=='running' or action=='resume' and operator=='paused'))

    def check_action(self, action: str, parameters: tuple) -> bool | None:
        if len(self.screen_stack)>1 and action not in ('detach','interrupt','next_pane','previous_pane'):
            return False
        return self.control_allowed(action) if action in CONTROL_OPS else True

    def confirm(self, action: str) -> None:
        if not self.control_allowed(action):
            return
        target=self.state.target
        dialog=Confirmation(action,target)
        self.confirmation=dialog
        def completed(approved):
            self.confirmation=None
            if not approved:
                return
            if self.state.target!=target or not self.control_allowed(action):
                self.state.notice='Target or control availability changed; no request sent.'
                self.render_status()
                return
            self._command_worker=self.send_control(action,target)
        self.push_screen(dialog,completed)

    def action_start(self) -> None:
        self.confirm('start')

    def action_stop(self) -> None:
        self.confirm('stop')

    def action_pause(self) -> None:
        if self.control_allowed('pause'):
            self._command_worker=self.send_control('pause',self.state.target)

    def action_resume(self) -> None:
        if self.control_allowed('resume'):
            self._command_worker=self.send_control('resume',self.state.target)

    def action_help(self) -> None:
        if len(self.screen_stack)==1:
            self.push_screen(Help())

    @work(group='controls')
    async def send_control(self, action: str, target: tuple) -> None:
        self.state.pending=(action,target)
        self.state.notice=f'Sending {action} / target {target[0]} / {target[1] or "idle"}'
        self.render_status()
        try:
            reply=await self.client.request(action,target=target)
        except ClientError as error:
            self.state.pending=None
            if error.unknown or error.code=='internal_error':
                self.state.unknown=f'Outcome unknown: {action} / target {target}. No automatic retry. Esc acknowledges.'
            else:
                self.state.notice=f'{action} rejected / {error.code}. Observe before acting again.'
        else:
            expected=(reply['service_id'],reply['session_id'])
            self.state.pending=(action,expected)
            self.state.notice=f'Accepted {action}; awaiting observed transition for {expected}.'
            self.reconcile_control()
        self._next_attempt=0
        self.poll()
        self.render_status()

    def reconcile_control(self) -> None:
        if self.state.pending is None or self.overview is None:
            return
        action,target=self.state.pending
        if target!=self.state.target:
            if self._command_worker is not None and not self._command_worker.is_finished:
                return
            self.state.unknown=f'Outcome unknown: {action} for old target {target}. No retry; Esc acknowledges.'
            self.state.pending=None
            return
        phase=self.overview['lifecycle']
        operator=self.overview['operator_state']
        confirmed=(action=='start' and target[1] is not None and phase!='idle'
            or action=='stop' and phase in ('stopped','failed')
            or action=='pause' and operator=='paused'
            or action=='resume' and operator=='running' and phase in ('starting','ready','degraded'))
        if confirmed:
            self.state.pending=None
            self.state.notice=f'Observed {action} / lifecycle {phase}; inspect readiness, reason and cleanup state.'

    async def on_unmount(self) -> None:
        await self.client.close()


async def run_tui(path: str | Path) -> int:
    app=BotApp(path)
    try:
        await app.run_async()
        return app.return_code
    finally:
        await app.client.close()
