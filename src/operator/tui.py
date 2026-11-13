"""Human terminal client. The independent service owns all trading lifetime."""
from pathlib import Path
import time
import sys

from textual import work
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.widgets import DataTable, Footer, Static, Tab, Tabs
from textual.events import Resize

from .client import ClientError, RuntimeClient
from .tui_state import ViewState, label
from .tui_dialogs import Confirmation, Help
from .tui_layout import DiagnosticDivider
from .tui_diagnostics import Diagnostics,DIAGNOSTIC_VIEWS
from .tui_findings import read_findings
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
WIDTHS={'strategies':(18,8,7,5,20,6,9),'orders':(12,14,7,5,11,10,8),
    'positions':(12,14,7,5,10,10),'fills':(12,12,7,5,9,8,17),
    'events':(8,18,18,20),'findings':(8,14,16,43)}
TITLES={'name':'Strategy','execution_mode':'Mode','active_order_count':'Orders',
    'position_count':'Positions','market_id':'Market','remaining_quantity':'Remaining','timestamp':'Time'}


class BotApp(App[int]):
    TITLE='SYMPHONY'
    CSS_PATH='tui.tcss'
    BINDINGS=[Binding('q','detach','Detach',priority=True),
              ('?','help','Help'),
              ('f2','expand_diagnostics','Expand diagnostics'),
              Binding('f3','next_diagnostic','Diagnostic view',show=False),
              Binding('f4','focus_records','Main table',show=False),
              Binding('[','shrink_diagnostics','Smaller',show=False),
              Binding(']','grow_diagnostics','Larger',show=False),
              Binding('ctrl+c','interrupt','Detach',show=False,priority=True),
              Binding('tab','next_pane','Next tab',priority=True,key_display='Tab'),
              Binding('shift+tab','previous_pane','Previous tab',priority=True,show=False),
              ('n','next_page','Next page'),('b','first_page','First page'),
              Binding('escape','back','Overview',key_display='Esc'),
              Binding('j','down','Down',show=False),Binding('k','up','Up',show=False),
              ('s','start','Start'),('p','pause','Pause'),('r','resume','Resume'),
              ('x','stop','Stop')]

    def __init__(self, socket_path: str | Path, *, clock=time.monotonic, ascii_mode: bool | None = None):
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
        self._detaching=False
        self._terminal_usable=False
        self.diagnostic_fraction=1/3
        self.diagnostics_expanded=False
        encoding=(sys.stdout.encoding or 'utf-8').lower().replace('-','')
        self.ascii_mode=ascii_mode if ascii_mode is not None else encoding not in ('utf8','utf8sig')

    def text(self, value: object) -> str:
        text=label(value)
        return text.encode('ascii','backslashreplace').decode() if self.ascii_mode else text

    @property
    def overview(self):
        return self.state.overview

    def compose(self) -> ComposeResult:
        yield Static('SYMPHONY  /  PAPER BOT',id='banner',markup=False)
        yield Static('Connecting to local service...',id='connection',markup=False)
        yield Static('Resize terminal to at least 100x28. q detaches.',id='resize',markup=False)
        with Vertical(id='body'):
            yield Static('SESSION  /  unknown',id='session',markup=False)
            yield Static('EXECUTION  /  PAPER ONLY  |  LIVE unsupported',id='mode',markup=False)
            yield Static('Service / unknown',id='identity',markup=False)
            yield Static('Waiting for observations',id='health',markup=False)
            yield Static(self.state.notice,id='notice',markup=False)
            yield Static('Pause gates strategy decisions; existing orders and risk actions can continue.',
                id='control_note',markup=False)
            yield Static('ALL STRATEGIES',id='view_title',markup=False)
            yield Tabs(*(Tab(kind.title(),id='view-'+kind) for kind in COLLECTIONS),id='view_tabs')
            yield Static('STRATEGIES / loading',id='collection',markup=False)
            with Vertical(id='workspace'):
                yield DataTable(id='records',cursor_type='row',zebra_stripes=True)
                yield DiagnosticDivider()
                yield Diagnostics()
        yield Footer()

    def on_mount(self) -> None:
        if self.ascii_mode:self.add_class('ascii')
        self.set_interval(1,self.poll)
        self.set_interval(.2,self.render_status)
        self.poll()
        self.query_one('#records',DataTable).focus()
        self.update_size()

    def on_resize(self, event: Resize) -> None:
        self.update_size(event.size)

    def update_size(self, size=None) -> None:
        if not self.query('#body'):
            return
        size=self.size if size is None else size
        small=size.width<100 or size.height<28
        self._terminal_usable=not small
        self.set_class(size.height<36,'compact')
        self.query_one('#body').display=not small
        self.query_one('#resize').display=small
        if small and self.confirmation is not None:
            self.confirmation.dismiss(False)

    def adjust_diagnostics(self, delta: float) -> None:
        if self.diagnostics_expanded:return
        self.diagnostic_fraction=min(.75,max(.2,self.diagnostic_fraction+delta))
        self.query_one('#diagnostics').styles.height=f'{self.diagnostic_fraction}fr'
        self.query_one('#records').styles.height=f'{1-self.diagnostic_fraction}fr'

    def action_grow_diagnostics(self) -> None:
        self.adjust_diagnostics(.1)

    def action_shrink_diagnostics(self) -> None:
        self.adjust_diagnostics(-.1)

    def action_expand_diagnostics(self) -> None:
        self.diagnostics_expanded=not self.diagnostics_expanded
        self.set_class(self.diagnostics_expanded,'diagnostics_expanded')
        self.query_one('#records').display=not self.diagnostics_expanded
        self.query_one('#view_tabs').display=self.state.strategy is not None and not self.diagnostics_expanded
        self.update_text('#diagnostic_divider','DIAGNOSTICS / F2 or Esc restore'
            if self.diagnostics_expanded else
            'DIAGNOSTICS / drag divider / [ smaller / ] larger / F2 expand')
        if self.diagnostics_expanded:self.focus_diagnostics()
        else:self.query_one('#records').focus()

    def poll(self) -> None:
        if self._detaching:return
        if self._read_worker is not None and not self._read_worker.is_finished:
            return
        if self.clock()>=self._next_attempt:
            self._read_worker=self.load_overview()

    @work(group='observations')
    async def load_overview(self) -> None:
        try:
            reply=await self.client.request('overview')
        except ClientError as error:
            if self._detaching:return
            self.state.connected=False
            self.state.connection='Disconnected / '+error.code+' / read-only retry; b refreshes'
            self._next_attempt=self.clock()+self._retry_delay
            self._retry_delay=min(5,self._retry_delay*2)
            self.render_status()
            return
        if self._detaching:return
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
        key=(self.state.generation,self.state.view_generation)
        await self.load_collection()
        await self.load_diagnostics(key)

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
                self.state.summary=None
                self.state.notice='View unavailable / '+error.code
                self.render_records()
            return
        if key!=(self.state.generation,self.state.view_generation):
            return
        self.state.summary=summary
        self.state.page=reply['data']
        self.state.page_revision=reply['revision']
        self.render_records()

    async def load_diagnostics(self, key) -> None:
        if key!=(self.state.generation,self.state.view_generation) or self._detaching:return
        history=None
        failure=None
        try:
            history=await read_findings(self.client,self.state.target,self.state.strategy)
        except ClientError as error:
            failure=error.code
        if key!=(self.state.generation,self.state.view_generation) or self._detaching:return
        diagnostic=self.query_one(Diagnostics)
        if history is not None:diagnostic.history=history
        diagnostic.failure=failure
        diagnostic.render_diagnostics()

    def update_text(self, selector: str, content: str) -> None:
        widget=self.query_one(selector,Static)
        if widget.content!=content:
            widget.update(content)

    def render_status(self) -> None:
        if self._detaching:return
        age=self.state.age(self.clock())
        status=self.state.connection
        if not self.state.fresh(self.clock()):
            status+=' / STALE'
        status+=f' / last response {int(age)}s ago' if age!=float('inf') else ' / no observations yet'
        self.update_text('#connection',status)
        self.update_text('#notice',self.text(self.state.unknown or self.state.notice))
        self.query_one(Diagnostics).render_status()
        bindings=tuple(self.control_allowed(op) for op in sorted(CONTROL_OPS))
        if bindings!=self._last_bindings:
            self._last_bindings=bindings
            self.refresh_bindings()
        view=self.overview
        if view is None:
            return
        alert=('CLEANUP UNRESOLVED' if view['cleanup_unresolved'] else
            'FINDING APPEND PENDING' if view['finding_append_pending'] else
            'FINDINGS UNAVAILABLE' if not view['finding_publication_available'] else '')
        self.update_text('#session',
            f"SESSION  /  {self.text(view['lifecycle'])}  |  operator {self.text(view.get('operator_state'))}"
            +('  |  '+alert if alert else ''))
        pause=('Strategy decisions paused' if view['operator_state']=='paused' else
            'Pausing strategy decisions' if view['operator_state']=='pausing' else 'Pause gates strategy decisions')
        self.update_text('#control_note',pause+'; existing orders and risk actions can continue.')
        self.update_text('#mode','EXECUTION  /  PAPER ONLY  |  LIVE unsupported'
            f"  |  {self.text(view.get('source'))} data  |  observe-only {self.text(view.get('observe_only'))}")
        self.update_text('#identity',
            f'Service {self.text(self.state.target[0])}\nSession {self.text(self.state.target[1] or "none (idle)")}')
        self.update_text('#health',
            f"Strategies ready {self.text(view.get('strategy_ready_count'))}/{self.text(view.get('strategy_count'))}"
            f"  |  venues {self.text(view.get('venue_ready_count'))}/{self.text(view.get('venue_count'))}"
            f"  |  orders {self.text(view.get('active_order_count'))}  |  positions {self.text(view.get('position_count'))}\n"
            f"Observation available {self.text(view.get('observation_available'))}  |  {self.text(view.get('reason'))}\n"
            f"Snapshot {self.text(view.get('snapshot_at'))}  |  bot clock {self.text(view.get('market_time'))}")

    def action_detach(self) -> None:
        self._detaching=True
        self.workers.cancel_all()
        self.exit(0)

    def render_records(self) -> None:
        if self._detaching:return
        self.query_one(Diagnostics).set_context((self.state.target,self.state.strategy))
        table=self.query_one('#records',DataTable)
        old_row=table.cursor_row
        selected=self._visible_rows[old_row].get('id') if old_row<len(self._visible_rows) else None
        kind=self.state.collection if self.state.strategy is not None else 'strategies'
        self.update_text('#view_title',
            f'Strategy: {self.text(self.state.strategy)}  /  {kind.upper()}'
            if self.state.strategy is not None else 'ALL STRATEGIES')
        tabs=self.query_one('#view_tabs',Tabs)
        tabs.display=self.state.strategy is not None and not self.diagnostics_expanded
        if tabs.display and tabs.active!='view-'+kind:
            tabs.active='view-'+kind
        key=(self.state.generation,self.state.view_generation)
        table.clear(columns=True)
        for field,width in zip(COLUMNS[kind],WIDTHS[kind]):
            table.add_column(TITLES.get(field,field.replace('_',' ').title()),width=width)
        self._visible_rows=self.state.page['items'] if self.state.page is not None else []
        index=0
        for i,row in enumerate(self._visible_rows):
            table.add_row(*(Text('unrouted' if kind=='strategies' and field=='execution_mode'
                and row.get(field) is None else self.text(row.get(field)),overflow='crop',no_wrap=True)
                for field in COLUMNS[kind]),key=str(i))
            if key==self._table_key and row.get('id')==selected:index=i
        self._table_key=key
        if self._visible_rows:table.move_cursor(row=index,animate=False)
        page=self.state.page
        title=f'{self.text(self.state.strategy) if self.state.strategy is not None else "ALL STRATEGIES"} / {kind}'
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
        if self._detaching or not self.query('#records'):return
        lines=[]
        index=self.query_one('#records',DataTable).cursor_row
        if index<len(self._visible_rows):
            lines.append('SELECTED RECORD / Enter focuses this scrollable pane')
            lines.extend(f'{self.text(k)}: {self.text(v)}' for k,v in self._visible_rows[index].items())
        if self.state.summary:
            lines.append('STRATEGY SUMMARY')
            lines.extend(f'{self.text(k)}: {self.text(v)}' for k,v in self.state.summary.items())
        if self.overview:
            lines.append('OVERVIEW FIELDS')
            lines.extend(f'{self.text(k)}: {self.text(v)}' for k,v in self.overview.items()
                if k not in ('recent_events','recent_findings'))
        self.query_one('#record_details',Static).update('\n'.join(lines) or 'No rows in this collection scope.')

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id=='records':self.render_details()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id!='records':return
        if self.state.strategy is not None:
            self.query_one(Diagnostics).show_mode('details')
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

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        if event.tabs.id!='view_tabs' or self.state.strategy is None:
            return
        kind=event.tab.id.removeprefix('view-')
        if kind!=self.state.collection and event.tabs.active==event.tab.id:
            self.state.collection=kind
            self.navigate()

    def diagnostic_focused(self) -> bool:
        return self.focused is not None and self.query_one(Diagnostics) in self.focused.ancestors

    def focus_diagnostics(self) -> None:
        mode=self.query_one(Diagnostics).mode
        self.query_one('#detail_scroll' if mode=='details' else '#diagnostic_records').focus()

    def action_next_diagnostic(self) -> None:
        diagnostic=self.query_one(Diagnostics)
        diagnostic.show_mode(DIAGNOSTIC_VIEWS[(DIAGNOSTIC_VIEWS.index(diagnostic.mode)+1)%3])
        self.focus_diagnostics()

    def action_focus_records(self) -> None:
        if self.diagnostics_expanded:self.action_expand_diagnostics()
        self.query_one('#records').focus()

    def action_next_pane(self) -> None:
        if len(self.screen_stack)>1 or self.state.strategy is None or self.diagnostic_focused():
            self.screen.focus_next()
        else:
            self.state.collection=COLLECTIONS[(COLLECTIONS.index(self.state.collection)+1)%len(COLLECTIONS)]
            self.navigate()

    def action_previous_pane(self) -> None:
        if len(self.screen_stack)>1 or self.state.strategy is None or self.diagnostic_focused():
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
        if self.diagnostics_expanded:
            self.action_expand_diagnostics()
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
        self._detaching=True
        self.workers.cancel_all()
        self.exit(130,return_code=130)

    def control_allowed(self, action: str) -> bool:
        if (self._detaching or not self.state.fresh(self.clock()) or self.overview is None
                or not self._terminal_usable
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
        dialog=Confirmation(action,target,ascii_mode=self.ascii_mode)
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
        self._detaching=True
        self.workers.cancel_all()
        await self.client.close()


async def run_tui(path: str | Path) -> int:
    app=BotApp(path)
    try:
        await app.run_async()
        return app.return_code
    finally:
        await app.client.close()
