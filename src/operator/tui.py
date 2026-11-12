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
              ('escape','back','Overview'),('j','down','Down'),('k','up','Up')]

    def __init__(self, socket_path: str | Path, *, clock=time.monotonic):
        super().__init__()
        self.client=RuntimeClient(socket_path)
        self.state=ViewState()
        self.clock=clock
        self._read_worker=None
        self._next_attempt=0
        self._retry_delay=1
        self._visible_rows=[]
        self._table_key=None

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
            self.render_records()
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

    def render_status(self) -> None:
        age=self.state.age(self.clock())
        status=self.state.connection
        if not self.state.fresh(self.clock()):
            status+=' / STALE'
        status+=f' / last response {age:.1f}s ago' if age!=float('inf') else ' / no observations yet'
        self.query_one('#connection',Static).update(status)
        self.query_one('#notice',Static).update(self.state.notice)
        view=self.overview
        if view is None:
            return
        self.query_one('#session',Static).update(
            f"SESSION  /  {label(view['lifecycle'])}  |  operator {label(view.get('operator_state'))}")
        self.query_one('#mode',Static).update('EXECUTION  /  PAPER ONLY  |  LIVE unsupported'
            f"  |  {label(view.get('source'))} data  |  observe-only {label(view.get('observe_only'))}")
        self.query_one('#identity',Static).update(
            f'Service {self.state.target[0]}\nSession {self.state.target[1] or "none (idle)"}')
        self.query_one('#health',Static).update(
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
        if self.state.strategy is None:
            self.screen.focus_next()
        else:
            self.state.collection=COLLECTIONS[(COLLECTIONS.index(self.state.collection)+1)%len(COLLECTIONS)]
            self.navigate()

    def action_previous_pane(self) -> None:
        if self.state.strategy is None:
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

    async def on_unmount(self) -> None:
        await self.client.close()


async def run_tui(path: str | Path) -> int:
    app=BotApp(path)
    try:
        await app.run_async()
        return app.return_code
    finally:
        await app.client.close()
