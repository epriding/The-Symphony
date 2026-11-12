"""Human terminal client. The independent service owns all trading lifetime."""
from pathlib import Path
import time

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import Footer, Static

from .client import ClientError, RuntimeClient
from .tui_state import ViewState, label


class BotApp(App[int]):
    TITLE='SYMPHONY'
    CSS_PATH='tui.tcss'
    BINDINGS=[Binding('q','detach','Detach',priority=True),
              Binding('ctrl+c','interrupt','Detach',show=False,priority=True)]

    def __init__(self, socket_path: str | Path, *, clock=time.monotonic):
        super().__init__()
        self.client=RuntimeClient(socket_path)
        self.state=ViewState()
        self.clock=clock
        self._read_worker=None
        self._next_attempt=0
        self._retry_delay=1

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
        yield Footer()

    def on_mount(self) -> None:
        self.set_interval(1,self.poll)
        self.set_interval(.2,self.render_status)
        self.poll()

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
        self.state.accept_overview(reply,self.clock())
        # The timer supplies cadence; only failed discovery adds backoff.
        # Waiting another second after a reply would skip the next timer tick.
        self._next_attempt=0
        self._retry_delay=1
        self.render_status()

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
            f"  |  venues ready {label(view.get('venue_ready_count'))}/{label(view.get('venue_count'))}\n"
            f"Active orders {label(view.get('active_order_count'))}  |  owned positions {label(view.get('position_count'))}\n"
            f"Observation available {label(view.get('observation_available'))}  |  {label(view.get('reason'))}\n"
            f"Snapshot {label(view.get('snapshot_at'))}  |  bot clock {label(view.get('market_time'))}\n"
            'Connection freshness does not prove fresh quotes.')

    def action_detach(self) -> None:
        self.exit(0)

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
