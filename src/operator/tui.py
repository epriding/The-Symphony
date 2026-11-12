"""Human terminal client. The independent service owns all trading lifetime."""
from pathlib import Path

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import Footer, Static

from .client import ClientError, RuntimeClient


class BotApp(App[int]):
    TITLE='SYMPHONY'
    CSS_PATH='tui.tcss'
    BINDINGS=[Binding('q','detach','Detach',priority=True),
              Binding('ctrl+c','interrupt','Detach',show=False,priority=True)]

    def __init__(self, socket_path: str | Path):
        super().__init__()
        self.client=RuntimeClient(socket_path)
        self.overview=None

    def compose(self) -> ComposeResult:
        yield Static('SYMPHONY  /  PAPER BOT',id='banner',markup=False)
        yield Static('Connecting to local service...',id='connection',markup=False)
        with Vertical(id='body'):
            yield Static('SESSION  /  unknown',id='session',markup=False)
            yield Static('EXECUTION  /  PAPER ONLY  |  LIVE unsupported',id='mode',markup=False)
            yield Static('Attach observes the service. Opening this client never starts trading.',
                id='guidance',markup=False)
        yield Footer()

    def on_mount(self) -> None:
        self.load_overview()

    @work(group='observations')
    async def load_overview(self) -> None:
        try:
            reply=await self.client.request('overview')
        except ClientError:
            self.query_one('#connection',Static).update('Disconnected / service unavailable. q detaches.')
            return
        self.overview=reply['data']
        self.query_one('#connection',Static).update('Connected / local service')
        self.query_one('#session',Static).update('SESSION  /  '+self.overview['lifecycle'])

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
