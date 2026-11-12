"""Explicit human confirmations; text never becomes a command or finding action."""
from textual.app import ComposeResult
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Input, Static

from .tui_state import label


class Confirmation(ModalScreen[bool]):
    BINDINGS=[('escape','cancel','Cancel')]

    def __init__(self, action: str, target: tuple):
        super().__init__()
        self.action=action
        self.target=target

    def compose(self) -> ComposeResult:
        with Vertical(classes='dialog'):
            yield Static('CONFIRM '+self.action.upper(),classes='dialog_title',markup=False)
            yield Static(f'Service {label(self.target[0])}\nSession {label(self.target[1])}',markup=False)
            warning=('Start creates a NEW paper account. It does not resume held positions.'
                if self.action=='start' else
                'Stop requests cleanup. Positions remain, and cleanup can fail.')
            yield Static(warning,id='confirm_warning',markup=False)
            yield Static('Type '+self.action+' and Enter. Esc cancels; q detaches.',markup=False)
            yield Input(placeholder=self.action,id='confirm_action')
            yield Static('',id='confirm_error',markup=False)

    def on_mount(self) -> None:
        self.query_one(Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.value.strip()==self.action:
            self.dismiss(True)
        else:
            self.query_one('#confirm_error',Static).update('Confirmation did not match; no request sent.')

    def action_cancel(self) -> None:
        self.dismiss(False)


class Help(ModalScreen):
    BINDINGS=[('escape','close','Back')]

    def compose(self) -> ComposeResult:
        with VerticalScroll(classes='dialog'):
            yield Static('SYMPHONY / KEYS AND SEMANTICS',classes='dialog_title',markup=False)
            yield Static(
                'Up/Down or j/k: select or scroll. Enter: open strategy / focus record details.\n'
                'Tab / Shift-Tab: change collection, or focus overview panes.\n'
                'n: next page. b: first page. Esc: overview / cancel / acknowledge unknown outcome.\n'
                's: confirm fresh-session start. p: pause. r: resume. x: confirm stop.\n'
                'q: detach from any view. Ctrl-C: detach with exit code 130.\n\n'
                'Pause gates strategy decisions. Existing orders and risk actions can continue.\n'
                'Start creates a fresh paper account; it does not resume old positions.\n'
                'Stop preserves positions under existing cleanup rules.\n'
                'Controls apply to the whole session, never an individual strategy.\n\n'
                'PAPER is the only execution mode. Public data is not live trading.\n'
                'Fresh connection responses do not prove fresh quotes.\n'
                'Recent windows are bounded; empty pages do not prove no older activity.\n'
                'A lost control reply has an unknown outcome. Observe before acting again.\n'
                'Author names are labels, not authenticated identities. Findings/evidence\n'
                'are inert. Agent reasoning and repairs are external to this terminal.',markup=False)

    def action_close(self) -> None:
        self.dismiss()
