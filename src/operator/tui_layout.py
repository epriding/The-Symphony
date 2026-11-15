"""Mouse and keyboard handle for the bounded diagnostic split."""
from textual import events
from textual.widgets import Static


class DiagnosticDivider(Static,can_focus=True):
    BINDINGS=[('left','smaller','Smaller'),('right','larger','Larger'),('enter','expand','Expand')]

    def __init__(self):
        super().__init__('DIAGNOSTICS / drag divider / [ smaller / ] larger / F2 expand',
            id='diagnostic_divider',markup=False)
        self._drag_y=None

    def action_smaller(self) -> None:
        self.app.adjust_diagnostics(-.1)

    def action_larger(self) -> None:
        self.app.adjust_diagnostics(.1)

    def action_expand(self) -> None:
        self.app.action_expand_diagnostics()

    def on_mouse_down(self,event: events.MouseDown) -> None:
        if event.button==1:
            self._drag_y=event.screen_y
            self.capture_mouse()
            event.stop()

    def on_mouse_move(self,event: events.MouseMove) -> None:
        if self._drag_y is not None:
            space=max(1,self.parent.content_size.height-1)
            self.app.adjust_diagnostics((self._drag_y-event.screen_y)/space)
            self._drag_y=event.screen_y
            event.stop()

    def on_mouse_up(self,event: events.MouseUp) -> None:
        self._drag_y=None
        self.capture_mouse(False)
        event.stop()
