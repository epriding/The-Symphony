"""Client observation state; never an owner of accounting or session lifetime."""
from dataclasses import dataclass

from .observations import safe_text


def label(value: object) -> str:
    return 'unknown' if value is None else safe_text(value)


@dataclass
class ViewState:
    overview: dict | None = None
    target: tuple | None = None
    received_at: float | None = None
    connected: bool = False
    generation: int = 0
    notice: str = 'Attach observes the service. Opening this client never starts trading.'
    connection: str = 'Connecting'
    strategy: str | None = None
    collection: str = 'orders'
    cursor: str | None = None
    page: dict | None = None
    summary: dict | None = None
    page_revision: int | None = None
    view_generation: int = 0

    def accept_overview(self, reply: dict, now: float) -> None:
        target=(reply['service_id'],reply['session_id'])
        if target!=self.target:
            if self.target is not None:
                self.notice='Target changed / prior views and confirmations are invalid.'
            self.generation+=1
            self.strategy=None
            self.cursor=None
            self.page=None
            self.summary=None
            self.view_generation+=1
        self.target=target
        self.overview=reply['data']
        self.received_at=now
        self.connected=True
        self.connection='Connected'

    def age(self, now: float) -> float:
        return float('inf') if self.received_at is None else max(0,now-self.received_at)

    def fresh(self, now: float) -> bool:
        return self.connected and self.age(now)<3
