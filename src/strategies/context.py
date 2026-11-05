"""The deliberately small, deterministic strategy-facing capability set."""
from copy import deepcopy
from hashlib import sha256
import random

from ..paper_trading.clock import IdGenerator
from ..utilities.classes import OrderIntent


class StrategyContext:
    __slots__ = ('strategy_name', 'portfolio_id', 'settings', 'random', '_clock', '_ids', '_time')

    def __init__(self, strategy_name, portfolio_id, settings, clock, seed, session_id):
        self.strategy_name = strategy_name
        self.portfolio_id = portfolio_id
        self.settings = deepcopy(settings)
        self._clock = clock
        self._time = None
        seed_value = int.from_bytes(sha256(f'{seed}:{strategy_name}'.encode()).digest(), 'big')
        self.random = random.Random(seed_value)
        # A stable digest prevents arbitrary strategy names from growing intent IDs.
        suffix = sha256(strategy_name.encode()).hexdigest()[:16]
        self._ids = IdGenerator(f'{session_id}-{suffix}-intent')

    def now(self):
        return self._clock() if self._time is None else self._time

    def _set_time(self, timestamp):
        self._time = timestamp

    def new_intent(self, **values):
        if values.get('strategy', self.strategy_name) != self.strategy_name:
            raise ValueError('Intent strategy conflicts with registered strategy identity')
        values['strategy'] = self.strategy_name
        values['created_at'] = self.now()
        if 'intent_id' in values:
            raise ValueError('Intent identifiers are supplied by the strategy context')
        values['intent_id'] = self._ids()
        return OrderIntent(**values)
