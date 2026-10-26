"""Single-owner scheduled work; callbacks run without portfolio locks."""
import heapq
import inspect
import time
from collections import deque
from typing import NamedTuple, Callable
from .clock import nanoseconds


class ScheduledEvent(NamedTuple):
    timestamp_ns: int
    priority: int
    sequence: int
    callback: Callable
    args: tuple


class Scheduler:
    PUBLIC, CANCEL, ARRIVAL, REPORT = 0, 1, 2, 3

    def __init__(self, clock, limit=100_000):
        self.clock = clock
        self.limit = limit
        self.events = []
        self.counter = 0
        self.peak_pending = 0
        self.callback_count = 0
        self.callback_max_seconds = 0.0
        self.callback_samples = deque(maxlen=min(limit, 1024))

    def processing_metrics(self):
        """Wall-time diagnostics; never used to advance simulated time."""
        samples = sorted(self.callback_samples)

        def percentile(fraction):
            if not samples:
                return None
            index = min(len(samples) - 1, int((len(samples) - 1) * fraction))
            return samples[index]

        return {
            "callback_count": self.callback_count,
            "sample_count": len(samples),
            "sample_window": self.callback_samples.maxlen,
            "max_seconds": self.callback_max_seconds,
            "p50_seconds": percentile(.5),
            "p95_seconds": percentile(.95),
            "p99_seconds": percentile(.99),
        }

    def schedule(self, when, priority, callback, *args):
        if len(self.events) >= self.limit:
            raise BufferError("scheduler_overflow")
        if nanoseconds(when) < nanoseconds(self.clock()):
            raise ValueError("Cannot schedule work in the past")
        self.counter += 1
        event = ScheduledEvent(nanoseconds(when), priority, self.counter, callback, args)
        heapq.heappush(self.events, event)
        self.peak_pending = max(self.peak_pending, len(self.events))

    @property
    def next_time(self):
        return self.events[0].timestamp_ns / 1_000_000_000 if self.events else None

    def take_events(self, priority):
        """Remove one class of queued work, retaining its scheduled order."""
        selected = sorted(event for event in self.events if event.priority == priority)
        self.events = [event for event in self.events if event.priority != priority]
        heapq.heapify(self.events)
        return selected

    async def advance(self, until, inclusive=True):
        end = nanoseconds(until)
        while self.events:
            next_timestamp = self.events[0].timestamp_ns
            beyond_end = next_timestamp > end if inclusive else next_timestamp >= end
            if beyond_end:
                break
            event = heapq.heappop(self.events)
            when = event.timestamp_ns / 1_000_000_000
            if hasattr(self.clock, "advance"):
                # A driver may submit several commands before pumping timers.
                # Overdue work runs now; it cannot move the clock backwards.
                self.clock.advance(max(self.clock(), when))
            began = time.perf_counter()
            try:
                result = event.callback(*event.args)
                if inspect.isawaitable(result):
                    await result
            finally:
                elapsed = time.perf_counter() - began
                self.callback_count += 1
                self.callback_max_seconds = max(self.callback_max_seconds, elapsed)
                self.callback_samples.append(elapsed)
        if hasattr(self.clock, "advance"):
            self.clock.advance(max(self.clock(), until))
