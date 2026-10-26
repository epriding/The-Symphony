"""Session clocks use integer nanoseconds for deterministic timer ordering."""
import time
from decimal import Decimal


def nanoseconds(seconds):
    return int(Decimal(str(seconds)) * 1_000_000_000)


class VirtualClock:
    def __init__(self, start):
        self.now_ns = nanoseconds(start)

    def __call__(self):
        return self.now_ns / 1_000_000_000

    def advance(self, timestamp):
        stamp = nanoseconds(timestamp)
        if stamp < self.now_ns:
            raise ValueError("Virtual time cannot move backwards")
        self.now_ns = stamp


class RealClock:
    def __init__(self):
        self.origin = time.time_ns()
        self.monotonic_origin = time.monotonic_ns()

    def __call__(self):
        return (self.origin + time.monotonic_ns() - self.monotonic_origin) / 1_000_000_000


class IdGenerator:
    def __init__(self, prefix="paper"):
        self.prefix, self.counter = prefix, 0

    def __call__(self):
        self.counter += 1
        return f"{self.prefix}-{self.counter:012d}"
