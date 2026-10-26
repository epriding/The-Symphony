"""Registry is intentionally data driven: a third exchange needs a profile."""
from dataclasses import replace
from .models import Delay, FeeSchedule, VenueRules


def fixture_venue(exchange="TEST", *, fee="0", **changes):
    """Synthetic, unlimited-in-practice fixture settings, never live defaults."""
    base = VenueRules(exchange, FeeSchedule("synthetic", "flat", fee), 5.0,
                      Delay(0), Delay(0), Delay(0), Delay(0), 1000, 1000,
                      netted_outcomes=False)
    return replace(base, **changes)


class RequestBudget:
    """Token bucket reserves queued slots in chronological request order."""
    def __init__(self, rules, now):
        self.rules, self.time, self.tokens = rules, now, rules.request_capacity

    def request(self, now, operation):
        cost = self.rules.cancel_cost if operation == "cancel" else self.rules.submit_cost
        effective = max(now, self.time)
        tokens = min(self.rules.request_capacity,
                     self.tokens + max(0, now - self.time) * self.rules.request_rate)
        if tokens >= cost and now >= self.time:
            self.tokens, self.time = tokens - cost, now
            return now
        if self.rules.rate_policy == "reject":
            return None
        effective += max(0, cost - tokens) / self.rules.request_rate
        self.tokens, self.time = max(0, tokens - cost), effective
        return effective
