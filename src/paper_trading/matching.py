"""Pure allocation helpers. Committing consumption is the engine's job."""
from decimal import Decimal, ROUND_FLOOR
from .models import dec


def depth_allocations(pool, side, bound, quantity, step):
    """Plan price-level fills without consuming depth; return fills and remainder."""
    remaining = dec(quantity)
    allocations = []
    step = dec(step)
    for price, available in pool.levels(side, dec(bound)):
        fill_quantity = (min(remaining, available) / step).to_integral_value(rounding=ROUND_FLOOR) * step
        if fill_quantity > 0:
            allocations.append((price, fill_quantity))
            remaining -= fill_quantity
        if remaining == 0:
            break
    return allocations, remaining


class PassiveQueue:
    """One price level, ordered from the front of the queue to the back.

    A segment is [None, external_quantity] or [local_order_id, 0]. Local order
    quantities come from the engine so partial fills have one source of truth.
    """
    def __init__(self):
        self.segments = []
        self.last_observed = Decimal(0)

    def join(self, order_id, visible):
        visible = dec(visible)
        increase = max(Decimal(0), visible - self.last_observed)
        if not self.segments:
            increase = visible
        if increase:
            self.segments.append([None, increase])
        self.segments.append([order_id, Decimal(0)])
        self.last_observed = visible

    def observe(self, visible):
        # New external orders join behind existing locals; cancellations do not
        # advance a local order. A later local must join behind these arrivals.
        visible = dec(visible)
        increase = max(Decimal(0), visible - self.last_observed)
        if increase and self.segments:
            self.segments.append([None, increase])
        self.last_observed = visible

    def reset(self, order_ids, visible):
        self.segments = []
        if dec(visible):
            self.segments.append([None, dec(visible)])
        self.segments.extend([order_id, Decimal(0)] for order_id in order_ids)
        self.last_observed = dec(visible)

    def remove(self, order_id):
        self.segments = [segment for segment in self.segments if segment[0] != order_id]

    def consume(self, quantity, remaining_for, fill):
        """Spend one public trade budget; fill returns the quantity actually filled."""
        budget = dec(quantity)
        while budget > 0 and self.segments:
            order_id, ahead = self.segments[0]
            if order_id is None:
                used = min(budget, ahead)
                self.segments[0][1] -= used
                budget -= used
                if self.segments[0][1] == 0:
                    self.segments.pop(0)
            else:
                remaining = remaining_for(order_id)
                if remaining <= 0:
                    self.segments.pop(0)
                    continue
                used = min(budget, remaining)
                used = fill(order_id, used)
                if used <= 0:
                    break
                budget -= used
                if remaining_for(order_id) <= 0:
                    self.remove(order_id)
        self.last_observed = max(Decimal(0), self.last_observed - dec(quantity))
        return budget
