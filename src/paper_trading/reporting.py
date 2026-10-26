"""Execution diagnostics never supply prices to matching or risk decisions."""
from collections import Counter
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
import json
import shutil

from .matching import depth_allocations
from .fees import FeeAccumulator, FeeModel
from .models import dec
from .orderbook import canonical_order


class Reporter:
    def __init__(self, engine):
        self.engine = engine
        self.markouts = {}
        self.seen_executions = 0
        self.pending_markouts = []

    def observe(self):
        engine = self.engine
        for execution in engine.executions[self.seen_executions:]:
            for horizon in engine.config.markout_horizons:
                self.pending_markouts.append((execution, horizon))
        self.seen_executions = len(engine.executions)
        pending = []
        for execution, horizon in self.pending_markouts:
            pool = engine.books.get((execution.exchange, execution.market_id))
            if (engine.clock() < execution.timestamp + horizon or not pool or
                    pool.received_at < execution.timestamp + horizon or
                    pool.executable(engine.clock(), engine.config.venues[execution.exchange].max_book_age)):
                pending.append((execution, horizon))
                continue
            bid, ask = pool.quote("SELL", execution.outcome), pool.quote("BUY", execution.outcome)
            if bid is None or ask is None:
                pending.append((execution, horizon))
                continue
            mid = (dec(bid) + dec(ask)) / 2
            direction = 1 if execution.side == "BUY" else -1
            self.markouts[f"{execution.execution_id}:{horizon}"] = {
                "horizon": horizon, "observed_at": engine.clock(),
                "dollars": str(direction * (mid - dec(execution.price)) * dec(execution.quantity)),
            }
        self.pending_markouts = pending

    def summary(self, state):
        engine = self.engine
        orders = list(engine.orders.values())
        return {
            "session_id": engine.config.session_id,
            "assumptions": engine.config.assumptions,
            "counts": dict(Counter(order.record.status.value for order in orders)),
            "submitted": len(orders),
            "full_fills": sum(order.filled == dec(order.approved.size) for order in orders),
            "partial_fills": sum(0 < order.filled < dec(order.approved.size) for order in orders),
            "zero_fills": sum(order.filled == 0 for order in orders),
            **self._execution_cost_summary(),
            "accounts": self._account_summaries(state),
            "diagnostics": dict(engine.diagnostics),
            "markouts": self.markouts,
            "missing_markouts": len(self.pending_markouts),
            "peak_pending_events": engine.scheduler.peak_pending,
            "processing_wall_time": engine.scheduler.processing_metrics(),
            "undelivered_reports": len(engine.outbox) + len(engine.credit_outbox),
            "audit_window_events": len(engine.audit),
            "audit_total_events": sum(engine.diagnostics.values()),
            "full_audit_streamed": engine.audit_path is not None,
            "groups": self._group_summaries(),
            "by_venue": self._venue_summaries(orders),
            "by_strategy": self._strategy_summaries(orders),
            "by_time_in_force": dict(Counter(order.approved.tif.value for order in orders)),
            "fidelity_limits": [
                "Aggregated depth; counterparty fragmentation unknown",
                "Conservative queues can underfill; P&L is not a lower bound",
                "Receive-time simulation does not reconstruct unreceived exchange events",
            ],
        }

    def _execution_cost_summary(self):
        cost_vs_mid = Decimal(0)
        slippage = Decimal(0)
        spread = Decimal(0)
        delay_cost = Decimal(0)
        walking_cost = Decimal(0)
        missing_benchmark_quantity = Decimal(0)
        for fill in self.engine.executions:
            direction = 1 if fill.side == "BUY" else -1
            quantity = dec(fill.quantity)
            price = dec(fill.price)
            if fill.decision_mid is not None:
                cost_vs_mid += direction * (price - dec(fill.decision_mid)) * quantity
            if fill.decision_quote is not None:
                slippage += direction * (price - dec(fill.decision_quote)) * quantity
            benchmarks = (fill.decision_mid, fill.decision_quote, fill.arrival_quote)
            if all(value is not None for value in benchmarks):
                spread += direction * (dec(fill.decision_quote) - dec(fill.decision_mid)) * quantity
                delay_cost += direction * (dec(fill.arrival_quote) - dec(fill.decision_quote)) * quantity
                walking_cost += direction * (price - dec(fill.arrival_quote)) * quantity
            else:
                missing_benchmark_quantity += quantity

        fees = sum((dec(fill.fee) for fill in self.engine.executions), Decimal(0))
        credits = sum((dec(fill.credit) for fill in self.engine.executions), Decimal(0))
        turnover = sum((dec(fill.notional) for fill in self.engine.executions), Decimal(0))
        return {
            "fees": str(fees),
            "credits": str(credits),
            "execution_cost_vs_mid": str(cost_vs_mid),
            "slippage_vs_decision_quote": str(slippage),
            "turnover": str(turnover),
            "all_in_cost_vs_mid": str(cost_vs_mid + fees - credits),
            "cost_components": {
                "spread": str(spread),
                "delay": str(delay_cost),
                "depth_walking": str(walking_cost),
                "missing_benchmark_quantity": str(missing_benchmark_quantity),
            },
        }

    def _liquidation_estimate(self, position, state):
        """Value only the position quantity covered by fresh executable depth."""
        pool = self.engine.books.get((position.exchange, position.market_id))
        rules = self.engine.config.venues[position.exchange]
        if not pool or pool.executable(self.engine.clock(), rules.max_book_age):
            return {
                "market_id": position.market_id,
                "covered_quantity": 0,
                "net_proceeds": None,
                "complete": False,
            }

        side, bound = canonical_order(position.outcome, "SELL", 0)
        market = state.markets[f"{position.exchange}:{position.market_id}"]
        allocations, remaining = depth_allocations(
            pool, side, bound, position.size, market.quantity_step,
        )
        proceeds = sum((quantity * (price if position.outcome == "YES" else 1 - price)
                        for price, quantity in allocations), Decimal(0))
        estimate = {
            "market_id": position.market_id,
            "outcome": position.outcome,
            "covered_quantity": str(dec(position.size) - remaining),
            "net_proceeds": None,
            "complete": False,
        }
        try:
            fee_model = FeeModel(rules.fee_schedule(position.market_id, self.engine.clock()))
        except ValueError:
            estimate["reason"] = "exit_fee_schedule_unavailable"
            return estimate

        accumulator = FeeAccumulator()
        exit_fees = Decimal(0)
        for price, quantity in allocations:
            outcome_price = price if position.outcome == "YES" else 1 - price
            fee, _ = fee_model.charge(quantity, outcome_price, "SELL", "taker", accumulator)
            exit_fees += fee
        estimate["net_proceeds"] = str(proceeds - exit_fees)
        estimate["complete"] = remaining == 0
        return estimate

    def _account_summaries(self, state):
        accounts = {}
        for portfolio_id, portfolio in state.portfolios.items():
            if not portfolio.paper:
                continue
            liquidation = [self._liquidation_estimate(position, state)
                           for position in portfolio.positions.values()]
            equity = state.risk_managers[portfolio_id].equity(state._snapshot())
            starting_cash = sum(state.config.paper_starting_balances.get(portfolio_id, {}).values())
            fees = sum((dec(fill.fee) for fill in self.engine.executions
                        if self.engine.orders[fill.order_id].approved.portfolio_id == portfolio_id), Decimal(0))
            credits = sum((dec(amount) for amount in portfolio.cash_credits.values()), Decimal(0))
            net_pnl = None
            gross_pnl = None
            drawdown = None
            if equity is not None:
                net_pnl = equity - starting_cash - portfolio.net_cash_flows
                gross_pnl = float(dec(net_pnl) + fees - credits)
                if portfolio.equity_high_water:
                    drawdown = max(0, portfolio.equity_high_water - equity) / portfolio.equity_high_water

            accounts[portfolio_id] = {
                "cash": {venue: balances["USD"].cash for venue, balances in portfolio.balances.items()},
                "equity": equity,
                "realized_pnl": portfolio.realized_pnl,
                "net_pnl": net_pnl,
                "gross_pnl": gross_pnl,
                "fees": str(fees),
                "credits": str(credits),
                "drawdown": drawdown,
                "open_positions": len(portfolio.positions),
                "open_orders": len(portfolio.open_orders),
                "pending_reservations": len(portfolio.reservations),
                "reserved_cash": sum(order.commitment for order in portfolio.pending_orders),
                "liquidation": liquidation,
            }
        return accounts

    def _group_summaries(self):
        summaries = {}
        for batch_id, group in self.engine.groups.items():
            unmatched_seconds = group["unmatched_seconds"]
            if group["unmatched_since"] is not None:
                unmatched_seconds += self.engine.clock() - group["unmatched_since"]
            legs = []
            for order_id in group["ids"]:
                order = self.engine.orders[order_id]
                legs.append({
                    "order_id": order_id,
                    "exchange": order.approved.exchange,
                    "filled_quantity": str(order.filled),
                    "remaining_quantity": str(order.remaining),
                    "status": order.record.status.value,
                })
            summaries[batch_id] = {
                "failed": group["failed"],
                "unmatched_seconds": unmatched_seconds,
                "legs": legs,
            }
        return summaries

    def _venue_summaries(self, orders):
        summaries = {}
        for venue in self.engine.config.venues:
            venue_orders = [order for order in orders if order.approved.exchange == venue]
            summaries[venue] = {
                "orders": len(venue_orders),
                "roles": dict(Counter(fill.role for fill in self.engine.executions if fill.exchange == venue)),
                "time_to_first_fill": [
                    order.first_fill_at - order.submitted_at for order in venue_orders
                    if order.first_fill_at is not None
                ],
                "time_to_final_fill": [
                    order.last_fill_at - order.submitted_at for order in venue_orders
                    if order.filled == dec(order.approved.size)
                ],
            }
        return summaries

    def _strategy_summaries(self, orders):
        summaries = {}
        for strategy in sorted({order.approved.strategy for order in orders}):
            strategy_orders = [order for order in orders if order.approved.strategy == strategy]
            summaries[strategy] = {
                "orders": len(strategy_orders),
                "statuses": dict(Counter(order.record.status.value for order in strategy_orders)),
            }
        return summaries

    def write(self, directory, state):
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        summary = self.summary(state)
        (path / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
        with (path / "executions.jsonl").open("w") as output:
            for fill in self.engine.executions:
                output.write(json.dumps(asdict(fill), sort_keys=True) + "\n")
        target = path / "audit.jsonl"
        if self.engine.audit_path:
            if self.engine.audit_path.resolve() != target.resolve():
                shutil.copyfile(self.engine.audit_path, target)
        else:
            with target.open("w") as output:
                for row in self.engine.audit:
                    output.write(json.dumps(row, default=str, sort_keys=True) + "\n")
        return summary
