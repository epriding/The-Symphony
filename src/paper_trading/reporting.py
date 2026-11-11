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


class BoundedReporter(Reporter):
    """Full-session streams and scalar totals over committed, paged history.

    No legacy historical arrays are built. Pending future marks live in SQLite,
    including marks whose horizon never receives another executable book.
    """
    details = (('executions', 'executions.jsonl'), ('markouts', 'markouts.jsonl'),
               ('order_timings', 'order_timings.jsonl'), ('groups', 'groups.jsonl'))

    def __init__(self, engine, history_store):
        self.engine = engine
        self.history_store = history_store
        self.markouts = {}
        self.pending_markouts = []
        self._observation_signature = None

    def observe(self):
        # The session's bounded async seam invokes observe_async after committing
        # determined executions. Future horizon work remains in SQLite.
        return None

    def _flush_markouts(self, conn, *, force=False):
        store = self.history_store
        if not self.markouts or (not force and len(self.markouts) < store.page_size):
            return
        for identity, row in self.markouts.items():
            if store._put(conn, 'markout_ids', identity, True):
                store._append(conn, 'markouts', row)
            conn.execute('DELETE FROM pending_marks WHERE identity=?', (identity,))
        conn.commit()
        # Pending SQLite rows and these bounded determined rows both survive a
        # failed commit. Retire the hot batch only after a real durable commit.
        self.markouts.clear()

    @staticmethod
    def _markout_row(execution, horizon, identity, observed_at, quotes):
        bid, ask = quotes[execution.outcome]
        if bid is None or ask is None:
            return None
        mid = (dec(bid) + dec(ask)) / 2
        direction = 1 if execution.side == 'BUY' else -1
        return dict(kind='markout', identity=identity,
            execution_id=execution.execution_id, order_id=execution.order_id,
            quantity=execution.quantity, side=execution.side, price=execution.price,
            horizon=horizon, observed_at=observed_at,
            dollars=str(direction * (mid-dec(execution.price))*dec(execution.quantity)), missing=False)

    async def observe_async(self, *, force=False):
        store = self.history_store
        now = self.engine.clock()
        books = []
        for (exchange, market), pool in self.engine.books.items():
            if pool.executable(now, self.engine.config.venues[exchange].max_book_age):
                continue
            if pool.quote('SELL') is None or pool.quote('BUY') is None:
                continue
            # Copy only the current finite quote set. The worker must not read
            # mutable book state while public input advances on the loop.
            quotes = {outcome: (pool.quote('SELL', outcome), pool.quote('BUY', outcome))
                      for outcome in ('YES', 'NO')}
            books.append((exchange, market, min(now, pool.received_at), quotes))
        # These immutable executions already have finite engine admission bounds
        # and stay authoritative until their own archive commit. Observing them
        # now does not require an execution commit for every individual fill.
        hot_executions = tuple(self.engine.executions)
        if not books and not self.markouts:
            self._observation_signature = None
            return
        signature = (
            tuple((exchange, market, cutoff, tuple(quotes.items()))
                  for exchange, market, cutoff, quotes in books),
            tuple((row.execution_id, row.order_id, row.exchange, row.market_id,
                   row.outcome, row.side, row.quantity, row.price, row.timestamp)
                  for row in hot_executions),
            tuple(self.engine.config.markout_horizons),
        )
        if (not force and len(self.markouts) < store.page_size and
                self._observation_signature == (signature, store._change_count)):
            return
        # Failed work must remain retryable even with unchanged books and fills.
        self._observation_signature = None

        def commit(conn):
            self._flush_markouts(conn)
            current_books = {(exchange, market): (cutoff, quotes)
                             for exchange, market, cutoff, quotes in books}
            for execution in hot_executions:
                current = current_books.get((execution.exchange, execution.market_id))
                if current is None:
                    continue
                cutoff, quotes = current
                for horizon in self.engine.config.markout_horizons:
                    if execution.timestamp + horizon > cutoff:
                        continue
                    identity = f'{execution.execution_id}:{horizon}'
                    if (identity in self.markouts or
                            store._get(conn, 'markout_ids', identity, False)):
                        continue
                    row = self._markout_row(execution, horizon, identity, now, quotes)
                    if row is not None:
                        self.markouts[identity] = row
                        self._flush_markouts(conn)
            for exchange, market, cutoff, quotes in books:
                after_due, after_id = -float('inf'), ''
                while True:
                    store._check_deadline()
                    rows = conn.execute(
                        'SELECT identity,due,value FROM pending_marks WHERE exchange=? AND market=? AND due<=? AND (due>? OR (due=? AND identity>?)) ORDER BY due,identity LIMIT ?',
                        (exchange, market, cutoff, after_due, after_due, after_id, store.page_size)).fetchmany(store.page_size)
                    if not rows:
                        break
                    after_id, after_due = rows[-1][0], rows[-1][1]
                    for identity, due, raw in rows:
                        if identity in self.markouts:
                            continue
                        value = store._decode(raw)
                        execution, horizon = value['execution'], value['horizon']
                        row = self._markout_row(execution, horizon, identity, now, quotes)
                        if row is not None:
                            self.markouts[identity] = row
                            self._flush_markouts(conn)
            # A partial page remains bounded in RAM while its original pending
            # rows remain durable. Queries/finalization force its real commit.
            self._flush_markouts(conn, force=force)
            # Return this operation's boundary. A writer queued after us can
            # advance the store counter before the event loop resumes.
            return conn.total_changes
        observed_changes = await store.run(commit)
        self._observation_signature = (signature, observed_changes)

    @staticmethod
    def _execution_totals_value(aggregate):
        totals = {key: Decimal(0) for key in ('fees', 'credits', 'turnover',
            'execution_cost_vs_mid', 'slippage_vs_decision_quote', 'spread', 'delay',
            'depth_walking', 'missing_benchmark_quantity')}
        totals.update(aggregate.get('totals', {}))
        result = {key: str(totals[key]) for key in ('fees', 'credits', 'turnover', 'execution_cost_vs_mid', 'slippage_vs_decision_quote')}
        result['all_in_cost_vs_mid'] = str(totals['execution_cost_vs_mid'] + totals['fees'] - totals['credits'])
        result['cost_components'] = {key: str(totals[key]) for key in ('spread', 'delay', 'depth_walking', 'missing_benchmark_quantity')}
        return result, aggregate.get('accounts', {}), aggregate.get('roles', {}), aggregate.get('executions', 0)

    def _bounded_accounts(self, state, execution_accounts, credits):
        accounts = {}
        snapshot = state._snapshot()
        for pid, portfolio in state.portfolios.items():
            if not portfolio.paper:
                continue
            fees = execution_accounts.get(pid, {}).get('fees', Decimal(0))
            credit = credits.get(pid, Decimal(0))
            equity = state.risk_managers[pid].equity(snapshot)
            starting = sum((dec(v) for v in state.config.paper_starting_balances.get(pid, {}).values()), Decimal(0))
            net = None if equity is None else dec(equity) - starting - dec(portfolio.net_cash_flows)
            drawdown = None
            if equity is not None and portfolio.equity_high_water:
                drawdown = max(0, portfolio.equity_high_water - equity) / portfolio.equity_high_water
            accounts[pid] = dict(cash={venue: balances['USD'].cash for venue, balances in portfolio.balances.items()},
                equity=equity, realized_pnl=portfolio.realized_pnl,
                net_pnl=None if net is None else str(net),
                gross_pnl=None if net is None else str(net + fees - credit),
                fees=str(fees), credits=str(credit), drawdown=drawdown,
                open_positions=len(portfolio.positions), open_orders=len(portfolio.open_orders),
                pending_reservations=len(portfolio.reservations),
                reserved_cash=sum(order.commitment for order in portfolio.pending_orders),
                liquidation=[self._liquidation_estimate(position, state) for position in portfolio.positions.values()])
        return accounts

    def _summary_data(self, conn):
        store = self.history_store
        self._flush_markouts(conn, force=True)
        # Every historical total is updated atomically with its underlying row.
        # Finalization reads a bounded number of aggregates, not past fills.
        return dict(orders=store._get(conn, '_summary', 'orders', {}),
            executions=store._get(conn, '_summary', 'executions', {}),
            credits={pid: store._get(conn, '_summary', ('credits', pid), Decimal(0))
                     for pid in self.engine.coordinator.state.portfolios},
            missing=conn.execute('SELECT COUNT(*) FROM pending_marks').fetchone()[0],
            markouts=store._count(conn, 'markouts'), details=store._detail_references(conn))

    def _summary_value(self, state, data):
        orders = data['orders']
        execution, account, roles, execution_count = self._execution_totals_value(data['executions'])
        venues = {venue: dict(orders=orders.get('venues', {}).get(venue, 0), roles=roles.get(venue, {}))
                  for venue in self.engine.config.venues}
        strategies = {name: dict(orders=value['orders'], statuses={key: count for key, count in value['statuses'].items() if count})
                      for name, value in orders.get('strategies', {}).items()}
        return dict(session_id=self.engine.config.session_id, assumptions=self.engine.config.assumptions,
            counts={key: count for key, count in orders.get('counts', {}).items() if count},
            submitted=orders.get('submitted', 0), full_fills=orders.get('full', 0),
            partial_fills=orders.get('partial', 0), zero_fills=orders.get('zero', 0),
            executions=execution_count, **execution,
            accounts=self._bounded_accounts(state, account, data['credits']),
            diagnostics=dict(self.engine.diagnostics), markouts=dict(scope='full-session', records=data['markouts']),
            missing_markouts=data['missing'], peak_pending_events=self.engine.scheduler.peak_pending,
            processing_wall_time=self.engine.scheduler.processing_metrics(),
            undelivered_reports=len(self.engine.outbox) + len(self.engine.credit_outbox),
            audit_window_events=len(self.engine.audit), audit_total_events=sum(self.engine.diagnostics.values()),
            full_audit_streamed=self.engine.audit_path is not None, by_venue=venues,
            by_strategy=strategies, by_time_in_force={key: count for key, count in orders.get('tif', {}).items() if count},
            history=dict(scope='full-session', schema_version=self.history_store.schema_version, database='history.sqlite3',
                hot_view_scope='active state plus configured terminal history window', details=data['details']))

    def summary(self, state):
        return self._summary_value(state, self.history_store._call(self._summary_data))

    async def summary_async(self, state):
        await self.engine.compact(force=True)
        await self.observe_async(force=True)
        return self._summary_value(state, await self.history_store.run(self._summary_data))

    def _finalize_details(self, conn):
        store = self.history_store
        self._flush_markouts(conn, force=True)
        for oid, order in self.engine.orders.items():
            if store._put(conn, 'timing_ids', oid, True):
                store._append(conn, 'order_timings', self.engine._timing(order))
        for bid, group in self.engine.groups.items():
            if store._put(conn, 'group_ids', bid, True):
                store._append(conn, 'groups', self.engine._group_detail(bid, group))
        cursor = conn.execute('SELECT identity,value FROM pending_marks ORDER BY due,identity')
        while True:
            rows = cursor.fetchmany(store.page_size)
            if not rows:
                break
            store._check_deadline()
            for identity, raw in rows:
                value = store._decode(raw)
                execution = value['execution']
                if store._put(conn, 'markout_ids', identity, True):
                    store._append(conn, 'markouts', dict(kind='markout', identity=identity,
                        execution_id=execution.execution_id, horizon=value['horizon'],
                        observed_at=None, dollars=None, missing=True))
        conn.commit()

    def _copy_details(self, conn, path):
        path.mkdir(parents=True, exist_ok=True)
        if path.resolve() == self.history_store.path.parent.resolve():
            return
        # Alternate destinations remain streamed and deadline checked. Runtime
        # finalization uses the already continuously written session directory.
        for _, filename in self.details:
            with (self.history_store.path.parent / filename).open('rb') as source, (path / filename).open('wb') as output:
                while chunk := source.read(self.history_store.max_record_bytes):
                    self.history_store._check_deadline()
                    output.write(chunk)

    def write(self, directory, state):
        path = Path(directory)
        store = self.history_store
        store._call(self._finalize_details)
        store._call(lambda conn: self._copy_details(conn, path))
        summary = self.summary(state)
        encoded = json.dumps(summary, indent=2, allow_nan=False) + '\n'
        store._call(lambda conn: (path / 'summary.json').write_text(encoded))
        return summary

    async def write_async(self, directory, state):
        path = Path(directory)
        store = self.history_store
        await self.engine.compact(force=True)
        await self.observe_async(force=True)
        await store.run(self._finalize_details)
        await store.run(lambda conn: self._copy_details(conn, path))
        summary = await self.summary_async(state)
        encoded = json.dumps(summary, indent=2, allow_nan=False) + '\n'
        await store.run(lambda conn: (path / 'summary.json').write_text(encoded))
        return summary
