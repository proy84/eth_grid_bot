"""
main.py

Orchestrator for the Only-Short grid bot. Wires together `exchange.py`
(I/O), `strategy.py` (pure grid/Fibonacci decision logic), `fees.py` (PnL &
break-even math), `analytics.py` (cycle history) and `data_exporter.py`
(dashboard state).

Two independent loops:
  - Grid scheduler: in normal (production) mode, wakes up once per
    `config.timeframe` interval, 1 second after the boundary (e.g. "5m" ->
    XX:05:01, XX:10:01 UTC; "1h" -> HH:00:01 UTC), fetches the just-CLOSED
    candle and ALWAYS executes one grid order sized off the current range's
    distance from Range 0 -- there is no "idle" outcome, even if price
    stayed exactly where it was last candle. In `stress_test.enabled` mode
    (see below) this scheduler is replaced entirely by a variable-interval,
    tick-driven one.
  - Tick loop (every `tick_poll_interval_sec`, default 2s): recomputes real
    net PnL, checks the fixed take-profit target, exports live state for
    the frontend, and (stress-test mode only) checks whether the position's
    average entry price has caught up to the market's current grid band.

STRESS TEST MODE (`config.stress_test.enabled`): a deliberate, reversible
override of the production scheduling model for high-frequency demo
testing. Three changes apply only while enabled:
  1. All ranges evaluate at a flat `stress_test.base_interval_sec` cadence
     (no per-range table) -- UNLESS RSI tick mode is active (see below), in
     which case evaluation fires every `stress_test.tick_mode_interval_sec`
     instead. Evaluation itself reads the live mark price, not a closed
     candle, since sub-minute cadences have no matching OHLCV timeframe.
  2. `stress_test.unlimited_fib_level` removes the `risk.max_fib_level`
     safety cap for grid sizing -- the only limit left is the exchange
     itself rejecting an order for insufficient margin, which the existing
     error handling in `_execute_planned_order` already absorbs gracefully.
  3. RSI trigger (`_maybe_update_rsi` / `_maybe_handle_rsi_tick_mode`): an
     RSI(`rsi_period`) reading on `rsi_timeframe` crossing UP through
     `rsi_overbought_threshold` (a fresh crossing from below -- staying
     overbought does not re-trigger it) switches on tick-mode execution on
     the CURRENT range. Grid orders at that cadence pull the position's
     average entry price (break-even) up with every fill; once break-even
     catches up to the live market's grid band, tick mode turns off
     immediately, Range 0 re-anchors there mid-cycle (no Take Profit
     needed), and the base cadence resumes -- a fresh RSI crossing is
     required to re-arm it.
Disabling `stress_test.enabled` restores the exact production behavior
(fixed timeframe, candle-close evaluation, capped Fibonacci level, no RSI
trigger) with no other code path affected.

Exit is a FIXED take profit, not a trailing stop: the position closes at
market the instant `mark_price` crosses `take_profit.net_breakeven_pct`%
below the current Break-Even NETTO price (see `fees.compute_breakeven_prices`
-- already fee/funding-adjusted). No retracement wait, no peak tracking:
this avoids giving back gains while waiting to confirm a peak, which is what
a trailing stop's callback window does. `net_breakeven_pct` is a real PRICE
percentage, not a leveraged-margin percentage.

Grid-triggered entries and take-profit-triggered closes both mutate the
same position state, and each involves at least one `await` (an exchange
call) -- without protection, a grid evaluation firing in the same instant as
a take-profit close could interleave and corrupt the PnL bookkeeping (the
close would execute against the exchange's real, up-to-date position while
our recorded gross/fees were snapshotted from a stale, smaller position).
`self._position_lock` (asyncio.Lock) serializes every position mutation --
`_execute_planned_order` and the close+record portion of `_take_profit` --
so the two flows can never interleave.

Two situations bypass the candle-close wait and fire a market order
immediately, per spec:
  1. Bot startup with no open position on the symbol -> immediate base
     SHORT (Fibonacci(1) * BASE_NOTIONAL_USDT) at market, anchoring Range 0
     to the current price.
  2. A cycle closing via the take-profit target -> immediate re-open of the
     new base SHORT at market in the new Range 0.
Every subsequent order (staying in Range 0, advancing to Range +1/+2/...,
or a downside Range Shift) is decided strictly on candle close.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from typing import List

from analytics import AnalyticsEngine, CycleRecord, OrderRecord
from data_exporter import DataExporter, LiveState
from exchange import ExchangeClient
from fees import EntryFill, FeeEngine, FeeSchedule, FundingPayment, compute_breakeven_prices, compute_net_pnl
from strategy import (
    PlannedOrder,
    PositionManager,
    RangeGrid,
    StrategyConfig,
    compute_rsi,
    evaluate_grid_close,
    fibonacci,
)

logger = logging.getLogger("eth_grid_bot.main")

BOT_VERSION = "1.1"
CONFIG_PATH = "config.json"
CANDLE_CLOSE_OFFSET_SEC = 1.0  # evaluate the grid 1s after each timeframe boundary
RSI_POLL_INTERVAL_SEC = 15.0  # how often the tick loop re-checks for a newly-closed RSI candle


def parse_timeframe_seconds(timeframe: str) -> int:
    """Converts a CCXT-style timeframe string ('5m', '1h', '1d', ...) to seconds."""
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    if len(timeframe) < 2 or timeframe[-1] not in units:
        raise ValueError(f"Unsupported timeframe format: {timeframe!r}")
    try:
        value = int(timeframe[:-1])
    except ValueError as exc:
        raise ValueError(f"Unsupported timeframe format: {timeframe!r}") from exc
    if value <= 0:
        raise ValueError(f"Timeframe must be positive: {timeframe!r}")
    return value * units[timeframe[-1]]


class GridBotOrchestrator:
    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self.timeframe_sec = parse_timeframe_seconds(cfg.timeframe)

        self.exchange = ExchangeClient(cfg)
        # base_price=0.0 here is a throwaway placeholder: _bootstrap_position() always
        # overwrites it from the live market price (or an existing position's entry
        # price) before the grid is ever evaluated. There is no config-driven default --
        # the grid has no fixed price list, only a step percentage.
        self.grid = RangeGrid(base_price=0.0, step_pct=cfg.grid_step_pct)
        self.position = PositionManager()
        self.fee_engine = FeeEngine(schedule=FeeSchedule(taker_rate=cfg.taker_rate, maker_rate=cfg.maker_rate))
        self.analytics = AnalyticsEngine(cfg.trade_history_path)
        self.exporter = DataExporter(cfg.state_export_path, self.analytics)

        self.base_notional_usdt = cfg.base_notional_usdt
        self.cycle_id = self.analytics.summary().completed_cycles + 1
        self.cycle_start_ts_ms = int(time.time() * 1000)
        self.cycle_orders: List[OrderRecord] = []

        self._last_funding_poll_ts = 0.0
        self._stop_event = asyncio.Event()
        self._position_lock = asyncio.Lock()
        # Updated by the tick loop every `tick_poll_interval_sec`; used by the
        # stress-test scheduler to pick the next evaluation delay without an
        # extra dedicated price fetch. Only ever a few seconds stale, which is
        # immaterial against the coarsest cadence (600s).
        self._last_mark_price: float | None = None
        # RSI trigger state (stress-test only). `_current_rsi` is the last
        # computed reading; `_rsi_last_candle_ts` gates recomputation to once
        # per newly-closed rsi_timeframe candle. `_rsi_tick_mode_active`
        # becomes True only on an UPWARD CROSSING of the overbought threshold
        # (previous reading < threshold <= new reading) -- staying overbought
        # across candles does not re-trigger it, only a fresh crossing does.
        # It turns back off (and requires a fresh crossing to re-arm) once the
        # position's break-even price has caught up to the market's current
        # grid band, or on any other base_price change (downside shift, TP
        # reset).
        self._current_rsi: float | None = None
        self._rsi_last_candle_ts: int | None = None
        self._last_rsi_poll_ts = 0.0
        self._rsi_tick_mode_active = False

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        logger.info("[INFO] Avvio Bot Trading - v%s", BOT_VERSION)
        await self.exchange.setup()
        await self._bootstrap_position()
        logger.info("Bot running: cycle_id=%d range_base_price=%.4f grid_step_pct=%.3f%% base_notional=%.2f "
                    "timeframe=%s (%ds)",
                    self.cycle_id, self.grid.base_price, self.cfg.grid_step_pct * 100.0, self.base_notional_usdt,
                    self.cfg.timeframe, self.timeframe_sec)
        if self.cfg.stress_test_enabled:
            logger.warning(
                "[stress-test] STRESS TEST MODE ENABLED: base cadence %.0fs for all ranges; RSI(%d) on %s >= %.1f "
                "triggers tick mode (every %.0fs) until Break-Even catches up to the market's grid band; "
                "Neutral Zone %s (%.2f%% around Break-Even LORDO, Fibonacci entries only -- Range Shifts always "
                "proceed); Fibonacci cap %s. Set stress_test.enabled=false in config.json to return to "
                "production behavior.",
                self.cfg.stress_test_base_interval_sec, self.cfg.stress_test_rsi_period,
                self.cfg.stress_test_rsi_timeframe, self.cfg.stress_test_rsi_overbought_threshold,
                self.cfg.stress_test_tick_mode_interval_sec,
                "ENABLED" if self.cfg.stress_test_neutral_zone_enabled else "DISABLED",
                self.cfg.stress_test_neutral_zone_percent,
                "DISABLED" if self.cfg.stress_test_unlimited_fib_level else self.cfg.max_fib_level,
            )
        await asyncio.gather(self._grid_scheduler_loop(), self._tick_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        await self.exchange.close()

    # -- startup bootstrap (rule 3: immediate first entry) -------------------

    async def _bootstrap_position(self) -> None:
        """No candle wait for the very first Range 0 entry: if the exchange
        reports no open position on the symbol, open the base SHORT right
        now at market and anchor Range 0 to the current price."""
        positions = await self.exchange.fetch_open_positions()
        existing = next((p for p in positions if p.qty > 0), None)

        if existing is not None:
            logger.warning(
                "Existing open position detected on %s at startup (qty=%.6f, avg_entry=%.2f). "
                "Reconciling it as a single synthetic entry -- per-level Fibonacci history cannot "
                "be recovered from the exchange after a restart.",
                self.cfg.symbol, existing.qty, existing.entry_price,
            )
            self.position.add_entry(EntryFill(
                price=existing.entry_price, qty=existing.qty,
                notional_usdt=existing.entry_price * existing.qty,
                taker_fee_usdt=0.0, fib_level=0, timestamp_ms=int(time.time() * 1000),
            ))
            self.grid.full_reset(existing.entry_price)
            logger.info("Range 0 anchored to reconciled entry price %.4f", existing.entry_price)
            return

        mark_price = await self.exchange.fetch_mark_price()
        self.grid.full_reset(mark_price)
        logger.info("No open position found on %s. Opening the base SHORT immediately at market "
                    "(Range 0, Fibonacci 1), base_price=%.4f.",
                    self.cfg.symbol, mark_price)
        await self._execute_immediate_base_order(kind="range_zero_immediate")

    async def _execute_immediate_base_order(self, kind: str) -> None:
        """Fires the Range 0 base SHORT (Fibonacci(1) * BASE_NOTIONAL_USDT) at market,
        bypassing the candle-close wait. Used at startup and right after a Take Profit reset."""
        try:
            mark_price = await self.exchange.fetch_mark_price()
        except Exception:
            logger.exception("Failed to fetch mark price for the immediate base order; "
                              "the position stays flat until the next tick can retry implicitly.")
            return

        order = PlannedOrder(level_index=0, range_offset=0, fib_n=1,
                              notional_usdt=fibonacci(1) * self.base_notional_usdt, kind=kind)
        await self._execute_planned_order(order, mark_price, int(time.time() * 1000))

    # -- grid scheduling (rule 2/3: increments & shifts wait for candle close, --
    # -- unless stress_test.enabled overrides with a variable tick-driven cadence) -

    def _seconds_until_next_boundary(self) -> float:
        now = time.time()
        next_boundary = (math.floor(now / self.timeframe_sec) + 1) * self.timeframe_sec
        target = next_boundary + CANDLE_CLOSE_OFFSET_SEC
        return max(0.0, target - now)

    async def _wait_interruptible(self, timeout_sec: float) -> bool:
        """Like `_wait_or_stop`, but for the stress-test BASE cadence only:
        polls in 1s steps so that if RSI tick mode activates mid-wait (see
        `_maybe_update_rsi`), the scheduler breaks out immediately instead of
        finishing out a stale up-to-5-minute wait -- the whole point of the
        RSI trigger is a fast reaction to an overbought reading. Returns True
        only if shutdown was requested; a tick-mode interruption returns
        False just like a normal timeout, so the caller re-decides cadence
        from scratch either way."""
        elapsed = 0.0
        step = 1.0
        while elapsed < timeout_sec:
            this_step = min(step, timeout_sec - elapsed)
            if await self._wait_or_stop(this_step):
                return True
            elapsed += this_step
            if self._rsi_tick_mode_active:
                return False
        return False

    async def _grid_scheduler_loop(self) -> None:
        while not self._stop_event.is_set():
            if self.cfg.stress_test_enabled:
                if self._rsi_tick_mode_active:
                    sleep_sec = self.cfg.stress_test_tick_mode_interval_sec
                    logger.info("[stress-test] Next grid evaluation in %.1fs (RSI tick mode).", sleep_sec)
                    if await self._wait_or_stop(sleep_sec):
                        break
                else:
                    sleep_sec = self.cfg.stress_test_base_interval_sec
                    logger.info("[stress-test] Next grid evaluation in %.1fs (base cadence).", sleep_sec)
                    if await self._wait_interruptible(sleep_sec):
                        break
            else:
                sleep_sec = self._seconds_until_next_boundary()
                next_run = datetime.fromtimestamp(time.time() + sleep_sec, tz=timezone.utc)
                logger.info("Next grid evaluation in %.1fs, at %s (timeframe=%s).",
                            sleep_sec, next_run.strftime("%Y-%m-%d %H:%M:%S UTC"), self.cfg.timeframe)
                if await self._wait_or_stop(sleep_sec):
                    break
            await self._run_grid_evaluation()

    async def _run_grid_evaluation(self) -> None:
        if self.cfg.stress_test_enabled:
            try:
                price = await self.exchange.fetch_mark_price()
            except Exception:
                logger.exception("Failed to fetch mark price for stress-test grid evaluation; skipping this window.")
                return
            ts_ms = int(time.time() * 1000)
            logger.info("[stress-test] Tick evaluation: mark=%.4f", price)
        else:
            try:
                candle = await self.exchange.fetch_last_closed_candle()
            except Exception:
                logger.exception("Failed to fetch the last closed candle; skipping this evaluation window.")
                return
            price, ts_ms = candle.close, candle.timestamp_ms
            logger.info("Candle closed (%s): ts=%d close=%.2f", self.cfg.timeframe, ts_ms, price)

        max_fib_level = None if (self.cfg.stress_test_enabled and self.cfg.stress_test_unlimited_fib_level) \
            else self.cfg.max_fib_level
        order = evaluate_grid_close(price, self.grid, self.base_notional_usdt, max_fib_level)
        if order.kind == "range_shift_base":
            # base_price just moved to a new (lower) anchor -- any catch-up
            # tracked against the OLD anchor is no longer meaningful. A fresh
            # RSI crossing is required to re-arm tick mode. Range shifts are
            # NEVER suspended by the Neutral Zone (see below) -- the grid must
            # keep tracking price freely even while Fibonacci buys are paused.
            self._rsi_tick_mode_active = False
        elif self.cfg.stress_test_enabled and self.cfg.stress_test_neutral_zone_enabled:
            if self._maybe_suspend_for_neutral_zone(price):
                return
        logger.info("Grid evaluation: close=%.4f base_price=%.4f offset=%d -> fib_n=%d (%s)",
                    price, self.grid.base_price, order.range_offset, order.fib_n, order.kind)
        await self._execute_planned_order(order, price, ts_ms)

    def _maybe_suspend_for_neutral_zone(self, mark_price: float) -> bool:
        """Neutral Zone (stress-test only): pauses Fibonacci accumulation
        entries -- never Range Shifts, see caller -- while the market price
        sits within `stress_test_neutral_zone_percent`% of the Break-Even
        LORDO (`avg_entry_price`; the Break-Even NETTO stays reserved for the
        take-profit target). Buying more this close to break-even is just fee
        churn with no strategic benefit. Bypassed unconditionally whenever the
        current RSI(5m) reading is >= the overbought threshold -- a direct
        check on the live RSI value, independent of whether tick mode is
        presently active or already turned off after a catch-up -- so a
        genuinely overbought market can still accumulate right through the
        zone. Returns True if the caller should skip execution this round."""
        if self.position.is_flat:
            return False
        avg_entry = self.position.avg_entry_price
        if avg_entry <= 0:
            return False
        dist_pct = abs(mark_price - avg_entry) / avg_entry * 100.0
        if dist_pct >= self.cfg.stress_test_neutral_zone_percent:
            return False

        if self._rsi_overbought_now():
            logger.info(
                "Post Catch-up: Accumulo normale %.0fs con RSI >= %.1f (Neutral Zone bypassata, "
                "distanza Break-Even=%.3f%% < %.2f%%).",
                self.cfg.stress_test_base_interval_sec, self.cfg.stress_test_rsi_overbought_threshold,
                dist_pct, self.cfg.stress_test_neutral_zone_percent,
            )
            return False

        logger.info(
            "Neutral Zone attiva (In Pausa): distanza Break-Even=%.3f%% < %.2f%% -- entrata Fibonacci sospesa.",
            dist_pct, self.cfg.stress_test_neutral_zone_percent,
        )
        return True

    def _rsi_overbought_now(self) -> bool:
        return (self._current_rsi is not None
                and self._current_rsi >= self.cfg.stress_test_rsi_overbought_threshold)

    async def _execute_planned_order(self, order: PlannedOrder, ref_price: float, ts_ms: int) -> None:
        """Holds `_position_lock` for its entire body so a concurrent take-profit
        close can never observe a half-added entry (or vice versa)."""
        async with self._position_lock:
            qty = self.exchange.compute_qty_from_notional(order.notional_usdt, ref_price)
            if qty <= 0:
                logger.warning("Computed qty <= 0 for notional=%.2f @ price=%.2f; skipping %s order.",
                                order.notional_usdt, ref_price, order.kind)
                return

            try:
                filled = await self.exchange.place_market_short(qty)
            except Exception:
                logger.exception("Failed to execute %s order (fib_n=%d, notional=%.2f)",
                                  order.kind, order.fib_n, order.notional_usdt)
                return

            fee = self.fee_engine.taker_fee_for_notional(filled.notional_usdt)
            self.position.add_entry(EntryFill(
                price=filled.price, qty=filled.qty, notional_usdt=filled.notional_usdt,
                taker_fee_usdt=fee, fib_level=order.fib_n, timestamp_ms=ts_ms,
            ))
            self.cycle_orders.append(OrderRecord(
                timestamp_ms=ts_ms, fib_level=order.fib_n, kind=order.kind,
                price=filled.price, qty=filled.qty, notional_usdt=filled.notional_usdt, fee_usdt=fee,
            ))
            logger.info("Grid order filled: kind=%s fib_n=%d qty=%.6f price=%.2f notional=%.2f fee=%.4f",
                        order.kind, order.fib_n, filled.qty, filled.price, filled.notional_usdt, fee)

    # -- tick loop: real-time net PnL + fixed take-profit target (rules 3, 8, 9) -

    async def _tick_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._run_tick()
            except Exception:
                logger.exception("Error during tick evaluation")
            if await self._wait_or_stop(self.cfg.tick_poll_interval_sec):
                break

    async def _run_tick(self) -> None:
        await self._maybe_poll_funding()
        mark_price = await self.exchange.fetch_mark_price()
        self._last_mark_price = mark_price

        if self.cfg.stress_test_enabled:
            await self._maybe_update_rsi(mark_price)
            await self._maybe_handle_rsi_tick_mode(mark_price)

        margin_used = (self.position.total_notional / self.cfg.leverage) if not self.position.is_flat else 0.0
        breakdown = compute_net_pnl(
            avg_entry_price=self.position.avg_entry_price,
            mark_price=mark_price,
            qty=self.position.total_qty,
            open_fees_usdt=self.position.total_open_fees,
            fee_engine=self.fee_engine,
            margin_used_usdt=margin_used,
        )
        self._export_state(mark_price, breakdown, margin_used)

        if self.position.is_flat:
            return

        target_price = self._take_profit_target_price()
        if target_price is not None and mark_price <= target_price:
            logger.info("Take profit target reached: mark=%.4f <= target=%.4f (%.2f%% below Break-Even NETTO)",
                        mark_price, target_price, self.cfg.take_profit_net_breakeven_pct)
            await self._take_profit(mark_price)

    def _take_profit_target_price(self) -> float | None:
        """SHORT profits as price falls, so the target sits
        `take_profit_net_breakeven_pct`% BELOW the net breakeven price."""
        _, be_net = compute_breakeven_prices(self.position.entries, self.fee_engine)
        if be_net <= 0:
            return None
        return be_net * (1.0 - self.cfg.take_profit_net_breakeven_pct / 100.0)

    def _has_real_breakeven_gap(self, mark_price: float) -> bool:
        """True only if the market has genuinely pulled AHEAD of (above) the
        position's Break-Even price -- i.e. the SHORT is currently underwater
        and there is real ground to recover, which is the only case tick
        mode exists to accelerate. Deliberately one-directional (`>`, not
        `!=`): if Break-Even already sits ABOVE the market (position already
        in profit), an RSI crossing must NOT arm tick mode -- that would just
        pile more short exposure onto an already-winning position with no
        risk-based rationale, purely because RSI and a band mismatch happened
        to coincide. Also covers the flat-position/standing-start case (no
        gap, nothing to catch up from), and the redundant-reactivation case
        an RSI dip-then-recross could otherwise cause when Break-Even has
        already caught up. Mirrors the analogous arm guard the offset-based
        re-anchor mechanism used."""
        if self.position.is_flat:
            return False
        avg_entry = self.position.avg_entry_price
        if avg_entry <= 0:
            return False
        mark_offset = self.grid.classify_offset(mark_price)
        breakeven_offset = self.grid.classify_offset(avg_entry)
        return mark_offset > breakeven_offset

    async def _maybe_update_rsi(self, mark_price: float) -> None:
        """Recomputes RSI at most once per `RSI_POLL_INTERVAL_SEC`, and only
        actually updates state when that poll lands on a NEWLY closed
        `stress_test_rsi_timeframe` candle (`_rsi_last_candle_ts` gate) --
        the underlying candle hasn't changed in between, so recomputing more
        often would just repeat the same reading for extra API calls.

        Tick mode arms on an UPWARD CROSSING of the overbought threshold
        (previous reading < threshold <= new reading), never on merely
        *being* overbought -- so staying overbought across consecutive
        candles does not keep re-triggering it; only a fresh crossing from
        below does. A crossing is also ignored (no arm, no order) if
        Break-Even and market price are already in the same grid band at
        that moment -- see `_has_real_breakeven_gap`."""
        now = time.time()
        if now - self._last_rsi_poll_ts < RSI_POLL_INTERVAL_SEC:
            return
        self._last_rsi_poll_ts = now

        try:
            candles = await self.exchange.fetch_closed_candles(
                self.cfg.stress_test_rsi_timeframe, self.cfg.stress_test_rsi_period * 4)
        except Exception:
            logger.exception("Failed to fetch candles for RSI computation; keeping last known RSI.")
            return
        if not candles:
            return

        latest_ts = candles[-1].timestamp_ms
        if latest_ts == self._rsi_last_candle_ts:
            return
        self._rsi_last_candle_ts = latest_ts

        rsi = compute_rsi([c.close for c in candles], self.cfg.stress_test_rsi_period)
        if rsi is None:
            return

        prev = self._current_rsi
        self._current_rsi = rsi
        threshold = self.cfg.stress_test_rsi_overbought_threshold
        crossed_up = prev is not None and prev < threshold <= rsi

        if crossed_up and not self._rsi_tick_mode_active:
            if self._has_real_breakeven_gap(mark_price):
                self._rsi_tick_mode_active = True
                logger.info("RSI >= %.1f: Tick Mode Attivato (RSI %s = %.1f)",
                            threshold, self.cfg.stress_test_rsi_timeframe, rsi)
            else:
                logger.info(
                    "[rsi-trigger] RSI %s = %.1f >= %.1f crossing ignored: Break-Even already in the same "
                    "grid band as the market (no real gap to catch up) -- tick mode not armed.",
                    self.cfg.stress_test_rsi_timeframe, rsi, threshold,
                )
        else:
            logger.info("[rsi-trigger] RSI(%d, %s)=%.2f (threshold=%.1f)",
                        self.cfg.stress_test_rsi_period, self.cfg.stress_test_rsi_timeframe, rsi, threshold)

    async def _maybe_handle_rsi_tick_mode(self, mark_price: float) -> None:
        """Stress-test only, deactivation half of the RSI trigger. While tick
        mode is active, grid orders fire every `stress_test_tick_mode_interval_sec`
        on the CURRENT range, which pulls the position's average entry price
        (break-even) up with every fill. Once break-even has caught up to the
        live market's grid band, there is nothing left to accelerate: turn
        tick mode off immediately, re-anchor Range 0 right here (mid-cycle, no
        Take Profit needed), and fall back to the base cadence. A fresh RSI
        crossing is required to re-arm (see `_maybe_update_rsi`) -- this
        method never turns tick mode back ON, only off."""
        if not self._rsi_tick_mode_active:
            return
        if self.position.is_flat:
            return
        avg_entry = self.position.avg_entry_price
        if avg_entry <= 0:
            return

        mark_offset = self.grid.classify_offset(mark_price)
        breakeven_offset = self.grid.classify_offset(avg_entry)
        if mark_offset != breakeven_offset:
            return

        old_base = self.grid.base_price
        self.grid.full_reset(mark_price)
        self._rsi_tick_mode_active = False
        logger.info(
            "Break-Even raggiunto: Tick Mode Disattivato -> Nuovo Range 0 (avg_entry=%.4f mark=%.4f, both in "
            "offset band %d relative to old base_price=%.4f) -> base_price=%.4f, frequency back to %.0fs.",
            avg_entry, mark_price, mark_offset, old_base, self.grid.base_price,
            self.cfg.stress_test_base_interval_sec,
        )

    async def _maybe_poll_funding(self) -> None:
        if self.position.is_flat:
            return
        now = time.time()
        if now - self._last_funding_poll_ts < self.cfg.funding_poll_interval_sec:
            return
        self._last_funding_poll_ts = now

        try:
            rate = await self.exchange.fetch_current_funding_rate()
        except Exception:
            logger.exception("Failed to fetch current funding rate")
            return

        self.fee_engine.record_funding(FundingPayment(
            timestamp_ms=int(now * 1000), funding_rate=rate, notional_usdt=self.position.total_notional,
        ))
        logger.info("Funding recorded: rate=%.6f notional=%.2f cashflow=%.4f",
                    rate, self.position.total_notional, rate * self.position.total_notional)

    # -- take profit / cycle reset (rule 13) ----------------------------------

    async def _take_profit(self, mark_price: float) -> None:
        """Holds `_position_lock` through snapshot -> exchange close -> cycle
        recording -> state reset, so a concurrent grid evaluation can never add
        an entry the close's PnL calculation doesn't account for (the bug that
        previously made our recorded net_pnl diverge from Bybit's own realized
        P&L whenever a candle-close evaluation landed at the same instant as a
        take-profit trigger). The immediate re-open happens AFTER the lock is
        released -- it goes through `_execute_planned_order`, which acquires
        the same lock itself, so it cannot be taken while still held here."""
        reset_ref_price: float | None = None

        async with self._position_lock:
            qty = self.position.total_qty
            avg_entry = self.position.avg_entry_price
            open_fees = self.position.total_open_fees
            max_fib = self.position.max_fib_level

            try:
                filled = await self.exchange.close_position_market()
            except Exception:
                logger.exception("Failed to close position at market on take-profit trigger")
                return

            if filled is None:
                logger.error("Take profit requested but no open position was found on the exchange; "
                             "resetting local state only.")
                reset_ref_price = mark_price
            else:
                close_fee = self.fee_engine.taker_fee_for_notional(filled.notional_usdt)
                self.cycle_orders.append(OrderRecord(
                    timestamp_ms=filled.timestamp_ms, fib_level=0, kind="close_tp",
                    price=filled.price, qty=filled.qty, notional_usdt=filled.notional_usdt, fee_usdt=close_fee,
                ))

                gross_pnl = (avg_entry - filled.price) * qty
                total_fees = open_fees + close_fee
                total_funding = self.fee_engine.total_funding_cashflow()
                net_pnl = gross_pnl - total_fees + total_funding

                self.analytics.record_cycle(CycleRecord(
                    cycle_id=self.cycle_id,
                    start_ts_ms=self.cycle_start_ts_ms,
                    end_ts_ms=filled.timestamp_ms,
                    orders=self.cycle_orders,
                    max_fib_level=max_fib,
                    gross_pnl_usdt=gross_pnl,
                    total_fees_usdt=total_fees,
                    total_funding_usdt=total_funding,
                    net_pnl_usdt=net_pnl,
                    base_notional_at_start_usdt=self.base_notional_usdt,
                ))
                logger.info("TAKE PROFIT: cycle=%d net_pnl=%.2f USDT (gross=%.2f fees=%.2f funding=%.2f)",
                            self.cycle_id, net_pnl, gross_pnl, total_fees, total_funding)
                reset_ref_price = filled.price

            self._reset_state_after_take_profit(reset_ref_price)

        logger.info("Reopening the new base SHORT immediately at market (no candle-close wait).")
        await self._execute_immediate_base_order(kind="range_zero_reset_immediate")

    def _reset_state_after_take_profit(self, ref_price: float) -> None:
        """Synchronous on purpose: called while `_position_lock` is held, so it
        must not `await` (that would yield control mid-reset to another
        coroutine waiting on the same lock -- fine for correctness since the
        lock stays held, but pointless since there's no I/O here anyway)."""
        if self.cfg.auto_compound_enabled:
            old = self.base_notional_usdt
            self.base_notional_usdt *= (1.0 + self.cfg.auto_compound_percentage / 100.0)
            logger.info("Auto-compound applied: base_notional_usdt %.2f -> %.2f", old, self.base_notional_usdt)

        self.position.clear()
        self.fee_engine.reset()

        self.grid.full_reset(ref_price)
        self._rsi_tick_mode_active = False

        self.cycle_id += 1
        self.cycle_start_ts_ms = int(time.time() * 1000)
        self.cycle_orders = []
        logger.info("Cycle reset: new cycle_id=%d, restarted at Range 0 (base_price=%.4f).",
                    self.cycle_id, self.grid.base_price)

    # -- dashboard export (rule 10) -------------------------------------------

    def _export_state(self, mark_price: float, breakdown, margin_used: float) -> None:
        be_gross, be_net = compute_breakeven_prices(self.position.entries, self.fee_engine)
        live = LiveState(
            timestamp_ms=int(time.time() * 1000),
            symbol=self.cfg.symbol,
            timeframe=self.cfg.timeframe,
            cycle_id=self.cycle_id,
            mark_price=mark_price,
            avg_entry_price=self.position.avg_entry_price,
            breakeven_gross_price=be_gross,
            breakeven_net_price=be_net,
            total_qty=self.position.total_qty,
            total_notional_usdt=self.position.total_notional,
            margin_used_usdt=margin_used,
            current_fib_level=self.position.max_fib_level,
            range_base_price=self.grid.base_price,
            base_notional_usdt=self.base_notional_usdt,
            gross_pnl_usdt=breakdown.gross_pnl_usdt,
            net_pnl_usdt=breakdown.net_pnl_usdt,
            net_pnl_pct_on_margin=breakdown.net_pnl_pct_on_margin,
            open_fees_usdt=breakdown.open_fees_usdt,
            estimated_close_fee_usdt=breakdown.estimated_close_fee_usdt,
            funding_cashflow_usdt=breakdown.funding_cashflow_usdt,
            take_profit_target_price=self._take_profit_target_price(),
            take_profit_net_breakeven_pct=self.cfg.take_profit_net_breakeven_pct,
        )
        self.exporter.export(live, self.cycle_orders)

    # -- helpers -----------------------------------------------------------

    async def _wait_or_stop(self, timeout_sec: float) -> bool:
        """Sleeps up to `timeout_sec`, returning True early if a shutdown was requested."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout_sec)
            return True
        except asyncio.TimeoutError:
            return False


async def _run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = StrategyConfig.load(CONFIG_PATH)
    bot = GridBotOrchestrator(cfg)
    try:
        await bot.start()
    finally:
        await bot.stop()


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
