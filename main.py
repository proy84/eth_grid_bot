"""
main.py

Orchestrator for the Only-Short grid bot. Wires together `exchange.py`
(I/O), `strategy.py` (pure grid/Fibonacci decision logic), `fees.py` (PnL &
break-even math), `analytics.py` (cycle history) and `data_exporter.py`
(dashboard state).

GRID IS STATIC FOR THE WHOLE CYCLE: `self.grid` (a `RangeGrid`) is anchored
ONCE, via `RangeGrid.full_reset()`, at the cycle's very first fill -- at bot
startup / position bootstrap (`_bootstrap_position`), and again exactly once
after each trailing-stop close (`_reset_state_after_close`). Between those
two moments `grid.base_price` NEVER changes: no downside re-anchoring, no
RSI-catch-up re-anchoring, nothing. Every subsequent order, at any offset
(positive above the anchor, negative below), is priced purely by
classifying the current price against that one fixed anchor (see
`strategy.evaluate_grid_close`). Break-Even (`PositionManager.avg_entry_price`
/ `fees.compute_breakeven_prices`) is tracked completely separately and
updates with every fill regardless of the grid -- it feeds only the
trailing-stop calculation below, never the grid's levels.

Two independent loops:
  - Grid scheduler: in normal (production) mode, wakes up once per
    `config.timeframe` interval, 1 second after the boundary (e.g. "5m" ->
    XX:05:01, XX:10:01 UTC; "1h" -> HH:00:01 UTC), fetches the just-CLOSED
    candle and ALWAYS executes one grid order sized off the current price's
    (signed) distance from the fixed anchor -- there is no "idle" outcome,
    even if price stayed exactly where it was last candle. In
    `stress_test.enabled` mode (see below) this scheduler is replaced
    entirely by a variable-interval, tick-driven one.
  - Tick loop (every `tick_poll_interval_sec`, default 2s): recomputes real
    net PnL, drives the SHORT-only trailing stop, exports live state for
    the frontend, and (stress-test mode only) checks whether the position's
    average entry price has caught up to the market's current grid band
    (to decide the RSI tick-mode cadence -- never to move the grid).

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
     overbought does not re-trigger it) switches on tick-mode execution at
     whatever offset price currently sits at, relative to the fixed anchor.
     Grid orders at that cadence pull the position's average entry price
     (break-even) with every fill; once break-even catches up to the live
     market's grid band, tick mode turns off and the base cadence resumes
     -- the grid's anchor itself is NEVER touched, only the cadence. A
     fresh RSI crossing is required to re-arm it.
Disabling `stress_test.enabled` restores the exact production behavior
(fixed timeframe, candle-close evaluation, capped Fibonacci level, no RSI
trigger) with no other code path affected.

Exit is config-driven via `trailing_stop.enabled`:
  - `false` (current default): FIXED take profit (`_maybe_handle_fixed_take_profit`)
    -- closes the whole position at market the instant `mark_price` first drops
    `trailing_stop.activation_pct`% below the current Break-Even NETTO price
    (see `fees.compute_breakeven_prices` -- already fee/funding-adjusted). A
    single threshold check, no arming, no trailing; `distance_pct` is unused.
  - `true`: SHORT-only TRAILING stop (`_maybe_handle_trailing_stop`) -- NOT
    active at position entry. It arms the instant `mark_price` first drops to
    that same `activation_pct`% threshold, pinning the initial stop
    `trailing_stop.distance_pct`% ABOVE that arming price. From then on, every
    tick that makes a NEW LOW pulls the stop down with it (still
    `distance_pct`% above the lowest mark_price seen since arming) -- the stop
    never retraces upward on a bounce. The position closes at market the
    instant `mark_price` rises back up to touch or cross the stop.
Both percentages are real PRICE percentages, not leveraged-margin percentages.

Grid-triggered entries and trailing-stop-triggered closes both mutate the
same position state, and each involves at least one `await` (an exchange
call) -- without protection, a grid evaluation firing in the same instant as
a trailing-stop close could interleave and corrupt the PnL bookkeeping (the
close would execute against the exchange's real, up-to-date position while
our recorded gross/fees were snapshotted from a stale, smaller position).
`self._position_lock` (asyncio.Lock) serializes every position mutation --
`_execute_planned_order` and the close+record portion of `_close_cycle` --
so the two flows can never interleave.

Two situations bypass the candle-close wait and fire a market order
immediately, per spec -- and both are the ONLY two moments the grid's
anchor is ever set:
  1. Bot startup with no open position on the symbol -> immediate base
     SHORT (Fibonacci(1) * BASE_NOTIONAL_USDT) at market, anchoring the
     grid to the current price for the whole cycle.
  2. A cycle closing via the trailing stop -> immediate re-open of the new
     base SHORT at market, anchoring a fresh grid for the new cycle.
Every subsequent order (at any offset, positive or negative, relative to
that one fixed anchor) is decided strictly on candle close.

In `stress_test.enabled` mode, case 2 also restarts the base-cadence
countdown (`_cadence_reset_event`): the cycle's SECOND order always lands
exactly `stress_test_base_interval_sec` after that immediate re-open, rather
than the base cadence continuing to tick on its own schedule regardless of
when cycles happen to close and reopen.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
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

BOT_VERSION = "1.2"
CONFIG_PATH = "config.json"
CANDLE_CLOSE_OFFSET_SEC = 1.0  # evaluate the grid 1s after each timeframe boundary
RSI_POLL_INTERVAL_SEC = 15.0  # how often the tick loop re-checks for a newly-closed RSI candle
STOP_SIGNAL_PATH = Path("STOP")  # touch this file (same dir as main.py) for a clean remote shutdown


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
        # Set by `_close_cycle` right after the new cycle's immediate base
        # order fires; consumed by `_wait_interruptible` to abort the base
        # cadence's countdown early and restart it fresh from that moment --
        # so "second order of the cycle" always lands exactly
        # `stress_test_base_interval_sec` after the cycle's immediate first
        # order, instead of the base cadence ticking on its own independent
        # schedule regardless of cycle boundaries.
        self._cadence_reset_event = asyncio.Event()
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
        # grid band, or whenever a new cycle starts (trailing-stop close).
        # The grid's own anchor is never touched by this flag either way.
        self._current_rsi: float | None = None
        self._rsi_last_candle_ts: int | None = None
        self._last_rsi_poll_ts = 0.0
        self._rsi_tick_mode_active = False
        # SHORT-only trailing stop. `_trailing_active` becomes True the
        # instant mark_price first drops to trailing_activation_pct% below
        # Break-Even NETTO (never at entry); `_trailing_lowest_price` then
        # tracks the lowest mark_price seen since arming, and the stop is
        # always trailing_distance_pct% above it -- monotonically
        # non-increasing, never retraces upward on a bounce. Both reset on
        # every cycle close (see `_reset_state_after_close`).
        self._trailing_active = False
        self._trailing_lowest_price: float | None = None

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
                "[stress-test] STRESS TEST MODE ENABLED: STATIC grid (anchored once per cycle, never re-anchors); "
                "base cadence %.0fs for all ranges; RSI(%d) on %s >= %.1f triggers tick mode (every %.0fs) until "
                "Break-Even catches up to the market's grid band; Neutral Zone %s (%.2f%% around Break-Even "
                "LORDO); Fibonacci cap %s. Set stress_test.enabled=false in config.json to return to "
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

        base_notional = self._effective_base_notional(mark_price)
        order = PlannedOrder(range_offset=0, fib_n=1,
                              notional_usdt=fibonacci(1) * base_notional, kind=kind)
        await self._execute_planned_order(order, mark_price, int(time.time() * 1000))

    def _effective_base_notional(self, price: float) -> float:
        """The Fibonacci sequence's base unit (fib_n=1) for THIS evaluation:
        whichever is larger between the configured `base_notional_usdt` and
        the exchange's minimum tradable notional at the current price
        (`min_order_qty() * price`). Without this, a `base_notional_usdt`
        below the exchange minimum would make every early fib_n level
        (however many it takes for `Fibonacci(n) * base_notional_usdt` to
        naturally exceed the minimum) collapse to the SAME floored qty
        independently, producing a run of identical orders instead of a
        real progression. Recomputed fresh on every call (not cached/stored)
        so it stays correct at any price, indefinitely -- same philosophy as
        `ExchangeClient.min_order_qty()` itself."""
        min_qty = self.exchange.min_order_qty()
        min_notional = min_qty * price if min_qty > 0 else 0.0
        return max(self.base_notional_usdt, min_notional)

    # -- grid scheduling (rule 2/3: increments & shifts wait for candle close, --
    # -- unless stress_test.enabled overrides with a variable tick-driven cadence) -

    def _seconds_until_next_boundary(self) -> float:
        now = time.time()
        next_boundary = (math.floor(now / self.timeframe_sec) + 1) * self.timeframe_sec
        target = next_boundary + CANDLE_CLOSE_OFFSET_SEC
        return max(0.0, target - now)

    async def _wait_interruptible(self, timeout_sec: float) -> bool:
        """Like `_wait_or_stop`, but for the stress-test BASE cadence only:
        polls in 1s steps so it can break out early on either of two events --
        RSI tick mode activating mid-wait (see `_maybe_update_rsi`), or a new
        cycle starting mid-wait (`_cadence_reset_event`, set by `_close_cycle`
        right after the new cycle's immediate order fires) -- instead of
        finishing out a stale wait. Returns True only if shutdown was
        requested; either other interruption returns False just like a
        normal timeout, so the caller re-decides cadence from scratch
        either way."""
        elapsed = 0.0
        step = 1.0
        while elapsed < timeout_sec:
            this_step = min(step, timeout_sec - elapsed)
            if await self._wait_or_stop(this_step):
                return True
            elapsed += this_step
            if self._rsi_tick_mode_active:
                return False
            if self._cadence_reset_event.is_set():
                return False
        return False

    async def _grid_scheduler_loop(self) -> None:
        """Every iteration's body runs under its own try/except: unlike a
        transient network error inside `_run_grid_evaluation` (already
        handled there), an unexpected exception ANYWHERE in this loop
        (scheduling math, Neutral Zone, order execution/bookkeeping) would
        otherwise propagate out of this coroutine, which `asyncio.gather` in
        `start()` would treat as fatal -- cancelling the tick loop right
        along with it and killing the whole process silently. Mirrors the
        try/except already wrapping `_tick_loop`'s body."""
        while not self._stop_event.is_set():
            try:
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
                        if self._cadence_reset_event.is_set():
                            self._cadence_reset_event.clear()
                            logger.info(
                                "[stress-test] Cadenza oraria riavviata: nuovo ciclo aperto durante l'attesa -- "
                                "prossimo ordine schedulato tra %.0fs esatti da adesso.",
                                self.cfg.stress_test_base_interval_sec,
                            )
                            continue
                else:
                    sleep_sec = self._seconds_until_next_boundary()
                    next_run = datetime.fromtimestamp(time.time() + sleep_sec, tz=timezone.utc)
                    logger.info("Next grid evaluation in %.1fs, at %s (timeframe=%s).",
                                sleep_sec, next_run.strftime("%Y-%m-%d %H:%M:%S UTC"), self.cfg.timeframe)
                    if await self._wait_or_stop(sleep_sec):
                        break
                await self._run_grid_evaluation()
            except Exception:
                logger.exception(
                    "Errore imprevisto nel grid scheduler loop -- il bot NON si ferma, riprovo al prossimo giro "
                    "invece di lasciare che l'eccezione termini l'intero processo."
                )

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
        breakeven_price = None if self.position.is_flat else self.position.avg_entry_price
        base_notional = self._effective_base_notional(price)
        order = evaluate_grid_close(price, self.grid, base_notional, max_fib_level, breakeven_price)
        if self.cfg.stress_test_enabled and self.cfg.stress_test_neutral_zone_enabled:
            if self._maybe_suspend_for_neutral_zone(price):
                return
        logger.info("Grid evaluation: close=%.4f base_price=%.4f offset=%d -> fib_n=%d (%s)",
                    price, self.grid.base_price, order.range_offset, order.fib_n, order.kind)
        await self._execute_planned_order(order, price, ts_ms)

    def _maybe_suspend_for_neutral_zone(self, mark_price: float) -> bool:
        """Neutral Zone (stress-test only): pauses Fibonacci accumulation
        entries -- above OR below the fixed anchor alike, there is no more
        "Range Shift" special case now that the grid never re-anchors mid-
        cycle -- while the market price sits within
        `stress_test_neutral_zone_percent`% of the Break-Even LORDO
        (`avg_entry_price`; the Break-Even NETTO stays reserved for the
        trailing-stop calculation). Buying more this close to break-even is
        just fee churn with no strategic benefit. Bypassed unconditionally
        whenever the current RSI(5m) reading is >= the overbought threshold --
        a direct check on the live RSI value, independent of whether tick
        mode is presently active or already turned off after a catch-up --
        so a genuinely overbought market can still accumulate right through
        the zone. Returns True if the caller should skip execution this
        round."""
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
            # `compute_qty_from_notional` itself can raise `InvalidOrder` (CCXT's
            # `amount_to_precision`) when notional/price rounds to LESS than one
            # full precision step -- it doesn't clamp to zero, it throws. So the
            # floor below must be reachable even when the raw computation never
            # produces a usable qty at all, not just when it produces a qty that
            # is merely "too small but still computable" (the previous version of
            # this floor only handled the latter case).
            try:
                qty = self.exchange.compute_qty_from_notional(order.notional_usdt, ref_price)
            except Exception:
                logger.info(
                    "notional=%.2f @ prezzo=%.2f e sotto un intero step di precisione dell'exchange "
                    "(quantita arrotondata a zero) -- alzo al minimo tradabile.",
                    order.notional_usdt, ref_price,
                )
                qty = 0.0

            # Floors qty up to the exchange's minimum tradable amount if the
            # configured notional would compute a smaller size than that --
            # e.g. base_notional_usdt=20 stops being enough once ETH trades
            # above ~2000 (0.01 min * price). Without this, a rising price
            # would eventually make every order raise InvalidOrder and kill
            # the bot; this keeps it tradable at any price, indefinitely,
            # instead of hard-coding price thresholds that would need manual
            # updates forever.
            min_qty = self.exchange.min_order_qty()
            if min_qty > 0 and qty < min_qty:
                logger.info(
                    "Qty %.6f (da notional=%.2f @ %.2f) sotto il minimo exchange %.6f -> alzata al minimo.",
                    qty, order.notional_usdt, ref_price, min_qty,
                )
                qty = min_qty

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

            was_flat_before = self.position.is_flat
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

            # Re-index Range 0 to the freshly-updated Break-Even -- skipped
            # for the cycle's very first fill (was_flat_before), since at
            # that instant Break-Even trivially equals the just-set anchor,
            # which would spuriously re-index to -1 (see RangeGrid.reindex_
            # to_breakeven's docstring for why).
            if not was_flat_before:
                old_zero_index = self.grid.zero_index
                if self.grid.reindex_to_breakeven(self.position.avg_entry_price):
                    logger.info(
                        "Range 0 RE-INDICIZZATO al Break-Even: avg_entry=%.4f -> zero_index %d -> %d "
                        "(quote di prezzo invariate, base_price=%.4f).",
                        self.position.avg_entry_price, old_zero_index, self.grid.zero_index, self.grid.base_price,
                    )

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

        if self.cfg.stress_test_enabled and self.cfg.stress_test_rsi_enabled:
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
            self._trailing_active = False
            self._trailing_lowest_price = None
            return

        if self.cfg.trailing_stop_enabled:
            await self._maybe_handle_trailing_stop(mark_price)
        else:
            await self._maybe_handle_fixed_take_profit(mark_price)

    async def _maybe_handle_fixed_take_profit(self, mark_price: float) -> None:
        """Fixed take-profit (used when `trailing_stop.enabled` is false): closes
        the whole position at market the instant mark_price first drops
        `trailing_activation_pct`% below Break-Even NETTO. No arming, no
        trailing -- it's a single fixed threshold, reusing the same field that
        sets the trailing stop's activation distance when trailing is enabled."""
        _, be_net = compute_breakeven_prices(self.position.entries, self.fee_engine)
        if be_net <= 0:
            return
        tp_price = be_net * (1.0 - self.cfg.trailing_activation_pct / 100.0)
        if mark_price <= tp_price:
            logger.info(
                "Take Profit COLPITO: mark=%.4f <= soglia=%.4f (%.2f%% sotto Break-Even NETTO).",
                mark_price, tp_price, self.cfg.trailing_activation_pct,
            )
            await self._close_cycle(mark_price)

    def _trailing_activation_price(self) -> float | None:
        """SHORT profits as price falls, so the trailing stop arms the
        instant mark_price first drops to `trailing_activation_pct`% BELOW
        the net breakeven price -- never at entry."""
        _, be_net = compute_breakeven_prices(self.position.entries, self.fee_engine)
        if be_net <= 0:
            return None
        return be_net * (1.0 - self.cfg.trailing_activation_pct / 100.0)

    def _trailing_stop_price(self) -> float | None:
        """Current stop level: `trailing_distance_pct`% above the lowest
        mark_price seen since the trailing stop armed. None if not active."""
        if not self._trailing_active or self._trailing_lowest_price is None:
            return None
        return self._trailing_lowest_price * (1.0 + self.cfg.trailing_distance_pct / 100.0)

    async def _maybe_handle_trailing_stop(self, mark_price: float) -> None:
        """SHORT-only trailing stop (replaces the old fixed take profit).

        Not active at entry: arms the instant `mark_price` first drops to
        `trailing_activation_pct`% below Break-Even NETTO, pinning the
        initial stop `trailing_distance_pct`% ABOVE that arming price. From
        then on, every new low pulls the stop down with it (still
        `distance_pct`% above the lowest mark_price seen since arming) --
        the stop is monotonically non-increasing and never retraces upward
        on a bounce. Closes the whole position at market the instant
        `mark_price` rises back up to touch or cross the stop."""
        if not self._trailing_active:
            activation_price = self._trailing_activation_price()
            if activation_price is None or mark_price > activation_price:
                return
            self._trailing_active = True
            self._trailing_lowest_price = mark_price
            logger.info(
                "Trailing Stop ATTIVATO: mark=%.4f <= soglia attivazione=%.4f (%.2f%% sotto Break-Even NETTO) "
                "-> stop iniziale=%.4f (%.2f%% sopra il prezzo di attivazione).",
                mark_price, activation_price, self.cfg.trailing_activation_pct,
                self._trailing_stop_price(), self.cfg.trailing_distance_pct,
            )
            return

        if mark_price < self._trailing_lowest_price:
            self._trailing_lowest_price = mark_price
            logger.info("Trailing Stop: nuovo minimo=%.4f -> stop aggiornato a %.4f.",
                        mark_price, self._trailing_stop_price())
            return

        stop_price = self._trailing_stop_price()
        if stop_price is not None and mark_price >= stop_price:
            logger.info("Trailing Stop COLPITO: mark=%.4f >= stop=%.4f (minimo raggiunto dopo l'attivazione=%.4f).",
                        mark_price, stop_price, self._trailing_lowest_price)
            await self._close_cycle(mark_price)

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
        on the CURRENT band (relative to the cycle's fixed anchor), which
        pulls the position's average entry price (break-even) with every
        fill. Once break-even has caught up to the live market's grid band,
        there is nothing left to accelerate: turn tick mode off and fall
        back to the base cadence. The grid's anchor is NEVER touched here --
        `RangeGrid.base_price` is fixed for the whole cycle (see
        strategy.py's module docstring); this only ever changes the
        scheduling cadence. A fresh RSI crossing is required to re-arm (see
        `_maybe_update_rsi`) -- this method never turns tick mode back ON,
        only off."""
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

        self._rsi_tick_mode_active = False
        logger.info(
            "Break-Even raggiunto: Tick Mode Disattivato (avg_entry=%.4f mark=%.4f, both in offset band %d "
            "relative to fixed base_price=%.4f) -> frequency back to %.0fs. Griglia invariata.",
            avg_entry, mark_price, mark_offset, self.grid.base_price,
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

    # -- trailing stop / cycle reset -------------------------------------------

    async def _close_cycle(self, mark_price: float) -> None:
        """Holds `_position_lock` through snapshot -> exchange close -> cycle
        recording -> state reset, so a concurrent grid evaluation can never add
        an entry the close's PnL calculation doesn't account for (the bug that
        previously made our recorded net_pnl diverge from Bybit's own realized
        P&L whenever a candle-close evaluation landed at the same instant as a
        trailing-stop trigger). The immediate re-open happens AFTER the lock is
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
                logger.exception("Failed to close position at market on trailing-stop trigger")
                return

            if filled is None:
                logger.error("Trailing stop triggered but no open position was found on the exchange; "
                             "resetting local state only.")
                reset_ref_price = mark_price
            else:
                # The exchange position is already closed at this point (the
                # `close_position_market()` call above succeeded) -- from here
                # on, `reset_ref_price` MUST end up set and `_reset_state_after_
                # close` MUST run no matter what, even if recording the cycle's
                # stats fails (e.g. a disk I/O error writing trade_history.json,
                # plausible on flaky mobile storage). Otherwise the bot's local
                # state would keep believing the old position is still open
                # while the exchange is flat -- a silent desync, not just a
                # missed log line.
                reset_ref_price = filled.price
                try:
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
                    logger.info("TRAILING STOP: cycle=%d net_pnl=%.2f USDT (gross=%.2f fees=%.2f funding=%.2f)",
                                self.cycle_id, net_pnl, gross_pnl, total_fees, total_funding)
                except Exception:
                    logger.exception(
                        "Failed to record closed cycle stats (trade_history.json) -- resetting local state "
                        "anyway since the exchange position is already closed; this cycle's stats are lost "
                        "but the bot stays in sync with the exchange."
                    )

            self._reset_state_after_close(reset_ref_price)

        logger.info("Reopening the new base SHORT immediately at market (no candle-close wait).")
        await self._execute_immediate_base_order(kind="range_zero_reset_immediate")
        # Wakes up `_grid_scheduler_loop` (if it's mid-wait on the base cadence)
        # to abort and restart its countdown fresh from right now -- so the
        # cycle's SECOND order lands exactly `stress_test_base_interval_sec`
        # after this immediate first order, instead of the base cadence
        # ticking on independently of cycle boundaries.
        self._cadence_reset_event.set()

    def _reset_state_after_close(self, ref_price: float) -> None:
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
        self._trailing_active = False
        self._trailing_lowest_price = None

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
            trailing_stop_active=self._trailing_active,
            trailing_activation_price=self._trailing_activation_price(),
            trailing_lowest_price=self._trailing_lowest_price,
            trailing_stop_price=self._trailing_stop_price(),
            trailing_activation_pct=self.cfg.trailing_activation_pct,
            trailing_distance_pct=self.cfg.trailing_distance_pct,
        )
        self.exporter.export(live, self.cycle_orders)

    # -- helpers -----------------------------------------------------------

    async def _wait_or_stop(self, timeout_sec: float) -> bool:
        """Sleeps up to `timeout_sec`, returning True early if a shutdown was requested.
        Also polls for STOP_SIGNAL_PATH (see module docstring / STOP_SIGNAL_PATH) so the
        bot can be stopped cleanly from any shell in this directory -- no PID hunting,
        no terminal reattachment -- on both Windows and Termux, e.g. `touch STOP`."""
        self._check_stop_file()
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout_sec)
            return True
        except asyncio.TimeoutError:
            return False

    def _check_stop_file(self) -> None:
        if not STOP_SIGNAL_PATH.exists():
            return
        try:
            STOP_SIGNAL_PATH.unlink()
        except OSError:
            pass
        logger.info("Stop-file '%s' rilevato: arresto pulito richiesto.", STOP_SIGNAL_PATH)
        self._stop_event.set()


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
