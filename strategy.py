"""
strategy.py

Pure decision logic for the Only-Short grid on ETH/USDT Perpetual, 100x,
CROSS margin. This module has NO network/exchange dependency by design --
exchange I/O lives in `exchange.py`, orchestration and scheduling in
`main.py`, fee/PnL math in `fees.py`, and persistence in `analytics.py` /
`data_exporter.py`.

Design notes (fixed price ladder, floating Range-0 label re-indexed to Break-Even):
  - The price QUOTES never change. `grid_step_pct` (config.json, e.g. 0.5
    for 0.5%) defines a GEOMETRIC step: relative to the anchor price B set
    ONCE at the cycle's very first fill, ABSOLUTE level N sits at
    `AbsoluteLevel_N = B * (1 + step)^N` for any integer N -- and that exact
    price never moves for the rest of the cycle. `RangeGrid.base_price` IS
    that anchor, mutated only by `full_reset` (cycle boundaries).
  - What DOES move is which absolute level is currently LABELED "Range 0":
    `RangeGrid.zero_index`. `RangeGrid.reindex_to_breakeven` re-points it to
    wherever the position's Break-Even (LORDO, `PositionManager.avg_entry_price`)
    currently falls, every time a new fill updates Break-Even -- WITHOUT
    touching `base_price` or any level's price. This only changes which
    label ("Range 0", "Range +1", "Range -2", ...) an existing, unchanged
    price quote currently wears -- a pure re-indexing, not a recalculation.
  - `RangeGrid.classify_offset` and `RangeGrid.level_price` both work in
    this LABELED (zero_index-relative) space, so every other piece of logic
    in the codebase (Fibonacci sizing, Neutral Zone, RSI gap/catch-up,
    dashboard export) is written purely in terms of "offset from Range 0"
    and needs no awareness that Range 0 itself is following Break-Even --
    they just see it happen. `zero_index` resets to 0 on `full_reset` too
    (a fresh cycle's first fill IS its Range 0, by definition).
  - Grid evaluation fires EXACTLY ONE order on every single candle close,
    with no "idle" outcome and no once-per-level de-duplication: the order
    size is always `Fibonacci(|offset| + 1) * BASE_NOTIONAL_USDT`, where
    `offset` is the SIGNED, LABELED distance (in 0.5% steps) between the
    candle's closing price and the CURRENT Range 0 -- 0 while price sits in
    Range 0 itself, 1 one step up, -1 one step down, etc., symmetric in
    both directions.
  - Break-Even (the position's fee-adjusted average entry) is computed in
    `fees.py` / `PositionManager`, entirely independently of the grid math
    -- the grid only reads it (via `reindex_to_breakeven`) after each fill.
    It is also used, separately, for the trailing-stop activation/trail
    calculation in `main.py`.
  - Grid evaluation must be invoked ONLY on candle close (rule 2) --
    enforced by the caller (main.py), not by this module.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from fees import EntryFill

logger = logging.getLogger("eth_grid_bot.strategy")

# Loads .env into the process environment (no-op if the file doesn't exist).
# Called at import time so BYBIT_API_KEY/BYBIT_API_SECRET/USE_TESTNET/SYMBOL/
# GRID_STEP_PERCENT are available to StrategyConfig.load() and to
# exchange.py's own os.environ.get() lookups, wherever this module is
# imported first.
load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


# --------------------------------------------------------------------------- #
# Fibonacci progression
# --------------------------------------------------------------------------- #

@lru_cache(maxsize=None)
def fibonacci(n: int) -> int:
    """Standard sequence: fib(1)=1, fib(2)=1, fib(3)=2, fib(4)=3, fib(5)=5 ..."""
    if n <= 0:
        return 0
    if n <= 2:
        return 1
    return fibonacci(n - 1) + fibonacci(n - 2)


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class StrategyConfig:
    symbol: str
    timeframe: str
    leverage: int
    margin_mode: str
    grid_step_pct: float
    base_notional_usdt: float
    trailing_stop_enabled: bool
    trailing_activation_pct: float
    trailing_distance_pct: float
    taker_rate: float
    maker_rate: float
    auto_compound_enabled: bool
    auto_compound_percentage: float
    tick_poll_interval_sec: float
    funding_poll_interval_sec: float
    trade_history_path: str
    state_export_path: str
    max_fib_level: int
    exchange_id: str
    exchange_options: dict
    exchange_urls: Optional[dict]
    api_key: str
    api_secret: str
    use_testnet: bool
    stress_test_enabled: bool
    stress_test_base_interval_sec: float
    stress_test_tick_mode_interval_sec: float
    stress_test_rsi_timeframe: str
    stress_test_rsi_period: int
    stress_test_rsi_overbought_threshold: float
    stress_test_neutral_zone_enabled: bool
    stress_test_neutral_zone_percent: float
    stress_test_unlimited_fib_level: bool

    @staticmethod
    def load(path: str | Path) -> "StrategyConfig":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        stress_test = raw.get("stress_test", {})
        return StrategyConfig(
            symbol=os.environ.get("SYMBOL") or raw["symbol"],
            timeframe=raw["timeframe"],
            leverage=int(raw["leverage"]),
            margin_mode=raw["margin_mode"],
            grid_step_pct=float(os.environ.get("GRID_STEP_PERCENT") or raw["grid_step_pct"]) / 100.0,
            base_notional_usdt=float(raw["base_notional_usdt"]),
            trailing_stop_enabled=bool(raw["trailing_stop"].get("enabled", True)),
            trailing_activation_pct=float(raw["trailing_stop"]["activation_pct"]),
            trailing_distance_pct=float(raw["trailing_stop"]["distance_pct"]),
            taker_rate=float(raw["fees"]["taker_rate"]),
            maker_rate=float(raw["fees"].get("maker_rate", 0.0)),
            auto_compound_enabled=bool(raw["auto_compound"]["enabled"]),
            auto_compound_percentage=float(raw["auto_compound"]["percentage"]),
            tick_poll_interval_sec=float(raw["polling"]["tick_poll_interval_sec"]),
            funding_poll_interval_sec=float(raw["polling"]["funding_poll_interval_sec"]),
            trade_history_path=raw["paths"]["trade_history_path"],
            state_export_path=raw["paths"]["state_export_path"],
            max_fib_level=int(raw["risk"]["max_fib_level"]),
            exchange_id=raw["exchange"]["id"],
            exchange_options=raw["exchange"].get("options", {}),
            exchange_urls=raw["exchange"].get("urls"),
            api_key=os.environ.get("BYBIT_API_KEY") or raw["exchange"].get("api_key", ""),
            api_secret=os.environ.get("BYBIT_API_SECRET") or raw["exchange"].get("api_secret", ""),
            use_testnet=_env_bool("USE_TESTNET", bool(raw["exchange"].get("demo", True))),
            stress_test_enabled=bool(stress_test.get("enabled", False)),
            stress_test_base_interval_sec=float(stress_test.get("base_interval_sec", 300.0)),
            stress_test_tick_mode_interval_sec=float(stress_test.get("tick_mode_interval_sec", 1.0)),
            stress_test_rsi_timeframe=stress_test.get("rsi_timeframe", "1m"),
            stress_test_rsi_period=int(stress_test.get("rsi_period", 14)),
            stress_test_rsi_overbought_threshold=float(stress_test.get("rsi_overbought_threshold", 78.0)),
            stress_test_neutral_zone_enabled=bool(stress_test.get("neutral_zone_enabled", False)),
            stress_test_neutral_zone_percent=float(stress_test.get("neutral_zone_percent", 0.15)),
            stress_test_unlimited_fib_level=bool(stress_test.get("unlimited_fib_level", False)),
        )


# --------------------------------------------------------------------------- #
# Range grid
# --------------------------------------------------------------------------- #

@dataclass
class PlannedOrder:
    range_offset: int  # signed: can be negative (below anchor), 0, or positive (above anchor)
    fib_n: int
    notional_usdt: float
    kind: str  # always "fibonacci" -- every mediation order, up or down, is sized the same way


@dataclass
class RangeGrid:
    """Geometric price ladder, `AbsoluteLevel_N = base_price * (1 + step_pct) ** N`
    for any integer N, PLUS a floating re-indexing point `zero_index`.

    The price QUOTES are immutable for the whole cycle: `base_price` is set
    ONCE, via `full_reset`, and never mutated again until the next cycle --
    there is no method here that changes it any other way, so every
    absolute level's exact price never moves.

    What DOES move is which absolute level is currently LABELED "Range 0":
    `zero_index` (an absolute level number, starting at 0 -- the anchor
    itself). `reindex_to_breakeven` re-points it to wherever the position's
    Break-Even currently falls, WITHOUT touching `base_price` or any level's
    price -- it only changes which existing label ("Range 0", "Range +1", ...)
    a given absolute level currently wears. `classify_offset` and
    `level_price` both work in this LABELED (zero_index-relative) space, so
    every other caller in the codebase (Fibonacci sizing, Neutral Zone, RSI
    gap/catch-up, dashboard export) keeps working unchanged -- they just see
    Range 0 "follow" Break-Even automatically."""
    base_price: float
    step_pct: float
    zero_index: int = 0

    def level_price(self, offset: int) -> float:
        """Price of the level currently LABELED `offset` (relative to the
        current Range 0, i.e. `zero_index`)."""
        return self.base_price * (1.0 + self.step_pct) ** (self.zero_index + offset)

    def _absolute_offset(self, price: float) -> int:
        """Integer ABSOLUTE level N (relative to the fixed `base_price`
        anchor, ignoring `zero_index`) such that price falls in
        `(AbsoluteLevel_N, AbsoluteLevel_{N+1}]` (a price exactly on a level
        boundary is assigned to the LOWER level, mirroring rule 5). This is
        the one true coordinate system -- price quotes are computed from
        this and `zero_index` never enters into it."""
        ratio = price / self.base_price
        raw = math.log(ratio) / math.log1p(self.step_pct)
        rounded = round(raw)
        if abs(raw - rounded) < 1e-9:
            raw = float(rounded)
        return math.ceil(raw) - 1

    def classify_offset(self, price: float) -> int:
        """LABELED offset from the current Range 0: `absolute_offset(price)
        - zero_index`. Unbounded in either direction -- N can be negative.

        NOTE: this is deliberately NOT used to anchor `base_price` itself --
        anchoring is a direct assignment (see `full_reset`/callers in
        main.py), not a classification of the anchor price against itself.
        """
        return self._absolute_offset(price) - self.zero_index

    def full_reset(self, new_base_price: float) -> None:
        """Anchor a fresh Range 0 directly at the given price (the cycle's
        first fill) AND reset the re-indexing point back to 0. Called
        exactly once per cycle -- at bot startup / position bootstrap, and
        again after a trailing-stop close -- and NEVER in between."""
        self.base_price = new_base_price
        self.zero_index = 0

    def reindex_to_breakeven(self, breakeven_price: float) -> bool:
        """Re-labels Range 0 to wherever `breakeven_price` currently falls,
        in absolute-level terms -- the price ladder itself is untouched.
        Returns True if this actually moved `zero_index` (a real re-index),
        False if Break-Even is still inside the level already labeled
        Range 0 (nothing to do). Callers should skip invoking this for a
        cycle's very first fill: at that instant Break-Even trivially
        equals the just-set anchor price, which (per the boundary rule
        above) classifies one level BELOW itself -- invoking this here
        would spuriously re-index Range 0 to -1 before the cycle has even
        started."""
        new_zero_index = self._absolute_offset(breakeven_price)
        if new_zero_index == self.zero_index:
            return False
        self.zero_index = new_zero_index
        return True


def compute_rsi(closes: List[float], period: int) -> Optional[float]:
    """Wilder's smoothed RSI over `period` bars (the standard RSI-14
    definition). `closes` must be oldest-first; needs at least `period + 1`
    closes (period deltas) to seed the average -- returns None otherwise.
    Extra leading closes beyond that just let the Wilder smoothing converge
    further before the returned value, which is why callers fetch more than
    the bare minimum."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def evaluate_grid_close(price: float, grid: RangeGrid, base_notional_usdt: float,
                         max_fib_level: Optional[int],
                         breakeven_price: Optional[float] = None) -> PlannedOrder:
    """Grid evaluation, run on EVERY candle close of the configured timeframe
    (rule 2). Always returns exactly one order -- the bot never sits idle just
    because price stayed inside the same range as last candle. The CALLER must
    guarantee this only runs on candle close; this function itself is
    stateless with respect to time AND never mutates `grid` -- the anchor is
    fixed for the whole cycle (see `RangeGrid`), so this is pure classification.

    Sizing depends on `price` vs `breakeven_price` (the position's Break-Even
    LORDO, i.e. `PositionManager.avg_entry_price` -- NOT the anchor/Range 0,
    which may itself be re-indexed to Break-Even but is a discrete band while
    this is a direct price comparison):
      - `price < breakeven_price` (SHORT currently in PROFIT): always the
        FIXED base quota, `BASE_NOTIONAL_USDT`, fib_n=1 -- no Fibonacci
        multiplier, regardless of how far below or which offset it lands in.
        Re-entering while already profitable shouldn't escalate size.
      - `price >= breakeven_price` (SHORT currently at a loss, recovering)
        -- or `breakeven_price is None` (no position yet, e.g. the very
        first order of a cycle, though that path bypasses this function
        entirely in `main.py`) -- the usual progression applies:
        `Fibonacci(|offset| + 1) * BASE_NOTIONAL_USDT`, symmetric whether
        `offset` (the signed distance in `grid_step_pct` steps from the
        fixed anchor) is positive, negative, or zero.

    `max_fib_level=None` disables the safety cap entirely (stress-test mode):
    the Fibonacci level grows without bound as |offset| increases. The cap
    is irrelevant to the fixed-quota (profit-zone) branch, since that is
    always fib_n=1.
    """
    offset = grid.classify_offset(price)

    if breakeven_price is not None and price < breakeven_price:
        return PlannedOrder(range_offset=offset, fib_n=1, notional_usdt=base_notional_usdt, kind="fibonacci")

    n = abs(offset) + 1
    if max_fib_level is not None and n > max_fib_level:
        logger.warning("Fib level %d (range offset %d) exceeds max_fib_level=%d; capping at %d.",
                        n, offset, max_fib_level, max_fib_level)
        n = max_fib_level
    notional = fibonacci(n) * base_notional_usdt
    return PlannedOrder(range_offset=offset, fib_n=n, notional_usdt=notional, kind="fibonacci")


# --------------------------------------------------------------------------- #
# Position & exit control (trailing stop currently unused -- see fixed take
# profit logic in main.py)
# --------------------------------------------------------------------------- #

@dataclass
class PositionManager:
    entries: List[EntryFill] = field(default_factory=list)

    def add_entry(self, entry: EntryFill) -> None:
        self.entries.append(entry)

    @property
    def total_qty(self) -> float:
        return sum(e.qty for e in self.entries)

    @property
    def total_notional(self) -> float:
        return sum(e.notional_usdt for e in self.entries)

    @property
    def total_open_fees(self) -> float:
        return sum(e.taker_fee_usdt for e in self.entries)

    @property
    def avg_entry_price(self) -> float:
        qty = self.total_qty
        if qty <= 0:
            return 0.0
        return sum(e.price * e.qty for e in self.entries) / qty

    @property
    def max_fib_level(self) -> int:
        return max((e.fib_level for e in self.entries), default=0)

    @property
    def is_flat(self) -> bool:
        return self.total_qty <= 0

    def clear(self) -> None:
        self.entries.clear()


@dataclass
class TrailingStopController:
    """Currently unwired from main.py -- replaced by a fixed take-profit
    target (net_breakeven_pct below the Break-Even NETTO price) to avoid
    giving back gains during a retracement wait. Left here, still fully
    functional, in case the fixed-target experiment gets reverted."""
    activation_pct: float
    callback_pct: float
    active: bool = False
    peak_pct: float = float("-inf")

    def update(self, net_pnl_pct: float) -> bool:
        """Feed the latest tick's net PnL %; returns True when the trailing stop
        should fire a market close now (rule 8)."""
        if not self.active:
            if net_pnl_pct >= self.activation_pct:
                self.active = True
                self.peak_pct = net_pnl_pct
            return False

        self.peak_pct = max(self.peak_pct, net_pnl_pct)
        return (self.peak_pct - net_pnl_pct) >= self.callback_pct

    def reset(self) -> None:
        self.active = False
        self.peak_pct = float("-inf")
