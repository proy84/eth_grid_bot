"""
strategy.py

Pure decision logic for the Only-Short grid on ETH/USDT Perpetual, 100x,
CROSS margin. This module has NO network/exchange dependency by design --
exchange I/O lives in `exchange.py`, orchestration and scheduling in
`main.py`, fee/PnL math in `fees.py`, and persistence in `analytics.py` /
`data_exporter.py`.

Design notes (interpretation of the dynamic percentage-grid spec):
  - There is no fixed price list anymore. `grid_step_pct` (config.json,
    e.g. 0.5 for 0.5%) defines a GEOMETRIC step: relative to a live anchor
    price B ("Range 0"), level N sits at `Level_N = B * (1 + step)^N` for
    any integer N (positive above B, negative below). `RangeGrid.base_price`
    IS that anchor -- there is no separate index into anything.
  - The anchor is set directly from the live market price by the caller
    (`main.py`): at startup, and again every time a new cycle starts after
    a Take Profit. Setting the anchor is NOT the same operation as
    classifying a candle close against an existing grid -- it simply
    declares "this price is Range 0" outright, by assignment
    (`RangeGrid.base_price = current_price`), with no boundary rule
    involved.
  - Ongoing classification (`RangeGrid.classify_offset`) maps a later
    candle-close price to an integer offset from the anchor: offset 0 means
    "still inside the first band above/at Range 0", offset 1 means one
    0.5% step higher, etc. It mirrors the old fixed-array rule 5 exactly
    (a price sitting precisely on a level boundary is assigned to the LOWER
    band), computed continuously via logarithms instead of a boundary
    array lookup.
  - Grid evaluation fires EXACTLY ONE order on every single candle close,
    with no "idle" outcome and no once-per-level de-duplication: the order
    size is always Fibonacci(offset + 1) * BASE_NOTIONAL_USDT, where
    `offset` is the distance (in 0.5% steps) between the candle's closing
    price and the current anchor -- 0 while price sits in Range 0 itself, 1
    one step up, 2 two steps up, etc. This holds whether price stayed put,
    advanced, or came right back to a level it already visited: every
    candle close is a fresh accumulation at whatever level price currently
    sits at. If price closes BELOW Range 0 (negative offset), the anchor
    shifts down to that level and the order size resets to Fibonacci(1)
    (the base quota).
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
    take_profit_net_breakeven_pct: float
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
            take_profit_net_breakeven_pct=float(raw["take_profit"]["net_breakeven_pct"]),
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
    level_index: int
    range_offset: int
    fib_n: int
    notional_usdt: float
    kind: str  # "fibonacci" (offset >= 0, includes staying in Range 0) | "range_shift_base"


@dataclass
class RangeGrid:
    """Dynamic geometric grid: `Level_N = base_price * (1 + step_pct) ** N`
    for any integer N. No fixed price list -- `base_price` (the "Range 0"
    anchor) and `step_pct` (e.g. 0.005 for 0.5%) fully define every level."""
    base_price: float
    step_pct: float

    def level_price(self, offset: int) -> float:
        return self.base_price * (1.0 + self.step_pct) ** offset

    def classify_offset(self, price: float) -> int:
        """Integer offset N such that price falls in (Level_N, Level_{N+1}]
        (a price exactly on a level boundary is assigned to the LOWER
        offset, mirroring rule 5). Computed via logarithms since levels are
        generated on the fly rather than looked up in a fixed array.

        NOTE: this is deliberately NOT used to anchor `base_price` itself --
        anchoring is a direct assignment (see `full_reset`/callers in
        main.py), not a classification of the anchor price against itself.
        """
        ratio = price / self.base_price
        raw = math.log(ratio) / math.log1p(self.step_pct)
        rounded = round(raw)
        if abs(raw - rounded) < 1e-9:
            raw = float(rounded)
        return math.ceil(raw) - 1

    def shift_base_down(self, new_offset: int) -> None:
        """Range Shift (rule 7): move Range 0 down to `Level_{new_offset}`."""
        self.base_price = self.level_price(new_offset)

    def full_reset(self, new_base_price: float) -> None:
        """Post take-profit reset (rule 13): anchor a fresh Range 0 directly
        at the given price (typically the live market price)."""
        self.base_price = new_base_price


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
                         max_fib_level: Optional[int]) -> PlannedOrder:
    """Grid evaluation, run on EVERY candle close of the configured timeframe
    (rule 2). Always returns exactly one order -- the bot never sits idle just
    because price stayed inside the same range as last candle. The CALLER must
    guarantee this only runs on candle close; this function itself is
    stateless with respect to time.

    - price closes BELOW Range 0 (negative offset) -> Range 0 shifts down to
      that level, order size is the base quota (Fibonacci(1) * BASE_NOTIONAL_USDT).
    - price closes AT OR ABOVE Range 0 (offset >= 0) -> order size is
      Fibonacci(offset + 1) * BASE_NOTIONAL_USDT, where offset = how many
      `grid_step_pct` steps above Range 0 the close price currently sits (0
      while still in Range 0 itself). This applies identically whether price
      held its ground, advanced, or returned to a level visited before.

    `max_fib_level=None` disables the safety cap entirely (stress-test mode):
    the Fibonacci level grows without bound as offset increases.
    """
    offset = grid.classify_offset(price)

    if offset < 0:
        grid.shift_base_down(offset)
        return PlannedOrder(level_index=offset, range_offset=0, fib_n=1,
                             notional_usdt=fibonacci(1) * base_notional_usdt, kind="range_shift_base")

    n = offset + 1
    if max_fib_level is not None and n > max_fib_level:
        logger.warning("Fib level %d (range offset %d) exceeds max_fib_level=%d; capping at %d.",
                        n, offset, max_fib_level, max_fib_level)
        n = max_fib_level
    notional = fibonacci(n) * base_notional_usdt
    return PlannedOrder(level_index=offset, range_offset=offset, fib_n=n, notional_usdt=notional, kind="fibonacci")


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
