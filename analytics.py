"""
analytics.py

Persists completed grid cycles to `trade_history.json` and derives running
performance statistics: average net profit, max drawdown (on the cumulative
net-PnL equity curve), max Fibonacci level ever reached, average cycle
duration, total fees/funding, cumulative net PnL, and operation counters
(completed cycles, total orders executed, average orders per cycle).
"""

from __future__ import annotations

import json
import statistics
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Union


@dataclass(frozen=True)
class OrderRecord:
    timestamp_ms: int
    fib_level: int
    kind: str  # "fibonacci" | "range_shift_base" | "close_tp"
    price: float
    qty: float
    notional_usdt: float
    fee_usdt: float


@dataclass
class CycleRecord:
    cycle_id: int
    start_ts_ms: int
    end_ts_ms: int
    orders: List[OrderRecord]
    max_fib_level: int
    gross_pnl_usdt: float
    total_fees_usdt: float
    total_funding_usdt: float
    net_pnl_usdt: float
    base_notional_at_start_usdt: float

    @property
    def duration_sec(self) -> float:
        return (self.end_ts_ms - self.start_ts_ms) / 1000.0

    @property
    def orders_count(self) -> int:
        return len(self.orders)


@dataclass(frozen=True)
class AnalyticsSummary:
    completed_cycles: int
    total_orders_executed: int
    avg_orders_per_cycle: float
    avg_net_profit_usdt: float
    max_drawdown_usdt: float
    max_fibonacci_level: int
    avg_cycle_duration_sec: float
    total_fees_usdt: float
    total_funding_usdt: float
    cumulative_net_pnl_usdt: float


_EMPTY_SUMMARY = AnalyticsSummary(
    completed_cycles=0,
    total_orders_executed=0,
    avg_orders_per_cycle=0.0,
    avg_net_profit_usdt=0.0,
    max_drawdown_usdt=0.0,
    max_fibonacci_level=0,
    avg_cycle_duration_sec=0.0,
    total_fees_usdt=0.0,
    total_funding_usdt=0.0,
    cumulative_net_pnl_usdt=0.0,
)


class AnalyticsEngine:
    """Owns `trade_history.json`: append-only cycle log plus a recomputed summary."""

    def __init__(self, history_path: Union[str, Path]):
        self.history_path = Path(history_path)
        self.cycles: List[CycleRecord] = []
        self._load()

    def _load(self) -> None:
        if not self.history_path.exists():
            return
        raw = json.loads(self.history_path.read_text(encoding="utf-8"))
        for c in raw.get("cycles", []):
            c = dict(c)
            orders = [OrderRecord(**o) for o in c.pop("orders", [])]
            c.pop("duration_sec", None)
            c.pop("orders_count", None)
            self.cycles.append(CycleRecord(orders=orders, **c))

    def record_cycle(self, cycle: CycleRecord) -> None:
        self.cycles.append(cycle)
        self._persist()

    def summary(self) -> AnalyticsSummary:
        if not self.cycles:
            return _EMPTY_SUMMARY

        total_orders = sum(c.orders_count for c in self.cycles)
        net_pnls = [c.net_pnl_usdt for c in self.cycles]

        equity_curve: List[float] = []
        running = 0.0
        for pnl in net_pnls:
            running += pnl
            equity_curve.append(running)

        return AnalyticsSummary(
            completed_cycles=len(self.cycles),
            total_orders_executed=total_orders,
            avg_orders_per_cycle=total_orders / len(self.cycles),
            avg_net_profit_usdt=statistics.fmean(net_pnls),
            max_drawdown_usdt=self._max_drawdown(equity_curve),
            max_fibonacci_level=max(c.max_fib_level for c in self.cycles),
            avg_cycle_duration_sec=statistics.fmean(c.duration_sec for c in self.cycles),
            total_fees_usdt=sum(c.total_fees_usdt for c in self.cycles),
            total_funding_usdt=sum(c.total_funding_usdt for c in self.cycles),
            cumulative_net_pnl_usdt=running,
        )

    @staticmethod
    def _max_drawdown(equity_curve: List[float]) -> float:
        peak = float("-inf")
        max_dd = 0.0
        for v in equity_curve:
            peak = max(peak, v)
            max_dd = max(max_dd, peak - v)
        return max_dd

    def _persist(self) -> None:
        payload = {
            "cycles": [asdict(c) for c in self.cycles],
            "summary": asdict(self.summary()),
        }
        tmp = self.history_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.history_path)
