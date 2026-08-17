"""
data_exporter.py

Continuously writes a snapshot of the bot's live state to `bot_state.json`
for a frontend/dashboard to poll. Every write is atomic (write to a temp
file, then os.replace) so the dashboard never reads a half-written file.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional, Union

from analytics import AnalyticsEngine, OrderRecord

logger = logging.getLogger("eth_grid_bot.data_exporter")


@dataclass(frozen=True)
class LiveState:
    timestamp_ms: int
    symbol: str
    timeframe: str
    cycle_id: int

    mark_price: float
    avg_entry_price: float
    breakeven_gross_price: float
    breakeven_net_price: float

    total_qty: float
    total_notional_usdt: float
    margin_used_usdt: float

    current_fib_level: int
    range_base_price: float
    base_notional_usdt: float

    gross_pnl_usdt: float
    net_pnl_usdt: float
    net_pnl_pct_on_margin: float
    open_fees_usdt: float
    estimated_close_fee_usdt: float
    funding_cashflow_usdt: float

    trailing_stop_active: bool
    trailing_activation_price: Optional[float]
    trailing_lowest_price: Optional[float]
    trailing_stop_price: Optional[float]
    trailing_activation_pct: float
    trailing_distance_pct: float


class DataExporter:
    """Owns `bot_state.json`. Pulls cumulative history from `AnalyticsEngine`
    so the dashboard gets live state + historical performance in one file."""

    def __init__(self, state_path: Union[str, Path], analytics: AnalyticsEngine, history_limit: int = 50):
        self.state_path = Path(state_path)
        self.analytics = analytics
        self.history_limit = history_limit

    def export(self, live: LiveState, current_cycle_orders: List[OrderRecord]) -> None:
        summary = self.analytics.summary()
        recent_cycles = self.analytics.cycles[-self.history_limit:]

        payload = {
            "live": asdict(live),
            "current_cycle_orders": [asdict(o) for o in current_cycle_orders],
            "performance_summary": asdict(summary),
            "recent_cycles": [
                {
                    "cycle_id": c.cycle_id,
                    "start_ts_ms": c.start_ts_ms,
                    "end_ts_ms": c.end_ts_ms,
                    "duration_sec": c.duration_sec,
                    "orders_count": c.orders_count,
                    "max_fib_level": c.max_fib_level,
                    "net_pnl_usdt": c.net_pnl_usdt,
                    "total_fees_usdt": c.total_fees_usdt,
                    "total_funding_usdt": c.total_funding_usdt,
                }
                for c in recent_cycles
            ],
        }

        tmp = self.state_path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp.replace(self.state_path)
        except OSError:
            logger.exception("Failed to write %s", self.state_path)
