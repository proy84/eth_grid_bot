"""
fees.py

Fee & funding accounting for the Only-Short ETH/USDT grid bot.

Responsibilities:
  - Taker fee calculation for opening fills and estimated market-close fees.
  - Funding rate cashflow tracking (a SHORT receives when funding_rate > 0,
    pays when funding_rate < 0 -- standard perpetual convention).
  - Net PnL breakdown (gross - open fees - estimated close fee + funding).
  - Break-even price calculation, both gross (average entry) and net
    (the close price at which realized net PnL is exactly zero).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Sequence, Tuple


@dataclass(frozen=True)
class FeeSchedule:
    taker_rate: float
    maker_rate: float = 0.0


@dataclass(frozen=True)
class FundingPayment:
    """A single REALIZED funding settlement, as reported by the exchange's own
    ledger (`ExchangeClient.fetch_realized_funding`) -- `cashflow_usdt` is the
    real amount already credited/debited to the account for that settlement,
    not an estimate derived from a polled rate. Positive = good for our SHORT
    book (received), negative = paid."""
    timestamp_ms: int
    cashflow_usdt: float


@dataclass(frozen=True)
class EntryFill:
    price: float
    qty: float
    notional_usdt: float
    taker_fee_usdt: float
    fib_level: int
    timestamp_ms: int


@dataclass
class FeeEngine:
    schedule: FeeSchedule
    funding_payments: List[FundingPayment] = field(default_factory=list)

    def taker_fee_for_notional(self, notional_usdt: float) -> float:
        return abs(notional_usdt) * self.schedule.taker_rate

    def estimate_close_fee(self, notional_to_close_usdt: float) -> float:
        return self.taker_fee_for_notional(notional_to_close_usdt)

    def record_funding(self, payment: FundingPayment) -> None:
        self.funding_payments.append(payment)

    def total_funding_cashflow(self) -> float:
        return sum(p.cashflow_usdt for p in self.funding_payments)

    def reset(self) -> None:
        self.funding_payments.clear()


@dataclass(frozen=True)
class NetPnlBreakdown:
    gross_pnl_usdt: float
    open_fees_usdt: float
    estimated_close_fee_usdt: float
    funding_cashflow_usdt: float
    net_pnl_usdt: float
    net_pnl_pct_on_margin: float


def compute_gross_pnl_short(avg_entry_price: float, mark_price: float, qty: float) -> float:
    """Gross PnL for a SHORT: profits when mark_price falls below avg_entry_price."""
    return (avg_entry_price - mark_price) * qty


def compute_net_pnl(
    *,
    avg_entry_price: float,
    mark_price: float,
    qty: float,
    open_fees_usdt: float,
    fee_engine: FeeEngine,
    margin_used_usdt: float,
) -> NetPnlBreakdown:
    """Net PnL netted against real opening fees, an estimated taker close fee at
    the current mark price, and accumulated funding cashflow."""
    gross = compute_gross_pnl_short(avg_entry_price, mark_price, qty)
    notional_to_close = mark_price * qty
    close_fee_est = fee_engine.estimate_close_fee(notional_to_close)
    funding_cf = fee_engine.total_funding_cashflow()
    net = gross - open_fees_usdt - close_fee_est + funding_cf
    net_pct = (net / margin_used_usdt * 100.0) if margin_used_usdt else 0.0
    return NetPnlBreakdown(
        gross_pnl_usdt=gross,
        open_fees_usdt=open_fees_usdt,
        estimated_close_fee_usdt=close_fee_est,
        funding_cashflow_usdt=funding_cf,
        net_pnl_usdt=net,
        net_pnl_pct_on_margin=net_pct,
    )


def compute_breakeven_prices(
    entries: Sequence[EntryFill],
    fee_engine: FeeEngine,
) -> Tuple[float, float]:
    """Returns (breakeven_gross_price, breakeven_net_price) for a SHORT position.

    breakeven_gross is simply the volume-weighted average entry price.

    breakeven_net solves net_pnl(price) == 0 for `price`, where the estimated
    close fee is itself a function of the close price:

        (avg_entry - price) * qty - open_fees - taker_rate * price * qty + funding_cf = 0
        => price = (avg_entry * qty - open_fees + funding_cf) / (qty * (1 + taker_rate))
    """
    total_qty = sum(e.qty for e in entries)
    if total_qty <= 0:
        return 0.0, 0.0

    be_gross = sum(e.price * e.qty for e in entries) / total_qty
    total_open_fees = sum(e.taker_fee_usdt for e in entries)
    funding_cf = fee_engine.total_funding_cashflow()
    taker_rate = fee_engine.schedule.taker_rate

    be_net = (be_gross * total_qty - total_open_fees + funding_cf) / (total_qty * (1.0 + taker_rate))
    return be_gross, be_net
