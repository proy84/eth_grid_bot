"""
eth_spot_accumulator.py

Optional, fully isolated hedge: buys ETH on the SPOT market (category=spot,
symbol "ETH/USDT" -- distinct from the short/perpetual strategy's
"ETH/USDT:USDT") at every new cycle's genesis, spending the entire free USDT
balance available at that moment. Never touches grid/Fibonacci/Break-Even
logic or the futures position in any way.

Rationale (context only, not enforced by code): the bot is always short ETH
on perpetuals; converting idle USDT into spot ETH creates a partial hedge --
if ETH rises (the risk scenario for the short), the spot ETH held rises in
value too. An idle USDT balance is deliberately NOT wanted.

NO MINIMUM CHECKS BY DESIGN: this module does not pre-check any threshold
(neither a configured minimum nor the exchange's live minimum order size) --
it always attempts the purchase with whatever free USDT is available (as
long as it's positive), and simply lets Bybit accept or reject the order.
A rejection (e.g. below Bybit's real minimum) is absorbed by the same
fail-safe as any other failure below: logged, swallowed, zero impact on the
main strategy.

ISOLATION: this file has zero effect on the rest of the bot unless called.
Deleting it entirely removes the feature -- the single call site in
main.py's `_execute_immediate_base_order` is wrapped in its own try/except
around a local, deferred import, so a missing/deleted file degrades to a
silent no-op rather than a crash (see that call site's comment). exchange.py,
strategy.py and fees.py are never modified by this feature and never import
from this file.

FAIL-SAFE: every exception anywhere in this module's call chain is caught
here, never propagated to the caller. Every non-fatal outcome (no free
balance, order rejected) is logged, never raised.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger("eth_grid_bot.spot_accumulator")

SPOT_SYMBOL = "ETH/USDT"  # unified CCXT symbol for Bybit spot (category=spot) --
                          # distinct from the perpetual's "ETH/USDT:USDT"


async def maybe_buy_eth_spot(exchange_client: Any, enabled: bool) -> None:
    """Entry point: called once at every new cycle's genesis (bootstrap and
    every immediate re-open after a close/take-profit/trailing-stop). Never
    raises -- any failure anywhere below is logged as a WARNING and
    swallowed, so the caller in main.py can treat this call as fire-and-
    forget with zero risk to the main strategy's cycle-opening flow."""
    if not enabled:
        return
    try:
        await _buy_all_free_usdt(exchange_client)
    except Exception:
        logger.warning("Accumulo spot ETH: fallimento imprevisto, ignorato -- il bot prosegue normalmente.",
                        exc_info=True)


async def _buy_all_free_usdt(exchange_client: Any) -> None:
    # Reuses the SAME already-authenticated ccxt clients the rest of the bot
    # uses (via the passed-in ExchangeClient) instead of opening a second
    # connection -- exchange.py itself is never modified to know about spot
    # trading, so this reaches into the underlying ccxt instance directly.
    private = exchange_client._private

    try:
        balance = await exchange_client._retry(private.fetch_balance)
        free_usdt = float((balance.get("USDT") or {}).get("free") or 0.0)
    except Exception:
        logger.warning("Accumulo spot ETH: lettura del saldo USDT libero fallita -- nessun acquisto questo ciclo.",
                        exc_info=True)
        return

    if free_usdt <= 0:
        logger.debug("Accumulo spot ETH: saldo USDT libero=%.4f <= 0 -- nessun acquisto.", free_usdt)
        return

    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    try:
        order = await exchange_client._retry(
            private.create_market_buy_order_with_cost, SPOT_SYMBOL, free_usdt,
            quiet_exchange_errors=True,
        )
        # Parsing the response is included in THIS try block on purpose (not
        # split into a separate one after `return`-ing from except): a
        # malformed/unexpected response shape must log the same specific
        # "ordine FALLITO" message below, not silently fall through to the
        # generic catch-all in maybe_buy_eth_spot's own try/except.
        filled_qty = float(order.get("filled") or order.get("amount") or 0.0)
        avg_price = float(order.get("average") or order.get("price") or 0.0)
    except Exception:
        # Covers a genuine Bybit rejection (e.g. below the exchange's real
        # minimum order size, which is no longer pre-checked here on purpose)
        # exactly the same way as any other failure -- logged, swallowed.
        # DEBUG, no traceback: on a small demo account this is the expected,
        # routine outcome almost every cycle, not something worth surfacing
        # on every run -- see LOG_LEVEL=DEBUG in main.py if it's ever needed.
        logger.debug(
            "[%s UTC] Accumulo spot ETH: ordine FALLITO o rifiutato da Bybit (saldo libero=%.4f USDT) "
            "-- nessun problema per il normale svolgimento della strategia.",
            ts, free_usdt,
        )
        return

    logger.info(
        "[%s UTC] Accumulo spot ETH: ACQUISTATI %.6f ETH a ~%.2f USDT (speso %.4f USDT).",
        ts, filled_qty, avg_price, free_usdt,
    )
