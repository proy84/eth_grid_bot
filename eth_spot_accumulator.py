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
value too. An idle USDT balance is deliberately NOT wanted; the minimum
threshold exists only to respect exchange technical limits, not as a
liquidity cushion to preserve.

ISOLATION: this file has zero effect on the rest of the bot unless called.
Deleting it entirely removes the feature -- the single call site in
main.py's `_execute_immediate_base_order` is wrapped in its own try/except
around a local, deferred import, so a missing/deleted file degrades to a
silent no-op rather than a crash (see that call site's comment). exchange.py,
strategy.py and fees.py are never modified by this feature and never import
from this file.

FAIL-SAFE: every exception anywhere in this module's call chain is caught
here, never propagated to the caller. Every non-fatal outcome (below
threshold, below exchange minimum, order failure) is logged, never raised.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger("eth_grid_bot.spot_accumulator")

SPOT_SYMBOL = "ETH/USDT"  # unified CCXT symbol for Bybit spot (category=spot) --
                          # distinct from the perpetual's "ETH/USDT:USDT"

# Bybit's lotSizeFilter.minOrderAmt for ETH/USDT spot -- value known/verified
# live on 2026-09-16 (5.0 USDT). Used ONLY as a fallback if the live lookup
# below fails; re-verify if Bybit's spot limits for this pair ever change.
FALLBACK_MIN_ORDER_COST_USDT = 5.0


async def maybe_buy_eth_spot(exchange_client: Any, enabled: bool, min_usdt_threshold: float) -> None:
    """Entry point: called once at every new cycle's genesis (bootstrap and
    every immediate re-open after a close/take-profit/trailing-stop). Never
    raises -- any failure anywhere below is logged as a WARNING and
    swallowed, so the caller in main.py can treat this call as fire-and-
    forget with zero risk to the main strategy's cycle-opening flow."""
    if not enabled:
        return
    try:
        await _buy_all_free_usdt(exchange_client, min_usdt_threshold)
    except Exception:
        logger.warning("Accumulo spot ETH: fallimento imprevisto, ignorato -- il bot prosegue normalmente.",
                        exc_info=True)


async def _buy_all_free_usdt(exchange_client: Any, min_usdt_threshold: float) -> None:
    # Reuses the SAME already-authenticated ccxt clients the rest of the bot
    # uses (via the passed-in ExchangeClient) instead of opening a second
    # connection -- exchange.py itself is never modified to know about spot
    # trading, so this reaches into the underlying ccxt instances directly.
    private = exchange_client._private
    public = exchange_client._public

    try:
        balance = await exchange_client._retry(private.fetch_balance)
        free_usdt = float((balance.get("USDT") or {}).get("free") or 0.0)
    except Exception:
        logger.warning("Accumulo spot ETH: lettura del saldo USDT libero fallita -- nessun acquisto questo ciclo.",
                        exc_info=True)
        return

    if free_usdt < min_usdt_threshold:
        logger.info(
            "Accumulo spot ETH: saldo USDT libero=%.4f sotto la soglia minima configurata=%.2f -- nessun acquisto.",
            free_usdt, min_usdt_threshold,
        )
        return

    min_cost = FALLBACK_MIN_ORDER_COST_USDT
    try:
        market = public.market(SPOT_SYMBOL)
        live_min = (market.get("limits") or {}).get("cost", {}).get("min")
        if live_min:
            min_cost = float(live_min)
    except Exception:
        logger.warning(
            "Accumulo spot ETH: lettura del minimo live dall'exchange fallita, uso il fallback %.2f USDT "
            "(valore noto al 2026-09-16, verificare se cambiato).",
            FALLBACK_MIN_ORDER_COST_USDT, exc_info=True,
        )

    if free_usdt < min_cost:
        logger.info(
            "Accumulo spot ETH: saldo USDT libero=%.4f sotto il minimo ordine dell'exchange=%.4f -- nessun acquisto.",
            free_usdt, min_cost,
        )
        return

    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    try:
        order = await exchange_client._retry(
            private.create_market_buy_order_with_cost, SPOT_SYMBOL, free_usdt,
        )
        # Parsing the response is included in THIS try block on purpose (not
        # split into a separate one after `return`-ing from except): a
        # malformed/unexpected response shape must log the same specific
        # "ordine FALLITO" message below, not silently fall through to the
        # generic catch-all in maybe_buy_eth_spot's own try/except.
        filled_qty = float(order.get("filled") or order.get("amount") or 0.0)
        avg_price = float(order.get("average") or order.get("price") or 0.0)
    except Exception:
        logger.warning(
            "[%s UTC] Accumulo spot ETH: ordine FALLITO o risposta inattesa (saldo libero=%.4f USDT, minimo richiesto=%.4f).",
            ts, free_usdt, min_cost, exc_info=True,
        )
        return

    logger.info(
        "[%s UTC] Accumulo spot ETH: ACQUISTATI %.6f ETH a ~%.2f USDT (speso %.4f USDT, minimo richiesto %.4f).",
        ts, filled_qty, avg_price, free_usdt, min_cost,
    )
