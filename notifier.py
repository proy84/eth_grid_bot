"""
notifier.py

Optional, fully isolated Telegram notification: fires once per closed cycle
(fixed take-profit or trailing stop), reporting cycle_id, net PnL, total
equity (USDT + EUR), and the collateral breakdown per coin. Never touches
grid/Fibonacci/Break-Even/order logic in any way.

ISOLATION: this file has zero effect on the rest of the bot unless called.
Deleting it entirely removes the feature -- the single call site in
main.py's `_close_cycle` is wrapped in its own try/except around a local,
deferred import, so a missing/deleted file degrades to a silent no-op
rather than a crash. exchange.py, strategy.py and fees.py are never
modified by this feature and never import from this file.

FAIL-SAFE: the ENTIRE body of `_notify_cycle_closed_async` -- the coroutine
actually passed to `asyncio.create_task()` in main.py -- runs under one
outer try/except, so no exception raised at ANY point during its async
execution (not just at creation time) can ever end up unretrieved on the
Task; every non-fatal outcome is logged as a WARNING and swallowed inside
this module, never propagated to the caller. Each optional piece of the
message (equity, EUR conversion, collateral breakdown) has its own inner
try/except: if one fails, it is simply omitted from the message instead of
blocking the whole notification.
"""

from __future__ import annotations

import asyncio
import logging
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("eth_grid_bot.notifier")

_TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"
EUR_RATE_SYMBOL = "USDT/EUR"  # direct Bybit spot pair -- no external FX API needed
_warned_once = False


def _credentials() -> tuple[str, str] | None:
    global _warned_once
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        if not _warned_once:
            logger.info(
                "Notifiche Telegram disabilitate: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID non configurati in .env."
            )
            _warned_once = True
        return None
    return token, chat_id


def _send_sync(token: str, chat_id: str, text: str) -> None:
    url = _TELEGRAM_API_URL.format(token=token)
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text, "parse_mode": "HTML"}).encode("utf-8")
    with urllib.request.urlopen(url, data=data, timeout=10) as resp:
        resp.read()


async def notify_cycle_closed(exchange_client: Any, enabled: bool, cycle_id: int, net_pnl_usdt: float) -> None:
    """Entry point. This whole coroutine is what main.py passes directly to
    `asyncio.create_task()` -- it must `await` the real work here, NOT
    schedule yet another nested `create_task()`, otherwise main.py's tracked
    task would complete instantly (right after scheduling) while the actual
    work runs in a second, untracked task nobody holds a reference to --
    exactly the unretrieved-exception / premature-GC risk this module's
    whole fail-safe design exists to avoid. Safe to call even if Telegram
    credentials are missing (silently does nothing) or if `enabled=False`."""
    if not enabled:
        return
    await _notify_cycle_closed_async(exchange_client, cycle_id, net_pnl_usdt)


async def _notify_cycle_closed_async(exchange_client: Any, cycle_id: int, net_pnl_usdt: float) -> None:
    try:
        creds = _credentials()
        if creds is None:
            return
        token, chat_id = creds

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        emoji = "\U0001F7E2" if net_pnl_usdt >= 0 else "\U0001F534"  # 🟢 / 🔴
        sign = "+" if net_pnl_usdt >= 0 else ""

        lines = [
            f"<b>Ciclo #{cycle_id} chiuso</b>",
            f"{emoji} Net PnL: <b>{sign}{net_pnl_usdt:.4f} USDT</b>",
        ]

        equity_usdt = None
        try:
            equity_usdt = await exchange_client.fetch_total_equity()
        except Exception:
            logger.warning("Notifica ciclo #%d: lettura equity fallita (omessa dal messaggio).",
                            cycle_id, exc_info=True)

        if equity_usdt:
            equity_line = f"\U0001F4B0 Equity totale: <b>{equity_usdt:.2f} USDT</b>"  # 💰
            try:
                ticker = await exchange_client._retry(exchange_client._public.fetch_ticker, EUR_RATE_SYMBOL)
                rate = float(ticker.get("last"))
                equity_line += f" (~<b>{equity_usdt * rate:.2f} EUR</b>)"
            except Exception:
                logger.warning("Notifica ciclo #%d: conversione USDT->EUR fallita (omessa dal messaggio).",
                                cycle_id, exc_info=True)
            lines.append(equity_line)

        try:
            balance = await exchange_client._retry(exchange_client._private.fetch_balance)
            coins = balance["info"]["result"]["list"][0]["coin"]
            collateral_lines = []
            for c in coins:
                if not c.get("collateralSwitch"):
                    continue
                qty = float(c.get("walletBalance") or 0.0)
                if qty <= 0:
                    continue
                usd_value = float(c.get("usdValue") or 0.0)
                collateral_lines.append(f"  • {c.get('coin')}: {qty:.6f} (~{usd_value:.2f} USDT)")
            if collateral_lines:
                lines.append("\U0001F4CA Collaterale attivo:")  # 📊
                lines.extend(collateral_lines)
        except Exception:
            logger.warning("Notifica ciclo #%d: lettura collaterale per coin fallita (omessa dal messaggio).",
                            cycle_id, exc_info=True)

        lines.append(f"⏱️ Chiuso: {ts}")  # ⏱️
        text = "\n".join(lines)

        try:
            await asyncio.to_thread(_send_sync, token, chat_id, text)
        except Exception:
            logger.warning("Notifica ciclo #%d: invio Telegram fallito.", cycle_id, exc_info=True)
            return

        logger.debug("Notifica ciclo #%d inviata su Telegram alle %s: %s",
                     cycle_id, ts, text.replace("\n", " | "))
    except Exception:
        logger.warning("Notifica ciclo #%d: fallimento imprevisto, ignorato -- il bot prosegue normalmente.",
                        cycle_id, exc_info=True)
