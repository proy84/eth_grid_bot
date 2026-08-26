"""
exchange.py

CCXT gateway to Bybit V5 Perpetual Futures, wired for Bybit's Demo Trading
environment.

IMPORTANT (discovered empirically while wiring this up): Bybit's Demo
Trading host (`https://api-demo.bybit.com`) only supports a narrow set of
AUTHENTICATED endpoint categories -- order/position/wallet management. Any
authenticated request outside that set (including the currency-metadata
lookup CCXT's `load_markets()` triggers automatically once an API key is
present, and market-data reads like klines/ticker/funding in general) gets
rejected with `retCode 10032 "Demo trading are not supported"`. Since a
single CCXT instance calls `load_markets()` internally before most unified
methods, pointing BOTH public and private URLs at the demo host (the
naive approach) makes EVERYTHING fail, including legitimate trading calls.

The fix used here: two CCXT instances.
  - `self._public`  -- unauthenticated, default (production) public host.
    Used for market data: candles, ticker, funding rate, market metadata.
    Market data is identical between demo and mainnet (same order book),
    so there is no correctness downside to reading it from production.
  - `self._private` -- authenticated with apiKey/secret, `private` URL
    pinned to the Bybit DEMO host WHEN `USE_TESTNET=true` (.env, default).
    With `USE_TESTNET=false` it uses the default production host instead --
    REAL FUNDS. Used only for orders, positions, balance, leverage and
    margin mode. Its market catalogue is seeded from `self._public` via
    `set_markets()` so it never has to call the demo-incompatible
    `load_markets()` itself.

Both apiKey and secret are read from the `BYBIT_API_KEY` / `BYBIT_API_SECRET`
environment variables (via `.env`, see `.env.example` -- loaded by
`strategy.py`'s `load_dotenv()` call), never committed to config.json. This
is what originally fixed the "bybit requires apiKey credential"
AuthenticationError, which was caused by silently handing CCXT an empty
string.

Responsibilities:
  - Connection setup: leverage, CROSS margin mode, demo/production URL split.
  - Last CLOSED candle lookup, at whatever timeframe config.json specifies.
  - Market SHORT order placement.
  - Open position lookup.
  - Market close of the open position.
  - Automatic retry with exponential backoff on transient network errors.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

import ccxt.async_support as ccxt_async  # type: ignore[import-untyped]
from ccxt.base.errors import ExchangeError, NetworkError  # type: ignore[import-untyped]

from strategy import StrategyConfig

logger = logging.getLogger("eth_grid_bot.exchange")

# Bybit V5 demo trading environment -- see https://bybit-exchange.github.io/docs/v5/demo
# Used ONLY for the authenticated (private) client; see module docstring.
BYBIT_DEMO_BASE_URL = "https://api-demo.bybit.com"


@dataclass(frozen=True)
class ClosedCandle:
    timestamp_ms: int
    close: float


@dataclass(frozen=True)
class FilledOrder:
    order_id: str
    side: str  # "sell" (open short) | "buy" (close short)
    price: float
    qty: float
    notional_usdt: float
    timestamp_ms: int


@dataclass(frozen=True)
class OpenPosition:
    symbol: str
    side: str
    qty: float
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    leverage: float


class ExchangeClient:
    """Async CCXT wrapper around Bybit V5 (demo trading), scoped to a single symbol."""

    def __init__(self, cfg: StrategyConfig, max_retries: int = 5, retry_base_delay_sec: float = 1.0):
        self.cfg = cfg
        self.max_retries = max_retries
        self.retry_base_delay_sec = retry_base_delay_sec

        api_key, api_secret = self._resolve_credentials(cfg)

        options = dict(cfg.exchange_options)
        options.setdefault("defaultType", "swap")
        options.setdefault("adjustForTimeDifference", True)

        # Public market data: unauthenticated, default production host.
        self._public = ccxt_async.bybit({
            "enableRateLimit": True,
            "options": options,
        })

        # Private trading/account: authenticated. USE_TESTNET=true (.env, default)
        # pins it to Bybit's Demo Trading host (paper money); USE_TESTNET=false
        # leaves it on the default production host -- REAL FUNDS.
        private_urls = {"api": {"private": BYBIT_DEMO_BASE_URL}} if cfg.use_testnet else {}
        self._private = ccxt_async.bybit({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": options,
            "urls": private_urls,
        })

        if cfg.use_testnet:
            logger.info("ExchangeClient configured: symbol=%s timeframe=%s mode=DEMO (%s) apiKey=%s...",
                        cfg.symbol, cfg.timeframe, BYBIT_DEMO_BASE_URL, api_key[:4] if api_key else "")
        else:
            logger.warning("ExchangeClient configured: symbol=%s timeframe=%s mode=PRODUCTION (LIVE FUNDS) "
                            "apiKey=%s...", cfg.symbol, cfg.timeframe, api_key[:4] if api_key else "")

    @staticmethod
    def _resolve_credentials(cfg: StrategyConfig) -> Tuple[str, str]:
        """Credentials always come from config.json (`exchange.api_key` /
        `exchange.api_secret`). `BYBIT_API_KEY` / `BYBIT_API_SECRET` env vars,
        if set, take precedence -- handy for CI or shared machines without
        touching the config file. Fails fast with a clear error instead of
        silently handing CCXT an empty string, which is what previously
        surfaced as a confusing `AuthenticationError` deep inside
        `create_order` instead of at startup."""
        api_key = (os.environ.get("BYBIT_API_KEY") or cfg.api_key or "").strip()
        api_secret = (os.environ.get("BYBIT_API_SECRET") or cfg.api_secret or "").strip()

        if not api_key or not api_secret:
            raise ValueError(
                "Missing Bybit credentials: set 'exchange.api_key' and 'exchange.api_secret' "
                "in config.json, or export BYBIT_API_KEY / BYBIT_API_SECRET."
            )
        return api_key, api_secret

    # -- lifecycle -----------------------------------------------------------

    async def setup(self) -> None:
        # Load markets unauthenticated (production) and seed them into the demo
        # client directly -- calling load_markets() on the demo client itself
        # would trigger CCXT's currency-metadata lookup, which the demo host
        # rejects (retCode 10032).
        markets = await self._retry(self._public.load_markets)
        self._private.set_markets(markets)

        # Bybit demo signing is strict about clock skew; sync explicitly rather
        # than relying on lazy/implicit correction.
        await self._retry(self._private.load_time_difference)

        try:
            await self._retry(self._private.set_margin_mode, self.cfg.margin_mode, self.cfg.symbol)
        except ExchangeError as exc:
            logger.info("set_margin_mode(%s, %s): %s (likely already set)",
                        self.cfg.margin_mode, self.cfg.symbol, exc)

        try:
            await self._retry(self._private.set_leverage, self.cfg.leverage, self.cfg.symbol)
        except ExchangeError as exc:
            logger.info("set_leverage(%dx, %s): %s (likely already set)",
                        self.cfg.leverage, self.cfg.symbol, exc)

        logger.info("Exchange ready: symbol=%s timeframe=%s leverage=%dx margin=%s "
                    "[market data: production public | trading: DEMO %s]",
                    self.cfg.symbol, self.cfg.timeframe, self.cfg.leverage, self.cfg.margin_mode,
                    BYBIT_DEMO_BASE_URL)

    async def close(self) -> None:
        await self._public.close()
        await self._private.close()

    # -- market data (production, public, unauthenticated) -------------------

    async def fetch_last_closed_candle(self) -> ClosedCandle:
        """Returns the most recently CLOSED candle at `cfg.timeframe`
        (e.g. '5m', '1h') -- never the still-forming one."""
        ohlcv = await self._retry(self._public.fetch_ohlcv, self.cfg.symbol,
                                   timeframe=self.cfg.timeframe, limit=3)
        closed = ohlcv[-2]
        return ClosedCandle(timestamp_ms=int(closed[0]), close=float(closed[4]))

    async def fetch_closed_candles(self, timeframe: str, count: int) -> List[ClosedCandle]:
        """Returns the last `count` CLOSED candles at an arbitrary `timeframe`
        (never the still-forming one), oldest first -- used for RSI, which
        needs a window of history rather than a single close."""
        ohlcv = await self._retry(self._public.fetch_ohlcv, self.cfg.symbol,
                                   timeframe=timeframe, limit=count + 2)
        closed = ohlcv[:-1][-count:]
        return [ClosedCandle(timestamp_ms=int(c[0]), close=float(c[4])) for c in closed]

    async def fetch_mark_price(self) -> float:
        ticker = await self._retry(self._public.fetch_ticker, self.cfg.symbol)
        price = ticker.get("last") or ticker.get("close")
        return float(price)

    async def fetch_current_funding_rate(self) -> float:
        funding = await self._retry(self._public.fetch_funding_rate, self.cfg.symbol)
        return float(funding.get("fundingRate") or 0.0)

    def compute_qty_from_notional(self, notional_usdt: float, price: float) -> float:
        raw_qty = notional_usdt / price
        return float(self._public.amount_to_precision(self.cfg.symbol, raw_qty))

    def min_order_qty(self) -> float:
        """Exchange-enforced minimum tradable amount for the symbol (e.g. 0.01
        ETH on ETH/USDT:USDT). Read live from the market catalogue rather than
        hard-coded, so it stays correct even if Bybit changes it."""
        market = self._public.market(self.cfg.symbol)
        return float((market.get("limits") or {}).get("amount", {}).get("min") or 0.0)

    async def fetch_total_equity(self) -> float:
        """Bybit V5 Unified Account total equity in USD, across ALL collateral
        assets -- NOT just USDT. CCXT's unified `balance['USDT']['total']`
        only reflects the USDT-denominated sub-balance, which massively
        understates real equity on an account also holding other coins as
        collateral (confirmed empirically on this demo account: ~16 USDT-only
        vs ~2992 real total equity). Reads Bybit's own
        `info.result.list[0].totalEquity` field instead, which already
        aggregates every collateral asset's USD value."""
        balance = await self._retry(self._private.fetch_balance)
        try:
            account = balance["info"]["result"]["list"][0]
            return float(account["totalEquity"])
        except (KeyError, IndexError, TypeError, ValueError):
            logger.exception("Unexpected balance response shape while reading totalEquity; raw info: %s",
                              balance.get("info"))
            return 0.0

    # -- orders / account (Bybit DEMO, authenticated) -------------------------

    async def place_market_short(self, qty: float) -> FilledOrder:
        order = await self._create_order_and_await_fill(self.cfg.symbol, "sell", qty)
        filled = self._to_filled_order(order, side="sell")
        logger.info("SHORT opened: qty=%.6f price=%.2f notional=%.2f", filled.qty, filled.price,
                    filled.notional_usdt)
        return filled

    async def fetch_open_positions(self) -> List[OpenPosition]:
        raw = await self._retry(self._private.fetch_positions, [self.cfg.symbol])
        positions: List[OpenPosition] = []
        for p in raw:
            qty = float(p.get("contracts") or 0.0)
            if qty <= 0:
                continue
            positions.append(OpenPosition(
                symbol=str(p.get("symbol") or self.cfg.symbol),
                side=str(p.get("side") or "short"),
                qty=qty,
                entry_price=float(p.get("entryPrice") or 0.0),
                mark_price=float(p.get("markPrice") or 0.0),
                unrealized_pnl=float(p.get("unrealizedPnl") or 0.0),
                leverage=float(p.get("leverage") or self.cfg.leverage),
            ))
        return positions

    async def close_position_market(self) -> Optional[FilledOrder]:
        positions = await self.fetch_open_positions()
        position = next((p for p in positions if p.qty > 0), None)
        if position is None:
            logger.warning("close_position_market: no open position found on %s.", self.cfg.symbol)
            return None

        order = await self._create_order_and_await_fill(self.cfg.symbol, "buy", position.qty,
                                                          params={"reduceOnly": True})
        filled = self._to_filled_order(order, side="buy")
        logger.info("Position closed: qty=%.6f price=%.2f notional=%.2f", filled.qty, filled.price,
                    filled.notional_usdt)
        return filled

    # -- internals -----------------------------------------------------------

    async def _create_order_and_await_fill(self, symbol: str, side: str, qty: float,
                                            params: Optional[dict] = None,
                                            poll_attempts: int = 5, poll_delay_sec: float = 0.3) -> dict:
        """Bybit V5's create-order response is a bare acknowledgement (order id
        only) -- it does NOT include the fill price/quantity, even for a market
        order. Polling `fetch_order` right after is required to get the real
        average price and filled quantity; using the create-order ack directly
        silently produced price=0.00/qty=0.000000 fills."""
        order = await self._retry(self._private.create_order, symbol, "market", side, qty, params=params or {})
        order_id = order.get("id")
        if not order_id:
            logger.error("create_order returned no order id for %s %s qty=%.6f: %s", side, symbol, qty, order)
            return order

        for attempt in range(poll_attempts):
            detail = await self._retry(self._private.fetch_order, order_id, symbol,
                                        params={"acknowledged": True})
            if detail.get("status") == "closed" and float(detail.get("filled") or 0.0) > 0:
                return detail
            await asyncio.sleep(poll_delay_sec)

        logger.warning("Order %s (%s %s qty=%.6f) did not report a fill after %d polls; "
                        "using the last known order state.", order_id, side, symbol, qty, poll_attempts)
        return detail

    @staticmethod
    def _to_filled_order(order: dict, side: str) -> FilledOrder:
        price = float(order.get("average") or order.get("price") or 0.0)
        qty = float(order.get("filled") or order.get("amount") or 0.0)
        ts = order.get("timestamp") or int(time.time() * 1000)
        return FilledOrder(order_id=str(order.get("id", "")), side=side, price=price, qty=qty,
                            notional_usdt=price * qty, timestamp_ms=int(ts))

    async def _retry(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Retries transient CCXT network errors with exponential backoff.
        Exchange-level rejections (bad request, insufficient margin, auth
        failures, unsupported-on-demo, etc.) are NOT retried -- they are
        raised immediately."""
        attempt = 0
        name = getattr(func, "__name__", str(func))
        while True:
            try:
                return await func(*args, **kwargs)
            except NetworkError as exc:
                attempt += 1
                if attempt > self.max_retries:
                    logger.error("Max retries (%d) exceeded calling %s: %s", self.max_retries, name, exc)
                    raise
                delay = self.retry_base_delay_sec * (2 ** (attempt - 1))
                logger.warning("Network error on %s (attempt %d/%d): %s -- retrying in %.1fs",
                               name, attempt, self.max_retries, exc, delay)
                await asyncio.sleep(delay)
            except ExchangeError as exc:
                logger.error("Exchange rejected %s: %s", name, exc)
                raise
