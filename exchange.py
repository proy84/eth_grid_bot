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
import uuid
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

        # ccxt's default is 10000ms -- too tight on a mobile/Termux connection,
        # where a request that's merely SLOW (not actually failed) can trip a
        # client-side timeout, turning into a NetworkError that triggers a
        # retry of an order-placing call that may have already reached Bybit.
        # Raised to reduce how often that retry path is even entered; it does
        # not by itself guarantee a request never times out.
        REQUEST_TIMEOUT_MS = 25000

        # Public market data: unauthenticated, default production host.
        self._public = ccxt_async.bybit({
            "enableRateLimit": True,
            "timeout": REQUEST_TIMEOUT_MS,
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
            "timeout": REQUEST_TIMEOUT_MS,
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
            await self._retry(self._private.set_margin_mode, self.cfg.margin_mode, self.cfg.symbol,
                               quiet_exchange_errors=True)
        except ExchangeError as exc:
            logger.debug("set_margin_mode(%s, %s): %s (likely already set)",
                         self.cfg.margin_mode, self.cfg.symbol, exc)

        try:
            await self._retry(self._private.set_leverage, self.cfg.leverage, self.cfg.symbol,
                               quiet_exchange_errors=True)
        except ExchangeError as exc:
            logger.debug("set_leverage(%dx, %s): %s (likely already set)",
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

    async def fetch_realized_funding(self, since_ms: Optional[int] = None, limit: int = 50) -> List[Tuple[str, int, float]]:
        """REALIZED funding settlements actually credited/debited to the
        account for this symbol -- (id, timestamp_ms, cashflow_usdt). NOT the
        current/estimated funding rate: Bybit settles funding roughly every
        8h (confirmed empirically: timestamps land exactly on 00:00/08:00/
        16:00 UTC), not continuously. Polling the live rate on a short
        interval and recording a payment on every poll (the previous
        approach) grossly over-counts funding income for a position held
        less than one real settlement -- this reads the exchange's own
        settlement ledger instead, so a short-lived position correctly
        accrues ~0 funding rather than one fabricated payment per poll."""
        raw = await self._retry(self._private.fetch_funding_history, self.cfg.symbol, since_ms, limit)
        result: List[Tuple[str, int, float]] = []
        for entry in raw:
            eid, ts, amount = entry.get("id"), entry.get("timestamp"), entry.get("amount")
            if eid is None or ts is None or amount is None:
                continue
            result.append((str(eid), int(ts), float(amount)))
        return result

    def compute_qty_from_notional(self, notional_usdt: float, price: float) -> float:
        raw_qty = notional_usdt / price
        return float(self._public.amount_to_precision(self.cfg.symbol, raw_qty))

    def min_order_qty(self) -> float:
        """Exchange-enforced minimum tradable amount for the symbol (e.g. 0.01
        ETH on ETH/USDT:USDT). Read live from the market catalogue rather than
        hard-coded, so it stays correct even if Bybit changes it. Never raises:
        `.market()` looking up an unexpected/missing symbol would otherwise
        propagate out of `_effective_base_notional`, which is called from the
        bootstrap path at startup with no enclosing try/except -- an
        unhandled exception there would crash the whole process before the
        main loops even start. Returns 0.0 (== "no minimum enforced", same
        as an exchange that genuinely reports none) on any lookup failure."""
        try:
            market = self._public.market(self.cfg.symbol)
            return float((market.get("limits") or {}).get("amount", {}).get("min") or 0.0)
        except Exception:
            logger.exception("Failed to look up min order qty for %s; treating as no minimum enforced.",
                              self.cfg.symbol)
            return 0.0

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
        logger.debug("SHORT opened: qty=%.6f price=%.2f notional=%.2f", filled.qty, filled.price,
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
        logger.debug("Position closed: qty=%.6f price=%.2f notional=%.2f", filled.qty, filled.price,
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
        silently produced price=0.00/qty=0.000000 fills.

        A single `clientOrderId` (Bybit's `orderLinkId`) is generated ONCE here
        and reused across every retry attempt for THIS call -- not a fresh one
        per attempt. See `_create_order_with_dedup` for why a NetworkError on
        `create_order` specifically CHECKS for that id on Bybit before ever
        resubmitting, rather than resubmitting first and only reacting to an
        explicit duplicate rejection: a blind resubmit risks a real duplicate
        position if Bybit's own request actually went through (response lost
        to the network on our end, more likely on mobile than on a stable
        connection) -- and, observed live, Bybit's own orderLinkId-uniqueness
        check does not always catch two near-simultaneous submissions of the
        same id, so waiting for it to reject the resubmission is not enough
        on its own."""
        order_params = dict(params or {})
        client_order_id = uuid.uuid4().hex[:24]
        order_params["clientOrderId"] = client_order_id

        order = await self._create_order_with_dedup(symbol, side, qty, order_params, client_order_id)

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
    def _is_duplicate_client_order_id_error(exc: Exception) -> bool:
        """True for Bybit's specific 'this clientOrderId/orderLinkId was
        already used' rejections (retCode 110072 'OrderLinkedID is duplicate',
        170141/12141 'Duplicate clientOrderId') -- the exact signal that a
        retried create_order call is NOT a genuine new order, but Bybit
        confirming it already has the original. Matched on the raw message
        text since ccxt re-raises Bybit's response body verbatim rather than
        exposing retCode as a structured field."""
        text = str(exc).lower()
        if any(code in text for code in ("110072", "170141", "12141")):
            return True
        return "duplicate" in text and ("orderlinkid" in text or "clientorderid" in text or "order-link" in text)

    async def _try_find_order_by_client_id(self, symbol: str, client_order_id: str) -> Optional[dict]:
        """Looks up an order by `clientOrderId` (Bybit's `orderLinkId`) without
        raising -- checks closed orders first since a market order normally
        fills within the time it takes Bybit to even respond to the original
        request, falling back to open orders for the rare case it hasn't yet.
        Returns None if nothing matches (a genuine "not found", not an error)."""
        for fetch_fn in (self._private.fetch_closed_orders, self._private.fetch_open_orders):
            try:
                orders = await self._retry(fetch_fn, symbol, None, 10, params={"orderLinkId": client_order_id})
            except Exception:
                logger.exception("Failed to look up order by clientOrderId=%s via %s",
                                  client_order_id, getattr(fetch_fn, "__name__", fetch_fn))
                continue
            match = next((o for o in orders if o.get("clientOrderId") == client_order_id), None)
            if match is not None:
                return match
        return None

    async def _create_order_with_dedup(self, symbol: str, side: str, qty: float,
                                        order_params: dict, client_order_id: str) -> dict:
        """create_order with CHECK-BEFORE-RESUBMIT semantics on network error.

        The naive approach -- resubmit on NetworkError and rely on Bybit
        rejecting the resubmission as a duplicate clientOrderId -- was tried
        first and observed live to be insufficient: Bybit's own orderLinkId-
        uniqueness check does not appear to be atomic against two requests
        carrying the same id arriving close together, so BOTH can come back
        as genuine, separately filled orders with no duplicate error raised
        at all (two distinct order ids, no error in our own logs -- exactly
        what a Termux trade-history screenshot showed).

        So here, a NetworkError does NOT immediately resubmit: it first asks
        Bybit whether an order with our `client_order_id` already exists. If
        it does, that original order is used and no second request is ever
        sent. Only a genuine "nothing there yet" makes it resubmit. This
        moves the race from "hope Bybit's dedup catches it" to "never send a
        second real order-placing request without checking first" -- the
        check-then-act still isn't perfectly atomic either, but it needs
        Bybit to be slow AND our check to land in a much narrower window,
        instead of relying on a rejection that has already been seen to not
        always happen."""
        attempt = 0
        while True:
            try:
                return await self._private.create_order(symbol, "market", side, qty, params=order_params)
            except NetworkError as exc:
                attempt += 1
                if attempt > self.max_retries:
                    logger.error("Max retries (%d) exceeded calling create_order: %s", self.max_retries, exc)
                    raise
                delay = self.retry_base_delay_sec * (2 ** (attempt - 1))
                logger.warning(
                    "Network error on create_order (attempt %d/%d): %s -- checking whether it actually "
                    "went through (clientOrderId=%s) before resubmitting.",
                    attempt, self.max_retries, exc, client_order_id,
                )
                await asyncio.sleep(delay)
                existing = await self._try_find_order_by_client_id(symbol, client_order_id)
                if existing is not None:
                    logger.warning(
                        "Found the ORIGINAL order (id=%s) already on Bybit for clientOrderId=%s -- the "
                        "network error only lost the response, the request itself had gone through. "
                        "Using it instead of resubmitting.", existing.get("id"), client_order_id,
                    )
                    return existing
                logger.warning("No order found yet for clientOrderId=%s -- safe to resubmit.", client_order_id)
            except ExchangeError as exc:
                if not self._is_duplicate_client_order_id_error(exc):
                    logger.error("Exchange rejected create_order: %s", exc)
                    raise
                logger.warning(
                    "create_order (clientOrderId=%s) rejected as a duplicate by Bybit -- a prior attempt "
                    "already went through. Recovering that original order (%s).", client_order_id, exc,
                )
                existing = await self._try_find_order_by_client_id(symbol, client_order_id)
                if existing is None:
                    raise RuntimeError(
                        f"Bybit reported clientOrderId={client_order_id} as a duplicate, but no matching "
                        f"order could be found on {symbol} -- cannot recover the original fill."
                    )
                return existing

    @staticmethod
    def _to_filled_order(order: dict, side: str) -> FilledOrder:
        price = float(order.get("average") or order.get("price") or 0.0)
        qty = float(order.get("filled") or order.get("amount") or 0.0)
        ts = order.get("timestamp") or int(time.time() * 1000)
        return FilledOrder(order_id=str(order.get("id", "")), side=side, price=price, qty=qty,
                            notional_usdt=price * qty, timestamp_ms=int(ts))

    async def _retry(self, func: Callable[..., Any], *args: Any,
                      quiet_exchange_errors: bool = False, **kwargs: Any) -> Any:
        """Retries transient CCXT network errors with exponential backoff.
        Exchange-level rejections (bad request, insufficient margin, auth
        failures, unsupported-on-demo, etc.) are NOT retried -- they are
        raised immediately.

        `quiet_exchange_errors=True` drops the generic ExchangeError log to
        DEBUG instead of ERROR -- for call sites like `set_leverage`/
        `set_margin_mode` in `setup()` where the overwhelmingly common
        "rejection" is just Bybit saying it's already set the way we asked,
        which the caller already logs its own friendly INFO line for. Left
        at ERROR (the default) everywhere else, where an ExchangeError
        really does mean something needs attention."""
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
                if quiet_exchange_errors:
                    logger.debug("Exchange rejected %s: %s", name, exc)
                else:
                    logger.error("Exchange rejected %s: %s", name, exc)
                raise
