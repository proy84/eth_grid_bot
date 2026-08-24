# Documentazione tecnica — Only-Short Grid Bot (ETH/USDT Perpetual, Bybit)

> Generata per consumo da parte di un altro assistente AI senza accesso diretto al codice.
> Versione bot al momento della generazione: **v1.2** (log `[INFO] Avvio Bot Trading - v1.2`).
> Riferimenti a file/righe basati sullo stato del repository al commit `8ae27f8`.

---

## 1. Struttura del progetto

| File | Responsabilità |
|---|---|
| `main.py` | Orchestratore. Classe `GridBotOrchestrator`: cicli di vita del bot, i due loop asincroni principali (scheduler griglia + tick loop), esecuzione ordini, trailing stop / take profit, RSI tick mode, Neutral Zone, chiusura/riapertura ciclo, export stato, stop-file. Entry point (`main()` / `if __name__ == "__main__"`). |
| `strategy.py` | Logica pura, **senza I/O di rete**: `StrategyConfig` (caricamento `config.json` + `.env`), `RangeGrid` (griglia geometrica statica + re-indexing), `PositionManager` (tracking posizione/entrate), `fibonacci()`, `compute_rsi()`, `evaluate_grid_close()` (sizing ordini), `PlannedOrder`. Contiene anche `TrailingStopController`, una classe **non utilizzata** (vedi §7/§8). |
| `exchange.py` | Wrapper async su CCXT/Bybit V5. Classe `ExchangeClient`: due istanze CCXT (pubblica per market data, privata per trading), setup leva/margine, fetch candele/ticker/funding, piazzamento ordini market, lookup posizioni aperte, chiusura a mercato, retry con backoff, calcolo qty da notional e qty minima exchange. |
| `fees.py` | Matematica fee/funding/PnL: `FeeSchedule`, `FeeEngine`, `FundingPayment`, `EntryFill`, `compute_gross_pnl_short()`, `compute_net_pnl()`, `compute_breakeven_prices()` (break-even lordo e netto). |
| `analytics.py` | Persistenza cicli chiusi su `trade_history.json` (append-only) e calcolo statistiche aggregate (`AnalyticsEngine`, `CycleRecord`, `OrderRecord`, `AnalyticsSummary`): PnL medio, max drawdown, livello Fibonacci massimo, durata media ciclo, totali fee/funding. |
| `data_exporter.py` | Scrive `bot_state.json` (stato live, per una dashboard esterna non presente in questo repo) ad ogni tick, in modo atomico (file temporaneo + `replace`). Classe `DataExporter`, dataclass `LiveState`. |
| `config.json` | Configurazione non sensibile (vedi §6). |
| `.env` / `.env.example` | Credenziali e pochi override sensibili (`BYBIT_API_KEY`, `BYBIT_API_SECRET`, `USE_TESTNET`, `SYMBOL`, `GRID_STEP_PERCENT`). `.env` è in `.gitignore`, mai committato. |
| `requirements.txt` | Due dipendenze: `ccxt`, `python-dotenv`. |
| `.gitignore` | Esclude `__pycache__/`, `*.pyc`, `.env`, `*.log`, `*.txt` (con eccezione `!requirements.txt`), `/STOP`, `/bot_state.json`, `/trade_history.json`. |
| `bot_state.json` | **Output runtime**, rigenerato ad ogni tick da `data_exporter.py`. Non più tracciato su git (rimosso dal tracking perché causava conflitti di merge — vedi §8). |
| `trade_history.json` | **Output runtime**, append-only da `analytics.py`. Stesso discorso di sopra. |
| `state.json` | File presente sul disco ma **non referenziato da nessun modulo Python** — residuo di una versione precedente (probabilmente da prima della rinomina a `bot_state.json`). Dead file, sicuro da eliminare. |

Non esiste alcuna test suite nel repository (nessun file `test_*.py` o `*_test.py`).

---

## 2. Architettura e flusso di esecuzione

**Entry point**: `python main.py` → `main()` (main.py:860) → `asyncio.run(_run())` (main.py:862), con `except KeyboardInterrupt` per uno shutdown pulito su `CTRL+C`/`SIGINT`.

`_run()` (main.py:850):
1. Configura `logging.basicConfig` (livello INFO).
2. `cfg = StrategyConfig.load(CONFIG_PATH)` — legge `config.json` (path costante `"config.json"`, main.py:128) e sovrascrive alcuni campi da variabili d'ambiente.
3. Istanzia `bot = GridBotOrchestrator(cfg)`.
4. `try: await bot.start()` … `finally: await bot.stop()`.

**`GridBotOrchestrator.__init__`** (main.py:149-199) costruisce, in ordine: `ExchangeClient(cfg)`, `RangeGrid(base_price=0.0, step_pct=cfg.grid_step_pct)` (0.0 è solo un placeholder, sovrascritto subito dopo), `PositionManager()`, `FeeEngine(...)`, `AnalyticsEngine(cfg.trade_history_path)`, `DataExporter(cfg.state_export_path, self.analytics)`. `self.cycle_id` parte da `analytics.summary().completed_cycles + 1` (continua la numerazione tra riavvii, leggendo `trade_history.json`). Inizializza inoltre tutto lo stato di RSI/tick-mode/trailing-stop a "spento".

**`start()`** (main.py:203-225):
1. Log versione bot.
2. `await self.exchange.setup()` — carica i mercati, imposta margin mode e leva (vedi §5).
3. `await self._bootstrap_position()` — **primo momento in cui la griglia viene ancorata** (vedi sotto).
4. Log di stato + (se `stress_test.enabled`) un warning riepilogativo della modalità stress test.
5. `await asyncio.gather(self._grid_scheduler_loop(), self._tick_loop())` — le due coroutine principali girano **per sempre**, concorrenti, finché `self._stop_event` non viene settato.

**`stop()`** (main.py:227-229): setta `_stop_event` e chiude entrambi i client CCXT.

### Ciclo di vita di una posizione

**1. Apertura di un ciclo** — sempre e solo in due punti (mai altrove):
- `_bootstrap_position()` (main.py:233-261), chiamata una volta all'avvio: se `exchange.fetch_open_positions()` non trova nulla, ancora la griglia al prezzo corrente (`grid.full_reset(mark_price)`) e spara **immediatamente** (senza attendere una candela) un ordine base via `_execute_immediate_base_order(kind="range_zero_immediate")` — Fibonacci(1) × `base_notional_usdt`. Se invece trova una posizione esistente, la "adotta" come entrata sintetica unica (vedi §7/§8 per il limite noto qui) e **non** spara un nuovo ordine.
- `_close_cycle()` (main.py:707-765), dopo ogni chiusura (trailing stop o take profit): dopo aver chiuso a mercato, resetta lo stato (`_reset_state_after_close`, che richiama `grid.full_reset(ref_price)`) e riapre **immediatamente** un nuovo ciclo con lo stesso pattern di `_execute_immediate_base_order(kind="range_zero_reset_immediate")`.

**2. Gestione (mediazione/accumulo)** — due loop indipendenti che girano in parallelo:
- **`_grid_scheduler_loop()`** (main.py:306-326): ad ogni cadenza (vedi §3 "Griglia"/"RSI") chiama `_run_grid_evaluation()`, che legge il prezzo (candela chiusa in produzione, mark price live in stress test), calcola l'ordine via `evaluate_grid_close()`, eventualmente lo sospende per Neutral Zone, e lo esegue con `_execute_planned_order()`. **Non esiste mai un esito "idle"**: ad ogni valutazione parte sempre esattamente un ordine, a meno di sospensione Neutral Zone.
- **`_tick_loop()`** (main.py:462-469): ogni `tick_poll_interval_sec` (2s default) chiama `_run_tick()`, che aggiorna il funding, il prezzo, lo stato RSI/tick-mode, ricalcola il PnL netto, esporta lo stato live e valuta la condizione di uscita (trailing stop o take profit fisso).

**3. Chiusura** — `_close_cycle(mark_price)` (main.py:707): sotto `_position_lock`, chiude l'intera posizione a mercato (`exchange.close_position_market()`), registra un `CycleRecord` in `AnalyticsEngine` (PnL lordo/netto, fee totali, funding totale, livello Fibonacci massimo raggiunto), poi chiama `_reset_state_after_close()` (auto-compound del notional base, reset posizione/fee-engine/griglia/RSI/trailing, incremento `cycle_id`). Fuori dal lock, riapre immediatamente il ciclo successivo.

Un `asyncio.Lock` (`self._position_lock`) serializza `_execute_planned_order` e la porzione di `_close_cycle` che tocca la posizione, per evitare che una mediazione e una chiusura si interlaccino a metà (race condition esplicitamente documentata nel docstring di modulo, main.py:79-87).

---

## 3. Logica della strategia (dettaglio completo)

### 3.1 Griglia dei livelli di prezzo

- **Formula**: livello assoluto N (intero qualsiasi, positivo o negativo) = `base_price * (1 + step_pct) ** N` — progressione **geometrica**, non aritmetica.
- `step_pct` = `grid_step_pct` di config (frazione, es. `0.005` per 0.5%).
- `base_price` è fissato **UNA SOLA VOLTA per ciclo**, tramite `RangeGrid.full_reset(new_base_price)` (strategy.py:239-245), che setta anche `zero_index = 0`. I due (e unici) punti di chiamata sono `_bootstrap_position` e `_reset_state_after_close` in main.py. **Non esiste alcun altro meccanismo che modifichi `base_price` durante un ciclo**: nessun re-anchoring su movimenti al ribasso, nessun re-anchoring da RSI/tick-mode. Questo è enunciato esplicitamente nel docstring di modulo di `strategy.py` (righe 10-16) e di `main.py` (righe 9-20).
- **Classificazione di un prezzo** (`RangeGrid._absolute_offset`, strategy.py:215-227):
  ```python
  ratio = price / self.base_price
  raw = math.log(ratio) / math.log1p(self.step_pct)
  rounded = round(raw)
  if abs(raw - rounded) < 1e-9:
      raw = float(rounded)
  return math.ceil(raw) - 1
  ```
  Nota comportamentale (documentata, non un bug): un prezzo **esattamente** su un livello viene classificato al livello **inferiore** (`ceil(raw) - 1`, con `raw` intero esatto). Per questo motivo, il primo fill di un ciclo (prezzo == `base_price` == anchor appena settato) va **escluso esplicitamente** da qualunque re-indexing, altrimenti classificherebbe a `-1` invece che a `0`.
- `zero_index` (int, default 0): vedi §3.2, è la "etichetta" mobile di Range 0, separata dal prezzo fisico dei livelli.

### 3.2 Re-indexing di Range 0 sul Break-Even

- `RangeGrid.reindex_to_breakeven(breakeven_price)` (strategy.py:247-262): calcola `new_zero_index = self._absolute_offset(breakeven_price)`; se diverso dall'attuale `zero_index`, lo aggiorna e ritorna `True` (altrimenti `False`, nessuna modifica). **I prezzi dei livelli non vengono mai toccati** — cambia solo quale livello assoluto è "chiamato" Range 0.
- Chiamato in `main.py::_execute_planned_order` (main.py:451-458), **dopo ogni fill**, tranne il primo del ciclo (`was_flat_before` guard, per il motivo del boundary quirk sopra). Usa `self.position.avg_entry_price` (Break-Even **lordo**) come input.
- `classify_offset(price) = _absolute_offset(price) - zero_index` (strategy.py:229-237) è l'offset "etichettato" (relativo al Range 0 corrente) usato da tutto il resto del codice (sizing Fibonacci, Neutral Zone, RSI catch-up, export dashboard) — nessuno di questi componenti sa che Range 0 sta "seguendo" il Break-Even, lo vedono semplicemente accadere.

### 3.3 Break-Even

Calcolato in `fees.compute_breakeven_prices(entries, fee_engine)` (fees.py:112-136):
- **Lordo** (`be_gross`): media dei prezzi di entrata pesata per quantità — identico a `PositionManager.avg_entry_price` (strategy.py:359-364).
- **Netto** (`be_net`): prezzo di chiusura al quale il PnL netto risulterebbe esattamente zero, includendo:
  - le fee di apertura già pagate (`total_open_fees`, somma di `EntryFill.taker_fee_usdt`)
  - il funding cashflow accumulato (`FeeEngine.total_funding_cashflow()`)
  - una stima della fee di chiusura al tasso taker (risolta algebricamente, non iterativa)
  ```python
  be_net = (be_gross * total_qty - total_open_fees + funding_cf) / (total_qty * (1.0 + taker_rate))
  ```
  Non include la fee **maker** (il bot piazza solo ordini `market`, quindi paga sempre il tasso taker).

### 3.4 Sizing degli ordini (`strategy.evaluate_grid_close`, strategy.py:290-332)

```python
offset = grid.classify_offset(price)

if breakeven_price is not None and price < breakeven_price:
    return PlannedOrder(range_offset=offset, fib_n=1, notional_usdt=base_notional_usdt, kind="fibonacci")

n = abs(offset) + 1
if max_fib_level is not None and n > max_fib_level:
    n = max_fib_level  # con warning di log
notional = fibonacci(n) * base_notional_usdt
return PlannedOrder(range_offset=offset, fib_n=n, notional_usdt=notional, kind="fibonacci")
```

- **`price < breakeven_price`** (SHORT attualmente in **profitto**, dato che un prezzo più basso è favorevole a uno short): notional **sempre fisso** = `base_notional_usdt`, `fib_n=1`, **nessun moltiplicatore**, indipendentemente da quanto sotto o in quale offset ci si trovi. `breakeven_price` qui è il Break-Even **lordo** (`PositionManager.avg_entry_price`), non quello netto.
- **`price >= breakeven_price`** (SHORT in perdita/recupero) — o `breakeven_price is None` (nessuna posizione ancora, caso che in pratica bypassa questa funzione perché il primo ordine del ciclo passa da `_execute_immediate_base_order`): progressione Fibonacci simmetrica, `n = |offset| + 1`, `notional = Fibonacci(n) * base_notional_usdt`.
- **Sequenza Fibonacci** (`strategy.fibonacci`, strategy.py:83-90, con `@lru_cache`): standard, `fib(1)=1, fib(2)=1, fib(3)=2, fib(4)=3, fib(5)=5, fib(6)=8, ...` — **nessun moltiplicatore custom**, è la sequenza matematica pura applicata a `base_notional_usdt`.
- **Cap di sicurezza**: `max_fib_level` = `config.risk.max_fib_level` (200 di default), ma passato come `None` (disabilitato) quando `stress_test.enabled=true` **e** `stress_test.unlimited_fib_level=true` (configurazione attuale: illimitato). Il cap si applica solo al ramo Fibonacci, mai al ramo a notional fisso (che è sempre `fib_n=1`).
- **Floor sulla quantità minima exchange** (main.py:410-424, aggiunto successivamente, non parte della logica di sizing originale): dopo il calcolo di `qty = notional / price`, se `qty` risulta sotto il minimo tradabile riportato da Bybit (`exchange.min_order_qty()`, letto live dal catalogo mercati), la `qty` viene alzata forzatamente a quel minimo. Questo può far sì che il notional realmente eseguito superi leggermente quello "teorico" calcolato da `evaluate_grid_close`.

### 3.5 Condizioni di entry / aggiunta / uscita

- **Entry (apertura ciclo)**: sempre immediata a mercato, mai su candle-close, Fibonacci(1) × `base_notional_usdt`. Vedi §2 "Apertura di un ciclo".
- **Aggiunta posizione (mediazione)**: valutata ad ogni ciclo dello scheduler (candle-close in produzione, cadenza fissa in stress test) — **sempre** esattamente un ordine per valutazione, mai "idle", salvo sospensione da Neutral Zone.
- **Uscita**: config-driven via `trailing_stop.enabled` (booleano), **mutuamente esclusiva**:
  - **`enabled=false`** (valore attuale di default) → **Take Profit fisso**, `_maybe_handle_fixed_take_profit` (main.py:501-516): chiude a mercato non appena `mark_price <= be_net * (1 - trailing_activation_pct/100)`. Soglia singola, nessun arming, nessun trailing. `trailing_distance_pct` è ignorato in questa modalità.
  - **`enabled=true`** → **Trailing Stop SHORT-only**, `_maybe_handle_trailing_stop` (main.py:534-569): **non attivo all'apertura della posizione**. Si arma (`_trailing_active=True`) al primo istante in cui `mark_price <= be_net * (1 - trailing_activation_pct/100)` (stessa soglia del TP fisso), fissando lo stop iniziale a `arming_price * (1 + trailing_distance_pct/100)`. Da quel momento, ogni nuovo minimo di `mark_price` abbassa lo stop (sempre `distance_pct`% sopra il minimo osservato), **mai retrocede verso l'alto** su un rimbalzo. Chiude a mercato quando `mark_price >= stop_price`.
  - Entrambe le percentuali (`activation_pct`, `distance_pct`) sono percentuali di **prezzo reale**, non di margine con leva.
- **Stop-loss**: **non esiste** uno stop-loss nel senso tradizionale — l'unico "controllo del rischio" verso il basso (posizione in perdita crescente) è la progressione Fibonacci che aumenta il notional mediando via via che il prezzo sale (ricordare: è uno SHORT, quindi "in perdita" = prezzo sale), fino al cap `max_fib_level` (disabilitato in configurazione attuale) e al limite implicito del margine disponibile sull'account.

### 3.6 RSI / Tick Mode (solo se `stress_test.enabled=true`)

- **Calcolo RSI**: `strategy.compute_rsi` (strategy.py:265-287), Wilder's smoothing standard su `period` barre di `stress_test.rsi_timeframe`. Richiede almeno `period+1` chiusure.
- **Ricalcolo**: `_maybe_update_rsi` (main.py:594-649), al massimo una volta ogni `RSI_POLL_INTERVAL_SEC` (**costante hardcoded a 15.0s**, main.py:130), e solo se il poll intercetta una candela **nuova** chiusa (gate su `_rsi_last_candle_ts`).
- **Attivazione Tick Mode**: richiede un **crossing verso l'alto** (`prev < threshold <= rsi`), **non** semplicemente "essere in ipercomprato" — restare sopra soglia per candele consecutive non ri-attiva nulla, serve un nuovo attraversamento dal basso.
- **Guard aggiuntivo** — `_has_real_breakeven_gap` (main.py:571-592): il crossing viene ignorato (nessuna attivazione) se il Break-Even (offset labeled) non è strettamente "dietro" (offset minore) rispetto al mark price — cioè se la posizione non è effettivamente in perdita/da recuperare rispetto alla griglia. Se Break-Even è già uguale o avanti (posizione già in pari/profitto rispetto alla banda corrente), un crossing RSI **non** arma il tick mode.
- **Effetto quando attivo**: la cadenza dello scheduler passa da `stress_test.base_interval_sec` a `stress_test.tick_mode_interval_sec` (1s di default) — **la griglia non viene mai toccata**, cambia solo la frequenza di valutazione.
- **Disattivazione** — `_maybe_handle_rsi_tick_mode` (main.py:651-683): quando l'offset labeled del Break-Even coincide con l'offset labeled del mark price (stessa "banda" della griglia), il tick mode si disattiva e torna alla cadenza base. Serve un nuovo crossing RSI per riarmarlo (questa funzione non lo riattiva mai, solo disattiva).

### 3.7 Neutral Zone (solo se `stress_test.enabled=true` e `stress_test.neutral_zone_enabled=true`)

`_maybe_suspend_for_neutral_zone` (main.py:357-394):
- Se la posizione non è flat e `|mark_price - avg_entry_price| / avg_entry_price * 100 < neutral_zone_percent` (0.15% di default), l'ordine Fibonacci di quel giro viene **sospeso** (nessuna esecuzione, log informativo).
- Basata sul Break-Even **lordo** (`avg_entry_price`), non netto.
- **Bypass incondizionato**: se l'RSI corrente è >= soglia overbought (`_rsi_overbought_now()`, indipendentemente dal fatto che il tick mode sia attivo o già disattivato), la sospensione viene **ignorata** e l'ordine parte comunque. Questo bypass è stato valutato esplicitamente per la rimozione e **mantenuto deliberatamente**, perché altrimenti il Tick Mode non potrebbe mai raggiungere la condizione di catch-up quando Break-Even e prezzo sono vicini (situazione tipica proprio a ridosso della convergenza, dove Neutral Zone e Tick Mode altrimenti si bloccherebbero a vicenda).

---

## 4. Nomi esatti da codice

| Concetto | Nome esatto | File:riga |
|---|---|---|
| Orchestratore principale | `class GridBotOrchestrator` | main.py:148 |
| Configurazione | `class StrategyConfig`, metodo `StrategyConfig.load()` | strategy.py:98, strategy.py:134 |
| Griglia | `class RangeGrid` | strategy.py:186 |
| Prezzo ancora (fisso per ciclo) | `RangeGrid.base_price` | strategy.py:206 |
| Etichetta mobile Range 0 | `RangeGrid.zero_index` | strategy.py:208 |
| Ancoraggio ciclo (una tantum) | `RangeGrid.full_reset()` | strategy.py:239 |
| Re-indexing su BE | `RangeGrid.reindex_to_breakeven()` | strategy.py:247 |
| Classificazione offset assoluto | `RangeGrid._absolute_offset()` | strategy.py:215 |
| Classificazione offset labeled | `RangeGrid.classify_offset()` | strategy.py:229 |
| Sizing ordine | funzione `evaluate_grid_close()` | strategy.py:290 |
| Ordine pianificato | `class PlannedOrder` (campi `range_offset`, `fib_n`, `notional_usdt`, `kind`) | strategy.py:179 |
| Sequenza Fibonacci | funzione `fibonacci()` | strategy.py:84 |
| Tracking posizione | `class PositionManager`, proprietà `avg_entry_price`, `total_qty`, `is_flat`, `max_fib_level` | strategy.py:341 |
| Break-Even lordo/netto | funzione `compute_breakeven_prices()` → `(be_gross, be_net)` | fees.py:112 |
| Motore fee/funding | `class FeeEngine` | fees.py:50 |
| PnL netto | funzione `compute_net_pnl()` → `NetPnlBreakdown` | fees.py:85 |
| Take profit fisso | metodo `_maybe_handle_fixed_take_profit()` | main.py:501 |
| Trailing stop (attivo/config `trailing_stop.enabled=true`) | metodo `_maybe_handle_trailing_stop()` | main.py:534 |
| Prezzo di attivazione trailing/TP | metodo `_trailing_activation_price()` | main.py:518 |
| Prezzo stop trailing corrente | metodo `_trailing_stop_price()` | main.py:527 |
| Config: soglia attivazione (%) | `StrategyConfig.trailing_activation_pct` ← `config.json: trailing_stop.activation_pct` | strategy.py:106 |
| Config: distanza trailing (%) | `StrategyConfig.trailing_distance_pct` ← `config.json: trailing_stop.distance_pct` | strategy.py:107 |
| Config: toggle trailing vs TP fisso | `StrategyConfig.trailing_stop_enabled` ← `config.json: trailing_stop.enabled` | strategy.py:105 |
| Chiusura + riapertura ciclo | metodo `_close_cycle()` | main.py:707 |
| Reset stato post-chiusura | metodo `_reset_state_after_close()` | main.py:767 |
| RSI (calcolo) | funzione `compute_rsi()` | strategy.py:265 |
| RSI (polling/attivazione) | metodo `_maybe_update_rsi()` | main.py:594 |
| RSI (disattivazione tick mode) | metodo `_maybe_handle_rsi_tick_mode()` | main.py:651 |
| Guard "gap reale" per l'arming RSI | metodo `_has_real_breakeven_gap()` | main.py:571 |
| Flag tick mode attivo | `GridBotOrchestrator._rsi_tick_mode_active` | main.py:190 |
| Neutral Zone | metodo `_maybe_suspend_for_neutral_zone()` | main.py:357 |
| Esecuzione ordine (con lock e floor qty minima) | metodo `_execute_planned_order()` | main.py:400 |
| Esecuzione ordine immediato (apertura/riapertura ciclo) | metodo `_execute_immediate_base_order()` | main.py:263 |
| Ancoraggio all'avvio/riconciliazione posizione esistente | metodo `_bootstrap_position()` | main.py:233 |
| Wrapper exchange | `class ExchangeClient` | exchange.py:95 |
| Client pubblico (market data) | `ExchangeClient._public` | exchange.py:110 |
| Client privato (trading) | `ExchangeClient._private` | exchange.py:119 |
| Calcolo qty da notional | `ExchangeClient.compute_qty_from_notional()` | exchange.py:216 |
| Qty minima exchange | `ExchangeClient.min_order_qty()` | exchange.py:220 |
| Retry con backoff | `ExchangeClient._retry()` | exchange.py:303 |
| Esportazione stato live | `class DataExporter`, metodo `export()` | data_exporter.py:57, data_exporter.py:66 |
| Stato live (dataclass) | `class LiveState` | data_exporter.py:22 |
| Persistenza cicli/statistiche | `class AnalyticsEngine` | analytics.py:81 |
| Record ordine | `class OrderRecord` | analytics.py:21 |
| Record ciclo | `class CycleRecord` | analytics.py:31 |
| Kill switch (stop-file) | `STOP_SIGNAL_PATH`, metodo `_check_stop_file()` | main.py:131, main.py:839 |
| Auto-compound del notional base | in `_reset_state_after_close()`, campo `GridBotOrchestrator.base_notional_usdt` | main.py:772-775 |
| Classe trailing legacy (NON usata) | `class TrailingStopController` | strategy.py:378 |

---

## 5. Gestione API ed esecuzione ordini

- **Libreria**: `ccxt.async_support` (CCXT, variante asincrona) — **non** `pybit`. Import: `import ccxt.async_support as ccxt_async` (exchange.py:56).
- **Due istanze CCXT separate** (motivo documentato in exchange.py:7-29): l'host Demo Trading di Bybit (`https://api-demo.bybit.com`) rifiuta molte chiamate autenticate non di trading (es. `load_markets()` autenticato, klines/ticker/funding autenticati) con `retCode 10032`.
  - `self._public`: **non autenticata**, punta sempre all'host di produzione, usata per tutti i dati di mercato (candele, ticker, funding rate, metadati mercato/precisione). Nessun downside di correttezza perché l'order book è identico tra demo e mainnet.
  - `self._private`: **autenticata** (`apiKey`/`secret`), URL privato forzato su `BYBIT_DEMO_BASE_URL` se `cfg.use_testnet=True` (default), altrimenti host di produzione (**fondi reali**). Usata solo per ordini, posizioni, leva, margin mode. Il suo catalogo mercati è "seminato" da quello pubblico via `set_markets()` (mai chiama `load_markets()` da sola).
- **Tipo di ordini**: **solo market**, mai limit. Apertura posizione: `create_order(symbol, "market", "sell", qty)` (SHORT). Chiusura: `create_order(symbol, "market", "buy", qty, params={"reduceOnly": True})`.
- **Fill polling**: la risposta di `create_order` di Bybit V5 è una semplice ack (solo order id, senza prezzo/qty di fill reali). `_create_order_and_await_fill()` (exchange.py:270-293) fa polling di `fetch_order` fino a 5 tentativi (`poll_attempts=5`) con 0.3s di pausa (`poll_delay_sec=0.3`), fermandosi al primo stato `"closed"` con `filled > 0`. Se dopo 5 tentativi non c'è conferma, procede comunque con l'ultimo stato noto (loggando un warning) — **non c'è verifica esplicita che `filled == qty richiesta`** (nessuna gestione dedicata di fill parziali).
- **Async/sync**: interamente asincrono (`asyncio`), le due coroutine principali (`_grid_scheduler_loop`, `_tick_loop`) girano concorrenti via `asyncio.gather`.
- **Gestione errori e retry** — `ExchangeClient._retry()` (exchange.py:303-324):
  - `NetworkError` (CCXT, errori transitori): retry con backoff esponenziale, `delay = retry_base_delay_sec * 2**(attempt-1)`, `retry_base_delay_sec=1.0`, fino a `max_retries=5` (entrambi parametri del costruttore `ExchangeClient`, non esposti in `config.json`). Oltre il limite, l'eccezione viene rilanciata.
  - `ExchangeError` (CCXT, rifiuti applicativi dell'exchange — fondi insufficienti, richiesta non valida, ecc.): **nessun retry**, loggato e rilanciato immediatamente.
  - A livello di chiamante (`main.py`), la maggior parte dei punti critici (fetch prezzo, esecuzione ordine, chiusura posizione, fetch candele RSI, fetch funding) sono avvolti in `try/except Exception` con `logger.exception(...)` e `return` — il loop principale **non crasha** per un singolo errore isolato, riprova al giro successivo. Un'eccezione **non gestita** in un punto non protetto farebbe comunque risalire fino a `main()`/`asyncio.run()` e terminare il processo (è quanto accaduto empiricamente con l'errore `InvalidOrder` su qty sotto il minimo, prima del fix — vedi §7).
- **Rate limiting**: `enableRateLimit: True` su entrambe le istanze CCXT (throttling automatico lato libreria, nessuna logica custom aggiuntiva).
- **Setup connessione** (`ExchangeClient.setup()`, exchange.py:155-182): `load_markets()` sul client pubblico, seed dei mercati sul privato, `load_time_difference()` (compensazione clock skew, il demo Bybit è severo su questo), `set_margin_mode(cross)`, `set_leverage(100x)` — entrambi questi ultimi due avvolti in `try/except ExchangeError` con log informativo (perché falliscono con "already set" se già configurati, non è un errore reale).

---

## 6. Configurazione

### `config.json`

| Chiave | Default attuale | Note |
|---|---|---|
| `exchange.id` | `"bybit"` | |
| `exchange.options.defaultType` | `"swap"` | forza perpetual futures |
| `exchange.options.adjustForTimeDifference` | `true` | |
| `exchange.options.recvWindow` | `10000` | ms |
| `symbol` | `"ETH/USDT:USDT"` | overridabile da env `SYMBOL` |
| `timeframe` | `"1m"` | usato dallo scheduler **solo in modalità produzione** (stress_test.enabled=false); vedi §3 |
| `leverage` | `100` | |
| `margin_mode` | `"cross"` | |
| `position_side` | `"short"` | **presente ma mai letto da `StrategyConfig.load()` — chiave morta, nessun effetto** (vedi §7) |
| `grid_step_pct` | `0.5` (→ 0.005 come frazione) | overridabile da env `GRID_STEP_PERCENT` |
| `base_notional_usdt` | `30.0` | |
| `trailing_stop.enabled` | `false` | `false`=take profit fisso, `true`=trailing stop |
| `trailing_stop.activation_pct` | `0.5` | soglia sotto Break-Even NETTO, in entrambe le modalità |
| `trailing_stop.distance_pct` | `0.7` | usato solo se `enabled=true` |
| `fees.taker_rate` | `0.00055` | |
| `fees.maker_rate` | `0.0002` | letto ma **mai usato in nessun calcolo** (il bot piazza solo ordini market) |
| `auto_compound.enabled` | `true` | |
| `auto_compound.percentage` | `5.0` | incremento % di `base_notional_usdt` ad ogni chiusura ciclo |
| `polling.tick_poll_interval_sec` | `2` | |
| `polling.funding_poll_interval_sec` | `300` | |
| `paths.trade_history_path` | `"trade_history.json"` | |
| `paths.state_export_path` | `"bot_state.json"` | |
| `risk.max_fib_level` | `200` | ignorato se stress test + `unlimited_fib_level` |
| `stress_test.enabled` | `true` | |
| `stress_test.base_interval_sec` | `3600` | cadenza base (1 ora) |
| `stress_test.tick_mode_interval_sec` | `1` | cadenza durante RSI tick mode |
| `stress_test.rsi_timeframe` | `"1d"` | |
| `stress_test.rsi_period` | `14` | |
| `stress_test.rsi_overbought_threshold` | `70` | |
| `stress_test.neutral_zone_enabled` | `true` | |
| `stress_test.neutral_zone_percent` | `0.15` | |
| `stress_test.unlimited_fib_level` | `true` | |

Chiavi di fallback ancora supportate dal codice (`StrategyConfig.load`) ma **non presenti** nel `config.json` attuale (rimosse durante l'hardening di sicurezza v1.1, sostituite da `.env`): `exchange.demo` (bool), `exchange.api_key`, `exchange.api_secret`.

### `.env` / variabili d'ambiente (via `python-dotenv`, caricato da `strategy.py:69`)

| Variabile | Default se assente | Note |
|---|---|---|
| `BYBIT_API_KEY` | — (obbligatoria, altrimenti `ValueError` in `exchange.py:_resolve_credentials`) | |
| `BYBIT_API_SECRET` | — (obbligatoria) | |
| `USE_TESTNET` | `true` | qualunque di `1/true/yes/on` (case-insensitive) è considerato vero |
| `SYMBOL` | fallback a `config.json: symbol` | |
| `GRID_STEP_PERCENT` | fallback a `config.json: grid_step_pct` | |

### Costanti hardcoded nel codice (non esposte in configurazione)

| Costante | Valore | File:riga |
|---|---|---|
| `BOT_VERSION` | `"1.2"` | main.py:127 |
| `CONFIG_PATH` | `"config.json"` | main.py:128 |
| `CANDLE_CLOSE_OFFSET_SEC` | `1.0` | main.py:129 |
| `RSI_POLL_INTERVAL_SEC` | `15.0` | main.py:130 |
| `STOP_SIGNAL_PATH` | `Path("STOP")` | main.py:131 |
| `ExchangeClient.max_retries` | `5` | exchange.py:98 |
| `ExchangeClient.retry_base_delay_sec` | `1.0` | exchange.py:98 |
| `poll_attempts` (fill polling) | `5` | exchange.py:272 |
| `poll_delay_sec` (fill polling) | `0.3` | exchange.py:272 |
| `DataExporter.history_limit` | `50` (cicli recenti esportati) | data_exporter.py:61 |
| `BYBIT_DEMO_BASE_URL` | `"https://api-demo.bybit.com"` | exchange.py:65 |

---

## 7. Differenze rispetto alla specifica originale

Il progetto è stato costruito incrementalmente su più sessioni, con diverse revisioni sostanziali rispetto alle specifiche iniziali:

1. **Griglia dinamica → griglia statica + re-indexing**: la prima implementazione della griglia era dinamica (re-anchoring del `base_price` su movimenti al ribasso, con sizing Fibonacci asimmetrico e reset a base su certi shift). È stata **completamente riscritta su richiesta esplicita** per rendere `base_price` immutabile per l'intera durata del ciclo, introducendo il meccanismo separato di `zero_index`/re-indexing descritto in §3.2 per far "seguire" l'etichetta Range 0 al Break-Even senza toccare i prezzi. Questo è il cambiamento architetturale più grande del progetto.

2. **Trailing stop vs take profit fisso**: inizialmente implementato **solo** il trailing stop SHORT-only (`_maybe_handle_trailing_stop`). Successivamente è stato richiesto di disattivarlo e sostituirlo con un take profit a soglia fissa. Anziché rimuovere il codice del trailing stop, è stato introdotto un toggle di configurazione (`trailing_stop.enabled`) che seleziona tra le due implementazioni, **entrambe mantenute nel codice**. Il valore attuale è `false` (take profit fisso attivo, 0.5%).

3. **Sizing sotto Break-Even**: la regola "notional fisso (no Fibonacci) quando il prezzo è sotto il Break-Even" è stata aggiunta in una fase successiva rispetto alla logica Fibonacci originale (che era simmetrica ovunque).

4. **Floor sulla quantità minima exchange** (main.py:410-424, `ExchangeClient.min_order_qty()`): non presente nella specifica originale. Aggiunto reattivamente dopo che il bot è crashato in produzione (`ccxt.base.errors.InvalidOrder: bybit amount of ETH/USDT:USDT must be greater than minimum amount precision of 0.01`) quando il prezzo di ETH è salito abbastanza da rendere `base_notional_usdt=20` insufficiente a generare una qty valida. Deliberatamente implementato come floor dinamico (letto dal catalogo mercati live) invece che come tabella di soglie di prezzo, per restare corretto indefinitamente senza manutenzione futura.

5. **Kill switch (stop-file)**: non presente nella specifica originale. Aggiunto per permettere uno shutdown pulito da un terminale diverso da quello in cui gira il processo (utile su Termux/mobile), senza dover cercare il PID o riattaccare una sessione `tmux`.

6. **Gap noto e deliberatamente non risolto — persistenza della griglia tra riavvii**: `_bootstrap_position()` (main.py:233), quando trova una posizione già aperta sull'exchange al riavvio, chiama comunque `grid.full_reset(existing.entry_price)` (main.py:252), **ri-ancorando** la griglia al prezzo medio corrente invece di preservare il `base_price`/`zero_index` del ciclo precedente. Questo **contraddice** la regola "la griglia si resetta solo alla chiusura completa del ciclo" enunciata in §3.1, in tutti i casi in cui il processo viene riavviato a metà ciclo (crash, aggiornamento config, ecc.). È stato identificato esplicitamente e la fix (persistere `base_price`+`zero_index` in `bot_state.json`/analogo e rileggerli al bootstrap) è stata **rimandata deliberatamente**, non ancora implementata.

7. **Classe `TrailingStopController` (strategy.py:378-403)**: implementazione originale/alternativa del trailing stop (basata su percentuale di PnL sul margine, non su prezzo), rimasta nel codice ma **mai istanziata né usata da `main.py`** — la logica di trailing effettivamente attiva è interamente in `main.py::_maybe_handle_trailing_stop`, che duplica il concetto con un'implementazione diversa (basata su prezzo, non su % di PnL). Codice morto.

8. **Chiave `position_side` in `config.json`**: presente nel file di configurazione ma **mai letta** da `StrategyConfig.load()` — probabile residuo di un'iterazione precedente della configurazione, oggi senza alcun effetto.

9. **Migrazione credenziali** (v1.1): le credenziali API sono state spostate da `config.json` (in chiaro) a `.env` (gitignored) per hardening di sicurezza. Il codice mantiene comunque un fallback a `raw["exchange"].get("api_key"/"api_secret", "")` per compatibilità, ma questi campi non esistono più nel `config.json` attuale.

10. **`bot_state.json` e `trade_history.json` rimossi dal tracking git**: originariamente versionati; causavano conflitti di merge (`git pull` rifiutato con "local changes would be overwritten") ogni volta che il processo era attivo su una macchina con modifiche locali non committate a questi file runtime. Rimossi dal tracking (restano su disco, generati/aggiornati come sempre) — non un cambio di logica applicativa, ma operativo/di repository.

---

## 8. Stato attuale e TODO

### Testato / funzionante
- Intera logica di griglia statica + re-indexing + sizing BE-dipendente + RSI tick mode + Neutral Zone verificata **live** su Bybit Demo Trading, con osservazione diretta di più cicli completi (incluso almeno un ciclo completo di attivazione/disattivazione del Tick Mode con re-indexing osservato in tempo reale).
- Take profit fisso e trailing stop entrambi implementati e verificati come mutuamente esclusivi via config.
- Retry di rete e gestione robusta di ordini market con polling del fill.
- Kill switch via stop-file, funzionante per processi avviati dopo l'introduzione della feature (non retroattivo su processi già in esecuzione con codice precedente in memoria — limite intrinseco, non un bug).
- Floor sulla quantità minima ordine, verificato risolvere il crash `InvalidOrder` osservato in produzione.

### Bug noti / limiti espliciti
- **Griglia non persistente tra riavvii con posizione aperta** (vedi §7 punto 6) — TODO esplicitamente rimandato dall'utente.
- **Nessuna protezione contro istanze multiple concorrenti**: nessun lock/mutex esterno, nessun controllo "sono già in esecuzione altrove". Incidente reale osservato: bot avviato contemporaneamente su due macchine (PC + telefono via Termux) sullo stesso account demo ha prodotto **ordini duplicati identici** (stesso prezzo/qty/timestamp, ID diversi), perché ciascuna istanza calcola la propria griglia/Break-Even localmente senza consapevolezza dell'altra.
- **Nessuna verifica di fill parziale**: `_create_order_and_await_fill` si ferma al primo stato `"closed"` con `filled > 0`, senza confrontare `filled` con la qty richiesta.
- **`fees.maker_rate`** è letto dalla configurazione ma non usato in nessun calcolo (il bot piazza solo ordini market).
- **`position_side`** in `config.json` è una chiave morta (vedi §7 punto 8).
- **`state.json`**: file residuo non referenziato da alcun modulo — da eliminare, non ha impatto funzionale.
- **`TrailingStopController`**: classe morta in `strategy.py`, mai usata.
- **Nessuna gestione esplicita di hedge mode / one-way mode** lato Bybit — il codice assume implicitamente la modalità di default dell'account demo, non testato in hedge mode.
- **Nessuna test suite automatizzata** nel repository.
- **Precisione decimali**: delegata interamente a `ccxt.amount_to_precision()` (basata sui `market limits` di Bybit) — non gestita manualmente, nessun problema noto.
- **Costanti di retry/polling non configurabili** da `config.json` (`max_retries`, `retry_base_delay_sec`, `poll_attempts`, `poll_delay_sec`) — richiederebbero una modifica del codice, non del file di configurazione, per essere cambiate.
