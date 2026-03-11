## Purpose
Short, actionable guidance for AI coding assistants working in this repository.

## High-level architecture (big picture)
- Components:
  - `ingestion/` — async data fetchers and storage helpers. Key files: `async_fetcher.py`, `sources.py`, `storage.py`.
  - `config/` — runtime configuration and constants (`settings.py`).
  - `data/` — persisted raw/processed payloads. Raw API JSON stored under `data/raw/<source>/<YYYY>/<MM>/<DD>/<SYMBOL>.json` (see `data/raw/alpha_vantage/2026/03/11/AAPL.json`).
  - `processing/` — downstream processing (empty top-level package present; normalization lives here in future work).

## Why this structure
- The repo separates: 1) fetching (networking, concurrency), 2) storage (filesystem layout), 3) processing (data normalization). This makes it easy to run ingestion independently and then process stored JSON offline.

## Key conventions and patterns (code you can copy)
- Concurrency: `async_fetcher.py` creates a single `aiohttp.ClientSession` and uses a semaphore + `asyncio.gather` for concurrent symbol fetches. Modify `CONCURRENT_REQUESTS` to tune throughput.
- API access: `sources.py` exports small async helpers like `fetch_alpha_vantage(session, symbol)` and `fetch_yahoo(symbol)`; note AlphaVantage expects a passed session while Yahoo creates its own session.
- Storage layout: `store_raw_data(source, symbol, data)` (in `ingestion/storage.py`) writes JSON to `RAW_DATA_PATH` (from `config/settings.py`) using `datetime.utcnow()` for year/month/day folders.
- Configuration: constants and secrets come from `config/settings.py`. `ALPHA_VANTAGE_API_KEY` is read from env. Don't hardcode secrets; follow the existing env-var approach.

## How to run (developer workflows)
- Install dependencies: `pip install -r requirements.txt`.
- Run ingestion (fetch and persist raw JSON):
  - Option A (module): `python -m ingestion.async_fetcher`
  - Option B (file): `python ingestion/async_fetcher.py`
- Check outputs under `data/raw/` for stored JSON per source/date/symbol.

## Integration points & external deps
- External APIs used:
  - Alpha Vantage: `https://www.alphavantage.co/query` (key via `ALPHA_VANTAGE_API_KEY`).
  - Yahoo finance chart API: `https://query1.finance.yahoo.com/v8/finance/chart/<SYMBOL>`.
- Networking uses `aiohttp` and `asyncio` primitives (ClientSession, TCPConnector).

## Useful code examples (copy/paste-ready)
- Persisting fetched payloads:
  - Call `store_raw_data(source="alpha_vantage", symbol=symbol, data=data)` after receiving JSON from `fetch_alpha_vantage`.
- Reading config values:
  - From code: `from config.settings import RAW_DATA_PATH, STOCK_SYMBOLS`.

## What agents should avoid changing without a test
- Do not change `RAW_DATA_PATH` behavior or the folder schema (year/month/day) without also updating downstream processing and tests.
- Avoid altering HTTP retry semantics in `async_fetcher.py` without adding an integration-style test (or a local mock for external APIs).

## Low-effort improvements agents may safely propose or implement
- Add a small unit test for `store_raw_data` to assert path creation + JSON writing.
- Make `fetch_yahoo` accept a session parameter (to reuse the shared session) for consistency and better connection reuse.
- Add environment/README notes that `ALPHA_VANTAGE_API_KEY` must be set to run the ingestion.

## Files to inspect first (quick links)
- `ingestion/async_fetcher.py` — ingestion entrypoint, concurrency and retries.
- `ingestion/sources.py` — API wrappers for Alpha Vantage and Yahoo.
- `ingestion/storage.py` — filesystem layout and writer.
- `config/settings.py` — paths, symbols list, and env-var usage.
- `data/raw/` — example stored API JSON for real responses.

## If you need more context
- Ask for which component you should change (ingestion vs processing). Provide the desired end-state (example: new parquet schema, or normalized daily OHLC format) and I'll infer where to update fetch/storage/processing code.

---
Please review for any missing pieces or conventions you want emphasized; I can iterate on this file.
