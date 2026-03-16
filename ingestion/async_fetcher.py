import argparse
import asyncio
import logging
import random
from typing import List

import aiohttp

from config.settings import STOCK_SYMBOLS
import config.settings as settings
from ingestion.sources import (
    fetch_alpha_vantage,
    fetch_yahoo,
    YahooRetryAfter,
    fetch_finnhub,
)
from ingestion.storage import store_raw_data, store_raw_error, store_checkpoint


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def fetch_symbol(
    session,
    symbol: str,
    semaphore: asyncio.Semaphore,
    alpha_semaphore: asyncio.Semaphore,
    yahoo_semaphore: asyncio.Semaphore,
    finnhub_semaphore: asyncio.Semaphore,
    only_alpha: bool = False,
    only_yahoo: bool = False,
    only_finnhub: bool = False,
    dry_run: bool = False,
):
    """Fetch data for a single symbol. Returns a result dict with statuses.

    Parameters:
    - session: aiohttp session
    - semaphore: concurrency semaphore
    - only_alpha / only_yahoo / only_finnhub: flags to skip the other source
    - dry_run: if True, do not write files, just validate responses
    """
    # overall concurrency semaphore (limits symbols in flight)
    async with semaphore:
        backoff = settings.INITIAL_BACKOFF
        for attempt in range(10):
            try:
                # prepare coroutines based on flags
                alpha_task = None
                yahoo_task = None
                finnhub_task = None

                # Decide which sources to call. If any --only-* flag is provided,
                # only call the selected source(s). Otherwise call all sources.
                has_only_flags = bool(only_alpha or only_yahoo or only_finnhub)
                use_alpha = only_alpha or not has_only_flags
                use_yahoo = only_yahoo or not has_only_flags
                use_finnhub = only_finnhub or not has_only_flags
                # wrap source calls with their per-source semaphore so we limit
                # concurrent requests per provider (prevents bursts to a single host)
                if use_alpha:
                    if alpha_semaphore is not None:

                        async def _call_alpha():
                            async with alpha_semaphore:
                                return await fetch_alpha_vantage(session, symbol)

                        alpha_task = _call_alpha()
                    else:
                        alpha_task = fetch_alpha_vantage(session, symbol)
                if use_yahoo:
                    if yahoo_semaphore is not None:

                        async def _call_yahoo():
                            async with yahoo_semaphore:
                                return await fetch_yahoo(session, symbol)

                        yahoo_task = _call_yahoo()
                    else:
                        yahoo_task = fetch_yahoo(session, symbol)
                if use_finnhub:
                    if finnhub_semaphore is not None:

                        async def _call_finnhub():
                            async with finnhub_semaphore:
                                return await fetch_finnhub(session, symbol)

                        finnhub_task = _call_finnhub()
                    else:
                        finnhub_task = fetch_finnhub(session, symbol)

                # gather only the coroutines that were created; return_exceptions=True
                coros = [
                    c for c in (alpha_task, yahoo_task, finnhub_task) if c is not None
                ]
                results = []
                if coros:
                    results = await asyncio.gather(*coros, return_exceptions=True)

                # If any result is a YahooRetryAfter exception (429 with Retry-After),
                # obey the Retry-After header (if provided) or fallback to backoff.
                retry_due_to_429 = False
                for val in results:
                    if isinstance(val, YahooRetryAfter):
                        ra = val.retry_after or 0
                        # pick sleep = max(current backoff + jitter, retry-after)
                        jitter = random.uniform(0, settings.BACKOFF_JITTER_MAX)
                        sleep_seconds = max(backoff + jitter, ra)
                        # cap by the yahoo-specific cap
                        sleep_seconds = min(sleep_seconds, settings.YAHOO_BACKOFF_CAP)
                        logging.warning(
                            f"Yahoo 429 for {symbol}, sleeping {sleep_seconds:.1f}s (Retry-After={ra})"
                        )
                        await asyncio.sleep(sleep_seconds)
                        # increase backoff for subsequent retries (cap by YAHOO_BACKOFF_CAP)
                        backoff = min(backoff * 2, settings.YAHOO_BACKOFF_CAP)
                        retry_due_to_429 = True
                        break

                if retry_due_to_429:
                    # retry the whole attempt loop
                    continue

                # prepare result object
                result = {
                    "symbol": symbol,
                    "alpha_status": "skipped" if not use_alpha else "ok",
                    "yahoo_status": "skipped" if not use_yahoo else "ok",
                    "finnhub_status": "skipped" if not use_finnhub else "ok",
                    "alpha_error": None,
                    "yahoo_error": None,
                    "finnhub_error": None,
                }

                # map results back (handle exceptions per-source)
                alpha_data = None
                yahoo_data = None
                finnhub_data = None
                idx = 0

                if alpha_task is not None:
                    val = results[idx] if idx < len(results) else None
                    idx += 1
                    if isinstance(val, Exception):
                        result["alpha_status"] = "error"
                        result["alpha_error"] = str(val)
                        if not dry_run:
                            store_raw_error(
                                source="alpha_vantage",
                                symbol=symbol,
                                data={"error": str(val)},
                            )
                    else:
                        alpha_data = val
                if yahoo_task is not None:
                    val = results[idx] if idx < len(results) else None
                    idx += 1
                    if isinstance(val, Exception):
                        result["yahoo_status"] = "error"
                        result["yahoo_error"] = str(val)
                        if not dry_run:
                            store_raw_error(
                                source="yahoo",
                                symbol=symbol,
                                data={"error": str(val)},
                            )
                    else:
                        yahoo_data = val
                if finnhub_task is not None:
                    val = results[idx] if idx < len(results) else None
                    idx += 1
                    if isinstance(val, Exception):
                        result["finnhub_status"] = "error"
                        result["finnhub_error"] = str(val)
                        if not dry_run:
                            store_raw_error(
                                source="finnhub",
                                symbol=symbol,
                                data={"error": str(val)},
                            )
                    else:
                        finnhub_data = val

                # validate alpha (only if we have data and haven't already flagged an exception)
                if alpha_task is not None and result.get("alpha_status") != "error":
                    if not alpha_data or "Time Series (Daily)" not in alpha_data:
                        result["alpha_status"] = "error"
                        result["alpha_error"] = alpha_data
                        if not dry_run:
                            store_raw_error(
                                source="alpha_vantage", symbol=symbol, data=alpha_data
                            )
                    else:
                        if not dry_run:
                            store_raw_data(
                                source="alpha_vantage", symbol=symbol, data=alpha_data
                            )

                # validate finnhub (only if we have data and haven't already flagged an exception)
                if finnhub_task is not None and result.get("finnhub_status") != "error":
                    # finnub returns {'s': 'ok', 'c': [...], ...} on success for candles
                    if (
                        not finnhub_data
                        or not isinstance(finnhub_data, dict)
                        or finnhub_data.get("s") != "ok"
                    ):
                        result["finnhub_status"] = "error"
                        result["finnhub_error"] = finnhub_data
                        if not dry_run:
                            store_raw_error(
                                source="finnhub", symbol=symbol, data=finnhub_data
                            )
                    else:
                        if not dry_run:
                            store_raw_data(
                                source="finnhub", symbol=symbol, data=finnhub_data
                            )

                # validate yahoo (only if we have data and haven't already flagged an exception)
                if yahoo_task is not None and result.get("yahoo_status") != "error":
                    if (
                        not yahoo_data
                        or "chart" not in yahoo_data
                        or "result" not in yahoo_data.get("chart", {})
                    ):
                        result["yahoo_status"] = "error"
                        result["yahoo_error"] = yahoo_data
                        if not dry_run:
                            store_raw_error(
                                source="yahoo", symbol=symbol, data=yahoo_data
                            )
                    else:
                        if not dry_run:
                            store_raw_data(
                                source="yahoo", symbol=symbol, data=yahoo_data
                            )

                logging.info(f"Fetched {symbol}")
                return result

            except Exception as e:
                logging.info(f"Failed {symbol}: {e}")
                # exponential backoff with jitter
                jitter = random.uniform(0, settings.BACKOFF_JITTER_MAX)
                sleep_seconds = backoff + jitter
                # cap the generic backoff by the alpha cap (if we were calling alpha) or yahoo cap
                # simple heuristic: use max cap so we don't prematurely limit waits
                sleep_seconds = min(
                    sleep_seconds,
                    max(settings.ALPHA_BACKOFF_CAP, settings.YAHOO_BACKOFF_CAP),
                )
                await asyncio.sleep(sleep_seconds)
                backoff = min(backoff * 2, settings.ALPHA_BACKOFF_CAP)

        logging.error(f"failed after retries: {symbol}")
        return None


async def run_ingestion(
    symbols: List[str],
    concurrency: int = 5,
    only_alpha: bool = False,
    only_yahoo: bool = False,
    only_finnhub: bool = False,
    dry_run: bool = False,
    save_checkpoint: bool = True,
    alpha_concurrency: int | None = None,
    yahoo_concurrency: int | None = None,
    finnhub_concurrency: int | None = None,
):
    connector = aiohttp.TCPConnector(limit=concurrency)
    semaphore = asyncio.Semaphore(concurrency)

    # per-source semaphores: default to the overall concurrency if not provided
    alpha_sem = asyncio.Semaphore(alpha_concurrency or concurrency)
    yahoo_sem = asyncio.Semaphore(yahoo_concurrency or concurrency)
    finnhub_sem = asyncio.Semaphore(finnhub_concurrency or concurrency)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_symbol(
                session,
                symbol,
                semaphore,
                alpha_sem,
                yahoo_sem,
                finnhub_sem,
                only_alpha=only_alpha,
                only_yahoo=only_yahoo,
                only_finnhub=only_finnhub,
                dry_run=dry_run,
            )
            for symbol in symbols
        ]

        results = await asyncio.gather(*tasks)

        summary = {
            "total": len(symbols),
            "fetched": 0,
            "errors": 0,
            "per_symbol": [],
        }

        for r in results:
            if not r:
                summary["errors"] += 1
                continue

            summary["per_symbol"].append(r)
            if (
                r.get("alpha_status") == "ok"
                and r.get("yahoo_status") == "ok"
                and r.get("finnhub_status") == "ok"
            ):
                summary["fetched"] += 1
            else:
                summary["errors"] += 1

        if save_checkpoint:
            store_checkpoint(summary)

        return summary


def parse_args():
    parser = argparse.ArgumentParser(description="Async stock data fetcher")
    parser.add_argument(
        "--concurrency", type=int, default=5, help="Number of concurrent requests"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of symbols to fetch (overrides config)",
    )
    # mutually exclusive flags to limit sources
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--only-alpha", action="store_true", help="Fetch only Alpha Vantage"
    )
    group.add_argument(
        "--only-yahoo", action="store_true", help="Fetch only Yahoo Finance"
    )
    group.add_argument("--only-finnhub", action="store_true", help="Fetch only Finnhub")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files; only validate responses",
    )
    parser.add_argument(
        "--no-checkpoint", action="store_true", help="Do not write run checkpoint file"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Override RAW_DATA_PATH (e.g. data/raw or /tmp/raw)",
    )
    parser.add_argument(
        "--alpha-concurrency",
        type=int,
        default=None,
        help="Max concurrent requests to Alpha Vantage (defaults to --concurrency)",
    )
    parser.add_argument(
        "--yahoo-concurrency",
        type=int,
        default=None,
        help="Max concurrent requests to Yahoo Finance (defaults to --concurrency)",
    )
    parser.add_argument(
        "--finnhub-concurrency",
        type=int,
        default=None,
        help="Max concurrent requests to Finnhub (defaults to --concurrency)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    # argparse enforces mutual exclusion for source flags; no need for explicit check

    # apply output dir override if provided (mutates config at runtime)
    if getattr(args, "output_dir", None):
        settings.RAW_DATA_PATH = args.output_dir

    # set logging level from CLI
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    symbols = STOCK_SYMBOLS
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    summary = asyncio.run(
        run_ingestion(
            symbols=symbols,
            concurrency=args.concurrency,
            only_alpha=args.only_alpha,
            only_yahoo=args.only_yahoo,
            only_finnhub=args.only_finnhub,
            dry_run=args.dry_run,
            save_checkpoint=not args.no_checkpoint,
            alpha_concurrency=args.alpha_concurrency,
            yahoo_concurrency=args.yahoo_concurrency,
            finnhub_concurrency=args.finnhub_concurrency,
        )
    )

    logging.info(f"Run summary: {summary}")


if __name__ == "__main__":
    main()
