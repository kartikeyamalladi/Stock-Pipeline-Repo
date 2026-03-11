import asyncio
import aiohttp

from config.settings import STOCK_SYMBOLS
from ingestion.sources import fetch_alpha_vantage
from ingestion.sources import fetch_yahoo
from ingestion.storage import store_raw_data
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

CONCURRENT_REQUESTS = 5

semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


async def fetch_symbol(session, symbol):
    async with semaphore:
        for attempt in range(3):
            try:
                alpha_task = await fetch_alpha_vantage(session, symbol)
                yahoo_task = await fetch_yahoo(symbol)
                alpha_data, yahoo_data = await asyncio.gather(alpha_task, yahoo_task)
                store_raw_data(source="alpha_vantage", symbol=symbol, data=alpha_data)
                store_raw_data(source="yahoo_finance", symbol=symbol, data=yahoo_data)
                logging.info(f"Fetched {symbol}")
                return

            except Exception as e:
                logging.info(f"Failed {symbol}: {e}")
                await asyncio.sleep(2)

        logging.error(f"failed after retries: {symbol}")


async def run_ingestion():
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_symbol(session, symbol) for symbol in STOCK_SYMBOLS]

        await asyncio.gather(*tasks)


def main():
    asyncio.run(run_ingestion())


if __name__ == "__main__":
    main()
