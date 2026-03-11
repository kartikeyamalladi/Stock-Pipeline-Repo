import asyncio
import aiohttp

from config.settings import STOCK_SYMBOLS
from ingestion.sources import fetch_alpha_vantage
from ingestion.storage import store_raw_data
import logging

loggin.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
)

CONCURRENT_REQUESTS = 5


async def fetch_symbol(session, symbol):

    try:

        data = await fetch_alpha_vantage(session, symbol)

        store_raw_data(
            source="alpha_vantage",
            symbol=symbol,
            data=data
        )

        logging.info(f"Fetched {symbol}")

    except Exception as e:
        logging.info(f"Failed {symbol}: {e}")


async def run_ingestion():

    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:

        tasks = [
            fetch_symbol(session, symbol)
            for symbol in STOCK_SYMBOLS
        ]

        await asyncio.gather(*tasks)


def main():
    asyncio.run(run_ingestion())


if __name__ == "__main__":
    main()
