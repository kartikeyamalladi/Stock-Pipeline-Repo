import aiohttp
from config.settings import ALPHA_VANTAGE_API_KEY


async def fetch_alpha_vantage(session, symbol):

    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={symbol}"
        f"&apikey={ALPHA_VANTAGE_API_KEY}"
    )

    async with session.get(url) as response:
        return await response.json()


async def fetch_yahoo(symbol):

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()
