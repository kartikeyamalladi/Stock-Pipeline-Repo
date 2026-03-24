import aiohttp
from config.settings import ALPHA_VANTAGE_API_KEY, FINNHUB_API_KEY
from aiolimiter import AsyncLimiter

# Global limiter for Alpha Vantage: 1 request per second
ALPHA_LIMITER = AsyncLimiter(max_rate=1, time_period=1)
# Yahoo limiter: be conservative to avoid 429s (1 req/sec)
YAHOO_LIMITER = AsyncLimiter(max_rate=1, time_period=1)
# Finnhub limiter (conservative)
FINNHUB_LIMITER = AsyncLimiter(max_rate=1, time_period=1)

# Default request headers to reduce bot-blocking (some endpoints expect a browser UA)
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
    "Chrome/112.0.0.0 Safari/537.36 StockPipeline/1.0"
}

# Yahoo Finance endpoint frequently rate limits.
# In production this would be replaced with a licensed provider.
# For portfolio purposes we keep Alpha Vantage as the primary source.


class YahooRetryAfter(Exception):
    """Raised when Yahoo returns 429 and provides a Retry-After header.

    Carry the retry_after value (seconds) so callers can wait appropriately.
    """

    def __init__(self, retry_after: int | None, message: str = "Yahoo 429"):
        super().__init__(message)
        self.retry_after = retry_after


async def fetch_alpha_vantage(session, symbol):
    if not ALPHA_VANTAGE_API_KEY:
        return {"error": "missing ALPHA_VANTAGE_API_KEY"}

    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={symbol}"
        f"&apikey={ALPHA_VANTAGE_API_KEY}"
    )

    async with ALPHA_LIMITER:
        async with session.get(url) as response:
            try:
                data = await response.json()
            except Exception:
                # fallback: return text payload when JSON parsing fails
                text = await response.text()
                return {"error": text}

            # Alpha Vantage returns informational messages in keys like "Note", "Information"
            if isinstance(data, dict) and (
                "Note" in data or "Information" in data or "Error Message" in data
            ):
                # return the info dict so the caller can record it as an error
                return data

            return data


async def fetch_finnhub(session, symbol, resolution: str = "D", count: int = 100):
    """Fetch Finnhub candlestick data for a symbol.

    Uses a limiter and returns structured errors on non-200 responses or parse failures.
    """
    if not FINNHUB_API_KEY:
        return {"error": "missing FINNHUB_API_KEY"}

    url = f"https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution={resolution}&count={count}"
    headers = {"X-Finnhub-Token": FINNHUB_API_KEY}

    try:
        async with FINNHUB_LIMITER:
            async with session.get(url, headers=headers) as response:
                text = await response.text()
                if response.status != 200:
                    return {"error": {"status": response.status, "text": text}}

                try:
                    data = await response.json()
                except Exception:
                    return {"error": {"status": response.status, "text": text}}

                return data

    except Exception as e:
        return {"error": str(e)}


async def fetch_yahoo(session, symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    try:
        # respect a conservative limiter for yahoo as well
        async with YAHOO_LIMITER:
            async with session.get(url, headers=DEFAULT_HEADERS) as response:
                text = await response.text()
                if response.status == 429:
                    # Too Many Requests — raise a typed exception that includes Retry-After
                    ra = response.headers.get("Retry-After")
                    ra_seconds = None
                    try:
                        if ra is not None:
                            ra_seconds = int(ra)
                    except Exception:
                        ra_seconds = None

                    # raise a structured exception so the caller can obey Retry-After
                    raise YahooRetryAfter(
                        ra_seconds, f"Yahoo 429: retry-after={ra} text={text}"
                    )

                if response.status != 200:
                    return {"error": {"status": response.status, "text": text}}

                try:
                    parsed = await response.json()
                except Exception:
                    return {"error": {"status": response.status, "text": text}}

                # Some Yahoo responses are literally `null` -> parsed is None
                if parsed is None:
                    return {"error": {"status": response.status, "text": text}}

                return parsed

    except Exception as e:
        # keep structured error for storage and better debugging
        return {"error": str(e)}
