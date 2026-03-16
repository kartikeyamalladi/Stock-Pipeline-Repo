import os

RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"

STOCK_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# Backoff and retry tuning (seconds)
# Maximum jitter to add to any backoff sleep (uniform 0..BACKOFF_JITTER_MAX)
BACKOFF_JITTER_MAX = 3

# Per-source backoff caps. AlphaVantage is usually quicker; Yahoo may request longer waits.
ALPHA_BACKOFF_CAP = 30
YAHOO_BACKOFF_CAP = 300
FINNHUB_BACKOFF_CAP = 30

# Default initial backoff (seconds)
INITIAL_BACKOFF = 1.0
