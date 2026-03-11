import os

RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"

STOCK_SYMBOLS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA"
]

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
