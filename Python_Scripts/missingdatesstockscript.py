import os
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta

import aiohttp
import pandas as pd
import boto3
from tenacity import retry, stop_after_attempt, wait_fixed

# -----------------------------------------------------------
# LOGGING
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------------------------------------
# BASE PATHS (🔥 FIXED HERE)
# -----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
TICKER_FILE = BASE_DIR / "ticker_data" / "sp500_tickers_api.csv"

# -----------------------------------------------------------
# ENVIRONMENT VARIABLES
# -----------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not S3_BUCKET_NAME:
    raise ValueError("Missing S3_BUCKET_NAME environment variable")

if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY environment variable")

# -----------------------------------------------------------
# AWS CLIENT
# -----------------------------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# -----------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------
ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
MAX_CONCURRENT_REQUESTS = 5

# -----------------------------------------------------------
# LOAD TICKERS (🔥 FIXED PATH)
# -----------------------------------------------------------
def load_sp500_tickers() -> list[str]:
    logging.info(f"Loading ticker file from: {TICKER_FILE}")

    if not TICKER_FILE.exists():
        raise FileNotFoundError(f"Ticker file not found: {TICKER_FILE}")

    df = pd.read_csv(TICKER_FILE)

    if "ticker" not in df.columns:
        raise ValueError("Ticker CSV must contain a 'ticker' column")

    tickers = df["ticker"].dropna().unique().tolist()
    logging.info(f"Loaded {len(tickers)} tickers")

    return tickers

# -----------------------------------------------------------
# ALPHA VANTAGE API CALL
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def fetch_daily_adjusted(session: aiohttp.ClientSession, ticker: str):
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": ticker,
        "outputsize": "full",
        "apikey": ALPHAVANTAGE_API_KEY,
    }

    async with session.get(ALPHAVANTAGE_URL, params=params) as response:
        if response.status != 200:
            raise RuntimeError(f"API error {response.status} for {ticker}")

        data = await response.json()

        if "Time Series (Daily)" not in data:
            raise ValueError(f"No daily data returned for {ticker}")

        return ticker, data["Time Series (Daily)"]

# -----------------------------------------------------------
# PROCESS & SAVE TO S3
# -----------------------------------------------------------
def process_and_upload(ticker: str, daily_data: dict):
    records = []

    for date_str, values in daily_data.items():
        records.append(
            {
                "date": date_str,
                "open": values.get("1. open"),
                "high": values.get("2. high"),
                "low": values.get("3. low"),
                "close": values.get("4. close"),
                "adjusted_close": values.get("5. adjusted close"),
                "volume": values.get("6. volume"),
                "dividend_amount": values.get("7. dividend amount"),
                "split_coefficient": values.get("8. split coefficient"),
                "ticker": ticker,
            }
        )

    df = pd.DataFrame(records)

    key = f"backfill/daily_prices/{ticker}.csv"
    csv_data = df.to_csv(index=False)

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=csv_data,
    )

    logging.info(f"Uploaded {ticker} backfill to s3://{S3_BUCKET_NAME}/{key}")

# -----------------------------------------------------------
# ASYNC WORKER
# -----------------------------------------------------------
async def run_backfill(tickers: list[str]):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:

        async def bound_fetch(ticker):
            async with semaphore:
                return await fetch_daily_adjusted(session, ticker)

        tasks = [bound_fetch(ticker) for ticker in tickers]

        for future in asyncio.as_completed(tasks):
            ticker, data = await future
            process_and_upload(ticker, data)

# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
async def main():
    logging.info("Starting missing dates stock backfill job")

    tickers = load_sp500_tickers()
    await run_backfill(tickers)

    logging.info("Backfill job completed successfully")

# -----------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
