import os
import asyncio
import logging
from pathlib import Path
from datetime import datetime

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
# BASE PATHS (🔥 PATH-SAFE, PROD-GRADE)
# -----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
TICKER_FILE = BASE_DIR / "ticker_data" / "sp500_tickers_api.csv"

logging.info(f"📂 Script location: {Path(__file__).resolve()}")
logging.info(f"📂 Repo base dir: {BASE_DIR}")
logging.info(f"📂 Ticker file path: {TICKER_FILE}")

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
# LOAD STATIC S&P 500 TICKERS (DEFENSIVE + NORMALIZED)
# -----------------------------------------------------------
def load_sp500_tickers() -> list[str]:
    logging.info(f"📌 Loading ticker file from: {TICKER_FILE}")

    if not TICKER_FILE.exists():
        raise FileNotFoundError(f"Ticker file not found: {TICKER_FILE}")

    df = pd.read_csv(TICKER_FILE)

    # 🔒 Normalize column names (handles BOM, casing, whitespace)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace("\ufeff", "", regex=False)
    )

    if "ticker" not in df.columns:
        raise ValueError(
            f"'ticker' column not found after normalization. "
            f"Found columns: {list(df.columns)}"
        )

    tickers = (
        df["ticker"]
        .dropna()
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # 🚫 Remove accidental header rows or junk values
    tickers = tickers[tickers != "TICKER"]

    tickers = sorted(tickers.unique().tolist())

    if not tickers:
        raise ValueError("Ticker list is empty after cleaning")

    logging.info(f"📌 Loaded {len(tickers)} valid S&P 500 tickers")
    return tickers

# -----------------------------------------------------------
# ALPHA VANTAGE API CALL
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def fetch_daily_adjusted(
    session: aiohttp.ClientSession,
    ticker: str,
):
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": ticker,
        "outputsize": "full",
        "apikey": ALPHAVANTAGE_API_KEY,
    }

    async with session.get(ALPHAVANTAGE_URL, params=params) as response:
        if response.status != 200:
            raise RuntimeError(f"API error {response.status} for {ticker}")

        data = await response.json(content_type=None)

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
                "load_time": datetime.utcnow(),
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

    logging.info(
        f"✅ Uploaded {ticker} backfill to s3://{S3_BUCKET_NAME}/{key}"
    )

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
    logging.info("🚀 Starting missing dates stock backfill job")

    tickers = load_sp500_tickers()
    await run_backfill(tickers)

    logging.info("✅ Backfill job completed successfully")

# -----------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())