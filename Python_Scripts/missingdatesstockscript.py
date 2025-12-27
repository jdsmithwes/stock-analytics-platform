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
# BASE PATHS
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
# LOAD TICKERS (DEFENSIVE)
# -----------------------------------------------------------
def load_sp500_tickers() -> list[str]:
    df = pd.read_csv(TICKER_FILE)

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace("\ufeff", "", regex=False)
    )

    tickers = (
        df["ticker"]
        .dropna()
        .astype(str)
        .str.upper()
        .str.strip()
    )

    tickers = tickers[tickers != "TICKER"]
    return sorted(tickers.unique().tolist())

# -----------------------------------------------------------
# API CALL (NON-FATAL ON EMPTY DATA)
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def fetch_daily_adjusted(session, ticker):
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

        # 🚫 NON-FATAL CONDITION
        if "Time Series (Daily)" not in data:
            logging.warning(f"⚠️ No daily data for {ticker} — skipping")
            return None

        return ticker, data["Time Series (Daily)"]

# -----------------------------------------------------------
# PROCESS & SAVE TO S3
# -----------------------------------------------------------
def process_and_upload(ticker, daily_data):
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

    if df.empty:
        logging.warning(f"⚠️ Empty dataframe for {ticker} — skipping upload")
        return

    key = f"backfill/daily_prices/{ticker}.csv"

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=df.to_csv(index=False),
    )

    logging.info(f"✅ Uploaded backfill for {ticker}")

# -----------------------------------------------------------
# ASYNC WORKER (SAFE)
# -----------------------------------------------------------
async def run_backfill(tickers):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    success, skipped = 0, 0

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:

        async def bound_fetch(ticker):
            async with semaphore:
                return await fetch_daily_adjusted(session, ticker)

        tasks = [bound_fetch(t) for t in tickers]

        for future in asyncio.as_completed(tasks):
            result = await future

            if result is None:
                skipped += 1
                continue

            ticker, data = result
            process_and_upload(ticker, data)
            success += 1

    logging.info(
        f"📊 Backfill summary — success: {success}, skipped: {skipped}"
    )

# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
async def main():
    logging.info("🚀 Starting missing-dates backfill job")
    tickers = load_sp500_tickers()
    await run_backfill(tickers)
    logging.info("🏁 Backfill job completed")

# ---------------------------------------
