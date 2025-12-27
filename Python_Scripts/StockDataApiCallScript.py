import os
import re
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta, date

import aiohttp
import pandas as pd
import boto3
from aiohttp import ClientSession, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed

# -----------------------------------------------------------
# LOGGING
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------------------------------------
# BASE PATHS (PATH-SAFE)
# -----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
SP500_TICKER_FILE = BASE_DIR / "ticker_data" / "sp500_tickers_api.csv"

logging.info(f"📂 Script location: {Path(__file__).resolve()}")
logging.info(f"📂 Repo base dir: {BASE_DIR}")
logging.info(f"📂 Ticker file path: {SP500_TICKER_FILE}")

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
# CONSTANTS
# -----------------------------------------------------------
S3_PREFIX = "stock_prices/"
DATE_REGEX = re.compile(r"(\d{4}-\d{2}-\d{2})")
FALLBACK_START_DATE = date(2020, 1, 1)

# -----------------------------------------------------------
# AWS CLIENT
# -----------------------------------------------------------
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# -----------------------------------------------------------
# LOAD STATIC S&P 500 TICKERS (DEFENSIVE)
# -----------------------------------------------------------
def load_sp500_tickers() -> list[str]:
    if not SP500_TICKER_FILE.exists():
        raise FileNotFoundError(f"Ticker file not found: {SP500_TICKER_FILE}")

    df = pd.read_csv(SP500_TICKER_FILE)

    df.columns = (
        df.columns
        .str.strip()
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
    tickers = sorted(tickers.unique().tolist())

    if not tickers:
        raise ValueError("Ticker list is empty after cleaning")

    logging.info(f"📌 Loaded {len(tickers)} valid S&P 500 tickers")
    return tickers

# -----------------------------------------------------------
# DISCOVER LAST LOADED DATE FROM S3
# -----------------------------------------------------------
def get_last_loaded_date_from_s3() -> date:
    paginator = s3_client.get_paginator("list_objects_v2")
    latest_date = None

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME,
        Prefix=S3_PREFIX,
    ):
        for obj in page.get("Contents", []):
            match = DATE_REGEX.search(obj["Key"])
            if match:
                parsed_date = datetime.strptime(
                    match.group(1), "%Y-%m-%d"
                ).date()
                if not latest_date or parsed_date > latest_date:
                    latest_date = parsed_date

    if latest_date:
        logging.info(f"📦 Last loaded S3 data date: {latest_date}")
        return latest_date

    logging.warning("⚠️ No dated objects found in S3 — using fallback")
    return FALLBACK_START_DATE

# -----------------------------------------------------------
# DETERMINE API DATE WINDOW
# -----------------------------------------------------------
def determine_api_date_window():
    last_loaded_date = get_last_loaded_date_from_s3()
    start_date = last_loaded_date + timedelta(days=1)
    end_date = date.today()

    if start_date > end_date:
        logging.info("✅ No new data to load — exiting")
        return None, None

    logging.info(f"📅 API window: {start_date} → {end_date}")
    return start_date, end_date

# -----------------------------------------------------------
# API CALL (NON-FATAL ON EMPTY DATA)
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def fetch_stock_data(session, ticker, start_date, end_date):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}"
        f"&apikey={ALPHAVANTAGE_API_KEY}"
        f"&outputsize=full"
    )

    async with session.get(url) as response:
        if response.status != 200:
            raise RuntimeError(f"API error {response.status} for {ticker}")

        data = await response.json(content_type=None)

        # 🚫 NON-FATAL: skip bad / unsupported tickers
        if "Time Series (Daily)" not in data:
            logging.warning(f"⚠️ No daily data for {ticker} — skipping")
            return None

        df = (
            pd.DataFrame.from_dict(
                data["Time Series (Daily)"], orient="index"
            )
            .reset_index()
            .rename(columns={"index": "date"})
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        df["ticker"] = ticker

        if df.empty:
            logging.warning(f"⚠️ No rows in date window for {ticker}")
            return None

        return df

# -----------------------------------------------------------
# MAIN (SAFE ASYNC EXECUTION)
# -----------------------------------------------------------
async def main():
    start_date, end_date = determine_api_date_window()
    if not start_date:
        return

    tickers = load_sp500_tickers()
    logging.info(f"🔎 API will run for {len(tickers)} tickers")

    timeout = ClientTimeout(total=60)
    success, skipped = 0, 0
    dfs = []

    async with ClientSession(timeout=timeout) as session:
        tasks = [
            fetch_stock_data(session, ticker, start_date, end_date)
            for ticker in tickers
        ]

        for future in asyncio.as_completed(tasks):
            result = await future

            if result is None:
                skipped += 1
                continue

            dfs.append(result)
            success += 1

    if not dfs:
        logging.warning("⚠️ No valid dataframes returned — exiting")
        return

    final_df = pd.concat(dfs, ignore_index=True)

    file_date = end_date.strftime("%Y-%m-%d")
    local_file = "/tmp/stock_prices.csv"
    s3_key = f"{S3_PREFIX}{file_date}_stock_prices.csv"

    final_df.to_csv(local_file, index=False)

    s3_client.upload_file(
        local_file,
        S3_BUCKET_NAME,
        s3_key,
    )

    logging.info(
        f"✅ Uploaded {len(final_df)} rows "
        f"(tickers success={success}, skipped={skipped}) "
        f"to s3://{S3_BUCKET_NAME}/{s3_key}"
    )

# -----------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
