import os
import re
import asyncio
import aiohttp
import pandas as pd
import boto3
import logging
from datetime import date, datetime, timedelta
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
# ENVIRONMENT VARIABLES (NO NEW ONES)
# -----------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not S3_BUCKET_NAME:
    raise ValueError("Missing S3_BUCKET_NAME")
if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY")

# -----------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------
S3_PREFIX = "stock_prices/"
DATE_REGEX = re.compile(r"(\d{4}-\d{2}-\d{2})")
BACKFILL_START_DATE = date(2025, 11, 1)
BOOTSTRAP_TICKER = "AAPL"

# -----------------------------------------------------------
# AWS CLIENT
# -----------------------------------------------------------
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# -----------------------------------------------------------
# DISCOVER EXISTING DATES IN S3
# -----------------------------------------------------------
def get_existing_dates() -> set[date]:
    paginator = s3.get_paginator("list_objects_v2")
    dates = set()

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME, Prefix=S3_PREFIX
    ):
        for obj in page.get("Contents", []):
            match = DATE_REGEX.search(obj["Key"])
            if match:
                dates.add(
                    datetime.strptime(
                        match.group(1), "%Y-%m-%d"
                    ).date()
                )
    logging.info(f"📦 Found {len(dates)} existing dates in S3")
    return dates

# -----------------------------------------------------------
# DISCOVER TICKERS FROM S3
# -----------------------------------------------------------
def discover_tickers() -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME, Prefix=S3_PREFIX
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                response = s3.get_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=obj["Key"],
                )
                df = pd.read_csv(response["Body"], nrows=500)
                if "ticker" in df.columns:
                    tickers = sorted(
                        df["ticker"].dropna().unique().tolist()
                    )
                    logging.info(
                        f"📈 Discovered {len(tickers)} tickers"
                    )
                    return tickers

    logging.warning("⚠️ No tickers found — using bootstrap")
    return [BOOTSTRAP_TICKER]

# -----------------------------------------------------------
# COMPUTE MISSING DATES
# -----------------------------------------------------------
def get_missing_dates(existing_dates: set[date]) -> list[date]:
    today = date.today()
    expected = {
        BACKFILL_START_DATE + timedelta(days=i)
        for i in range((today - BACKFILL_START_DATE).days + 1)
    }
    missing = sorted(expected - existing_dates)
    logging.info(f"❌ Missing {len(missing)} dates")
    return missing

# -----------------------------------------------------------
# API CALL
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def fetch_stock_data(session, ticker):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}"
        f"&apikey={ALPHAVANTAGE_API_KEY}"
        f"&outputsize=full"
    )

    async with session.get(url) as response:
        response.raise_for_status()
        data = await response.json()

        if "Time Series (Daily)" not in data:
            raise ValueError(f"Bad response for {ticker}")

        df = (
            pd.DataFrame.from_dict(
                data["Time Series (Daily)"], orient="index"
            )
            .reset_index()
            .rename(columns={"index": "date"})
        )

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["ticker"] = ticker
        return df

# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
async def main():
    existing_dates = get_existing_dates()
    missing_dates = get_missing_dates(existing_dates)

    if not missing_dates:
        logging.info("✅ No missing dates — exiting")
        return

    tickers = discover_tickers()

    timeout = ClientTimeout(total=90)
    async with ClientSession(timeout=timeout) as session:
        tasks = [
            fetch_stock_data(session, ticker)
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)

    full_df = pd.concat(results, ignore_index=True)

    for missing_date in missing_dates:
        day_df = full_df[full_df["date"] == missing_date]

        if day_df.empty:
            logging.warning(f"No data for {missing_date}")
            continue

        local_file = "/tmp/stock_prices.csv"
        s3_key = f"{S3_PREFIX}{missing_date}_stock_prices.csv"

        day_df.to_csv(local_file, index=False)
        s3.upload_file(local_file, S3_BUCKET_NAME, s3_key)

        logging.info(
            f"✅ Backfilled {missing_date} "
            f"({len(day_df)} rows)"
        )

# -----------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
