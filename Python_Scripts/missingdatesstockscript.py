import os
import re
import asyncio
import logging
import boto3
import pandas as pd
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
# ENVIRONMENT VARIABLES
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

TICKER_FILE = "ticker_data/sp500_tickers_api.csv"

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
# LOAD STATIC TICKERS (AUTHORITATIVE)
# -----------------------------------------------------------
def load_sp500_tickers() -> list[str]:
    if not os.path.exists(TICKER_FILE):
        raise FileNotFoundError(f"Ticker file not found: {TICKER_FILE}")

    df = pd.read_csv(TICKER_FILE)

    if "ticker" not in df.columns:
        raise ValueError("Ticker CSV must contain 'ticker' column")

    tickers = (
        df["ticker"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
        .tolist()
    )

    logging.info(f"📌 Loaded {len(tickers)} S&P 500 tickers")
    return sorted(tickers)

# -----------------------------------------------------------
# GET EXISTING DATES PER TICKER FROM S3
# -----------------------------------------------------------
def get_existing_dates_by_ticker() -> dict[str, set[date]]:
    paginator = s3.get_paginator("list_objects_v2")
    existing = {}

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME,
        Prefix=S3_PREFIX,
    ):
        for obj in page.get("Contents", []):
            match = DATE_REGEX.search(obj["Key"])
            if not match:
                continue

            d = datetime.strptime(match.group(1), "%Y-%m-%d").date()

            try:
                df = pd.read_csv(
                    s3.get_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=obj["Key"],
                    )["Body"],
                    usecols=["ticker"],
                )
            except Exception:
                continue

            for t in df["ticker"].dropna().unique():
                existing.setdefault(t, set()).add(d)

    logging.info("📦 Loaded existing ticker/date coverage from S3")
    return existing

# -----------------------------------------------------------
# EXPECTED DATE RANGE
# -----------------------------------------------------------
def get_expected_dates() -> set[date]:
    today = date.today()
    return {
        BACKFILL_START_DATE + timedelta(days=i)
        for i in range((today - BACKFILL_START_DATE).days + 1)
    }

# -----------------------------------------------------------
# API CALL (FULL HISTORY)
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def fetch_stock_history(session, ticker: str) -> pd.DataFrame:
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}"
        f"&apikey={ALPHAVANTAGE_API_KEY}"
        f"&outputsize=full"
    )

    async with session.get(url) as response:
        response.raise_for_status()
        data = await response.json(content_type=None)

        if "Time Series (Daily)" not in data:
            raise ValueError(f"Invalid API response for {ticker}")

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
    tickers = load_sp500_tickers()
    expected_dates = get_expected_dates()
    existing = get_existing_dates_by_ticker()

    missing_map: dict[date, list[str]] = {}

    for ticker in tickers:
        present_dates = existing.get(ticker, set())
        missing_dates = expected_dates - present_dates

        for d in missing_dates:
            missing_map.setdefault(d, []).append(ticker)

    logging.info(
        f"❌ Found {sum(len(v) for v in missing_map.values())} "
        f"missing ticker/date combinations"
    )

    if not missing_map:
        logging.info("✅ No missing data — exiting")
        return

    timeout = ClientTimeout(total=120)

    async with ClientSession(timeout=timeout) as session:
        for missing_date, tickers_for_date in missing_map.items():
            dfs = []

            for ticker in tickers_for_date:
                history = await fetch_stock_history(session, ticker)
                row = history[history["date"] == missing_date]
                if not row.empty:
                    dfs.append(row)

            if not dfs:
                logging.warning(f"⚠️ No data for {missing_date}")
                continue

            day_df = pd.concat(dfs, ignore_index=True)

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
