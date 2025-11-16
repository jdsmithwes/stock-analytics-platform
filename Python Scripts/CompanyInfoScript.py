import os
import asyncio
import aiohttp
import pandas as pd
import boto3
import logging
from datetime import datetime
from aiohttp import ClientSession, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed

# -----------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------------------------------------
# Environment Variables (same as your stock price script)
# -----------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not ALPHAVANTAGE_API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY environment variable.")

# -----------------------------------------------------------
# S3 Client
# -----------------------------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# -----------------------------------------------------------
# Fetch S&P 500 Tickers
# -----------------------------------------------------------
def fetch_sp500_tickers():
    df = pd.read_csv("https://datahub.io/core/s-and-p-500-companies/r/constituents.csv")
    return df["Symbol"].str.upper().tolist()

# -----------------------------------------------------------
# Normalize Overview JSON → DataFrame
# -----------------------------------------------------------
def normalize_overview_json(ticker, json_data):
    if not json_data or "Symbol" not in json_data:
        return None

    df = pd.DataFrame([json_data])
    df["ticker"] = ticker
    df["ingest_timestamp"] = datetime.now()

    return df

# -----------------------------------------------------------
# Async Overview Fetch with Retry Logic
# -----------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))  # faster retry for premium
async def fetch_overview(session: ClientSession, ticker: str):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=OVERVIEW&symbol={ticker}&apikey={ALPHAVANTAGE_API_KEY}"
    )

    async with session.get(url) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}")

        data = await resp.json()

    if not data or "Symbol" not in data:
        logging.warning(f"⚠️ {ticker}: No overview data returned.")
        return None

    return normalize_overview_json(ticker, data)

# -----------------------------------------------------------
# Premium Rate Limiter
# -----------------------------------------------------------
# Premium users reliably get 120 calls/min
# 0.5-second delay protects from burst throttling
CALL_DELAY = 0.5

# -----------------------------------------------------------
# Async Driver – Collect All DataFrames
# -----------------------------------------------------------
async def fetch_all_overviews(tickers):
    timeout = ClientTimeout(total=120)  # premium endpoints are fast
    connector = aiohttp.TCPConnector(limit=120)  # premium concurrency

    all_dfs = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for i, ticker in enumerate(tickers, start=1):

            logging.info(f"⏳ [{i}/{len(tickers)}] Fetching OVERVIEW for {ticker}…")

            df = await fetch_overview(session, ticker)

            if df is not None:
                all_dfs.append(df)
                logging.info(f"✅ Completed {ticker}")
            else:
                logging.warning(f"❌ No data for {ticker}")

            await asyncio.sleep(CALL_DELAY)  # premium pacing

    return all_dfs

# -----------------------------------------------------------
# Save ONE Unified CSV + Upload to S3
# -----------------------------------------------------------
def upload_unified_csv(all_dfs):
    unified_df = pd.concat(all_dfs, ignore_index=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"combined_company_overview_{timestamp}.csv"
    local_path = f"./{filename}"

    unified_df.to_csv(local_path, index=False)

    s3_key = f"combined_company_overview/{filename}"
    s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_key)

    logging.info(f"📤 Uploaded unified file → s3://{S3_BUCKET_NAME}/{s3_key}")

# -----------------------------------------------------------
# Main Workflow
# -----------------------------------------------------------
def main():
    logging.info("🚀 STARTING PREMIUM QUARTERLY COMPANY OVERVIEW INGEST")

    tickers = fetch_sp500_tickers()
    all_dfs = asyncio.run(fetch_all_overviews(tickers))

    if not all_dfs:
        logging.warning("⚠️ No overview data returned.")
        return

    upload_unified_csv(all_dfs)

    logging.info("🎉 COMPLETE — Unified overview snapshot uploaded!")

# -----------------------------------------------------------
# Run
# -----------------------------------------------------------
if __name__ == "__main__":
    main()
