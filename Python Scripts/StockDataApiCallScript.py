import os
import boto3
import pandas as pd
import yfinance as yf
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------------------------------------------------------------
# AWS CREDS
# ------------------------------------------------------------------------------
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# Output directory
local_dir = "./stock_data"
os.makedirs(local_dir, exist_ok=True)

# ------------------------------------------------------------------------------
# 1️⃣ Fetch all NASDAQ + NYSE tickers (using very reliable source)
# ------------------------------------------------------------------------------

def fetch_us_equity_tickers():
    """
    Uses NASDAQ Trader Symbol Directory to get all US equity tickers.
    """
    urls = {
        "nasdaq": "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "other": "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    }

    dfs = []
    for name, url in urls.items():
        print(f"📥 Downloading {name} symbols...")
        df = pd.read_csv(url, sep="|")
        df = df[df["Symbol"].notnull()]
        dfs.append(df)

    all_symbols = pd.concat(dfs, ignore_index=True)

    # Remove test symbols
    exclusions = ["Test", "test", "TEST"]
    all_symbols = all_symbols[
        ~all_symbols["Security Name"].str.contains("|".join(exclusions), na=False)
    ]

    tickers = all_symbols["Symbol"].unique().tolist()
    print(f"✅ Total US tickers discovered: {len(tickers)}")

    return tickers

# ------------------------------------------------------------------------------
# 2️⃣ Fetch daily price data with retry/backoff and throttling
# ------------------------------------------------------------------------------

def fetch_ticker_data(ticker, max_retries=3):
    """
    Fetches historical daily prices using yfinance with retry logic.
    """
    for attempt in range(max_retries):
        try:
            df = yf.download(ticker, period="1y", interval="1d", progress=False)

            if df.empty:
                print(f"⚠ No data for {ticker}")
                return None

            df["ticker"] = ticker
            return df

        except Exception as e:
            wait = (attempt + 1) * 2
            print(f"⚠ Error fetching {ticker}: {e} — retrying in {wait}s")
            time.sleep(wait)

    print(f"❌ Failed to fetch {ticker} after retries")
    return None

# ------------------------------------------------------------------------------
# 3️⃣ Use ThreadPoolExecutor to run fetches in parallel (10× faster)
# ------------------------------------------------------------------------------

def fetch_all_tickers_parallel(tickers, max_workers=20):
    all_data = []

    print(f"🚀 Fetching stock data in parallel with {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_ticker_data, t): t for t in tickers}

        for future in as_completed(futures):
            ticker = futures[future]
            result = future.result()

            if result is not None:
                all_data.append(result)
                print(f"✅ {ticker} fetched")

    return all_data

# ------------------------------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------------------------------

print("🔍 Fetching all US equity tickers...")
tickers = fetch_us_equity_tickers()

# Optional: limit tickers temporarily for testing
# tickers = tickers[:50]

print("\n📈 Starting data fetch...")
dataframes = fetch_all_tickers_parallel(tickers)

if not dataframes:
    print("❌ No data retrieved — nothing to upload")
    exit()

# Combine all data
combined_df = pd.concat(dataframes)
combined_df.index.name = "date"
combined_df.reset_index(inplace=True)

# Save locally
combined_path = os.path.join(local_dir, "combined_stock_data.csv")
combined_df.to_csv(combined_path, index=False)

print(f"\n💾 Saved: {combined_path}")

# Upload to S3
s3_client.upload_file(combined_path, s3_bucket_name, "combined_stock_data.csv")

print(f"🚀 Upload complete: s3://{s3_bucket_name}/combined_stock_data.csv")
