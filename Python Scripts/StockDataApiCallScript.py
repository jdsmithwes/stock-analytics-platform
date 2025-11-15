import os
import requests
import pandas as pd
import boto3
import logging
import time
from datetime import datetime

# ------------------------------------
# Logging Setup
# ------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ------------------------------------
# Environment Variables
# ------------------------------------
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
alpha_vantage_api_key = os.getenv("ALPHAVANTAGE_API_KEY")

if not alpha_vantage_api_key:
    raise ValueError("Missing Alpha Vantage key. Set ALPHAVANTAGE_API_KEY in your environment variables.")

# ------------------------------------
# Create S3 Client
# ------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)

# Output folder
local_dir = "./stock_data"
os.makedirs(local_dir, exist_ok=True)

# ------------------------------------
# Fetch S&P 500 Tickers (RELIABLE SOURCE)
# ------------------------------------
def fetch_sp500_tickers():
    logging.info("🔍 Fetching S&P 500 tickers from DataHub…")

    SP500_URL = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"

    try:
        df = pd.read_csv(SP500_URL)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch S&P500 list: {e}")

    if "Symbol" not in df.columns:
        raise RuntimeError("S&P500 CSV missing 'Symbol' column.")

    tickers = df["Symbol"].astype(str).str.upper().tolist()

    logging.info(f"✅ Loaded {len(tickers)} S&P500 tickers.")
    return tickers


# ------------------------------------
# Fetch Alpha Vantage Daily Close
# ------------------------------------
def fetch_daily_close(ticker):
    """Fetch TIME_SERIES_DAILY from Alpha Vantage. Returns dataframe or None."""
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={alpha_vantage_api_key}"
    )

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
    except Exception as e:
        logging.warning(f"⚠️ API request failed for {ticker}: {e}")
        return None

    json_data = response.json()

    if "Time Series (Daily)" not in json_data:
        logging.warning(f"⚠️ No daily data returned for {ticker}. Possible rate limit.")
        return None

    try:
        df = pd.DataFrame.from_dict(json_data["Time Series (Daily)"], orient="index")
        df.index.name = "date"
        df.reset_index(inplace=True)
        df["ticker"] = ticker
        return df
    except Exception as e:
        logging.warning(f"⚠️ Failed to parse data for {ticker}: {e}")
        return None


# ------------------------------------
# Main Process
# ------------------------------------
def main():
    logging.info("🔍 Starting S&P500 daily close fetch...")

    # 1️⃣ Fetch Tickers
    tickers = fetch_sp500_tickers()

    all_frames = []

    # If you have Alpha Vantage PREMIUM:
    # Allowed: 75 requests per minute
    PREMIUM_RATE_LIMIT = 75
    SLEEP_SECONDS = 15  # much faster — sleep every 75 calls only

    api_calls = 0

    # 2️⃣ Loop Through Tickers
    for ticker in tickers:
        logging.info(f"📈 Fetching {ticker}...")
        df = fetch_daily_close(ticker)

        if df is not None:
            all_frames.append(df)

        api_calls += 1

        # Rate-limit protection for PREMIUM plan
        if api_calls % PREMIUM_RATE_LIMIT == 0:
            logging.info(f"⏳ Sleeping {SLEEP_SECONDS} seconds (Premium rate limit)...")
            time.sleep(SLEEP_SECONDS)

    if not all_frames:
        logging.error("❌ No data returned for any ticker.")
        return

    # 3️⃣ Combine and Save
    combined = pd.concat(all_frames)
    combined_file_path = os.path.join(local_dir, "combinedstockdata.csv")

    combined.to_csv(combined_file_path, index=False)
    logging.info(f"💾 Saved combined file → {combined_file_path}")

    # 4️⃣ Upload to S3
    s3_client.upload_file(combined_file_path, s3_bucket_name, "combinedstockdata.csv")
    logging.info(
        f"🚀 Uploaded to S3 → s3://{s3_bucket_name}/combinedstockdata.csv"
    )

    logging.info("🎉 Script complete!")


# ------------------------------------
# Run
# ------------------------------------
if __name__ == "__main__":
    main()
