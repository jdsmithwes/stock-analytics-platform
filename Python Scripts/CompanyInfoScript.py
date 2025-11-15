import os
import aiohttp
import asyncio
import pandas as pd
import boto3
import logging
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
api_key = os.getenv("ALPHAVANTAGE_API_KEY")

if not api_key:
    raise ValueError("Missing Alpha Vantage API Key. Set ALPHAVANTAGE_API_KEY.")

# ------------------------------------
# Create S3 Client
# ------------------------------------
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)

# Output directory + cache
local_dir = "./stock_data"
cache_file = "./stock_data/alpha_cache.csv"
os.makedirs(local_dir, exist_ok=True)

# ------------------------------------
# Load or Create Cache
# ------------------------------------
def load_cache():
    if os.path.exists(cache_file):
        return pd.read_csv(cache_file)
    return pd.DataFrame(columns=["ticker", "date_retrieved"])

def update_cache(ticker):
    cache = load_cache()
    today = datetime.now().strftime("%Y-%m-%d")
    new_row = pd.DataFrame([{"ticker": ticker, "date_retrieved": today}])
    updated = pd.concat([cache, new_row], ignore_index=True)
    updated.to_csv(cache_file, index=False)

def is_cached(ticker):
    cache = load_cache()
    today = datetime.now().strftime("%Y-%m-%d")
    return (
        not cache.empty and
        ((cache["ticker"] == ticker) & (cache["date_retrieved"] == today)).any()
    )

# ------------------------------------
# Fetch Tickers (NASDAQ SOURCES – SAME AS ORIGINAL)
# ------------------------------------
def fetch_us_equity_tickers():
    logging.info("🔍 Fetching US tickers from NASDAQ Trader…")

    urls = {
        "nasdaq": "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "other":  "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    }

    dfs = []
    for name, url in urls.items():
        logging.info(f"📥 Downloading {name} symbols…")
        df = pd.read_csv(url, sep="|")
        df = df[df["Symbol"].notnull()]
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)

    exclusions = ["Test", "test", "TEST"]
    combined = combined[
        ~combined["Security Name"].str.contains("|".join(exclusions), na=False)
    ]

    tickers = combined["Symbol"].unique().tolist()
    logging.info(f"✅ Total tickers found: {len(tickers)}")

    return tickers

# ------------------------------------
# ASYNC Alpha Vantage Fetch – OVERVIEW FUNCTION
# ------------------------------------
async def fetch_overview(session, ticker):
    if is_cached(ticker):
        logging.info(f"⏭️ Cached: {ticker}")
        return None

    url = (
        f"https://www.alphavantage.co/query?function=OVERVIEW"
        f"&symbol={ticker}&apikey={api_key}"
    )

    try:
        async with session.get(url, timeout=20) as resp:
            if resp.status != 200:
                logging.warning(f"⚠️ HTTP {resp.status} for {ticker}")
                return None

            data = await resp.json()

            if not data or "Symbol" not in data:
                logging.warning(f"⚠️ No overview returned for {ticker}")
                return None

            update_cache(ticker)
            logging.info(f"✅ Retrieved {ticker}")
            return pd.DataFrame([data])

    except Exception as e:
        logging.warning(f"⚠️ Error fetching {ticker}: {e}")
        return None

# ------------------------------------
# ASYNC Worker Runner
# ------------------------------------
async def fetch_all_overviews(tickers, concurrency=20):
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:

        tasks = [fetch_overview(session, t) for t in tickers]
        results = await asyncio.gather(*tasks)

        # Filter out None responses
        return [r for r in results if r is not None]

# ------------------------------------
# MAIN PROCESS
# ------------------------------------
def main():
    logging.info("🚀 Starting async Alpha Vantage OVERVIEW fetch…")

    tickers = fetch_us_equity_tickers()

    # Run async fetcher
    results = asyncio.run(fetch_all_overviews(tickers))

    if not results:
        logging.error("❌ No overview data retrieved.")
        return

    combined_df = pd.concat(results, ignore_index=True)

    combined_file_path = os.path.join(local_dir, "combined_company_overview.csv")
    combined_df.to_csv(combined_file_path, index=False)

    logging.info(f"💾 Saved → {combined_file_path}")

    s3_client.upload_file(
        combined_file_path,
        s3_bucket_name,
        "combined_company_overview.csv"
    )

    logging.info("🎉 Upload complete → S3!")

if __name__ == "__main__":
    main()

