import os
import asyncio
import aiohttp
import pandas as pd
import boto3
import logging
from aiohttp import ClientSession, ClientTimeout

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
    raise ValueError("Missing Alpha Vantage key. Set ALPHAVANTAGE_API_KEY.")

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
# Fetch S&P 500 Tickers (DataHub)
# ------------------------------------
def fetch_sp500_tickers():
    logging.info("🔍 Fetching S&P 500 tickers from DataHub…")

    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    try:
        df = pd.read_csv(url)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch S&P500 list: {e}")

    if "Symbol" not in df.columns:
        raise RuntimeError("S&P500 list missing active 'Symbol' column.")

    tickers = df["Symbol"].str.upper().tolist()

    logging.info(f"✅ Loaded {len(tickers)} S&P 500 tickers.")
    return tickers

# ------------------------------------
# Async Alpha Vantage OVERVIEW Fetch
# ------------------------------------
async def fetch_overview(session: ClientSession, ticker: str):
    """Fetch OVERVIEW from Alpha Vantage asynchronously."""
    url = (
        "https://www.alphavantage.co/query"
        f"?function=OVERVIEW&symbol={ticker}&apikey={alpha_vantage_api_key}"
    )

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logging.warning(f"⚠️ {ticker}: HTTP {resp.status}")
                return None

            data = await resp.json()

    except Exception as e:
        logging.warning(f"⚠️ {ticker}: API request failed: {e}")
        return None

    if not data or "Symbol" not in data:
        # API returned empty or hit a limit
        return None

    try:
        df = pd.DataFrame([data])
        return df
    except Exception as e:
        logging.warning(f"⚠️ {ticker}: Failed to parse overview: {e}")
        return None

# ------------------------------------
# Async Driver (parallel requests)
# ------------------------------------
async def fetch_all_overviews(tickers):
    timeout = ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=100)  # greatly increases speed

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [fetch_overview(session, ticker) for ticker in tickers]

        logging.info(f"🚀 Launching {len(tasks)} async OVERVIEW calls…")

        results = await asyncio.gather(*tasks)

    return [df for df in results if df is not None]

# ------------------------------------
# Main
# ------------------------------------
def main():
    logging.info("🚀 Starting async Alpha Vantage OVERVIEW fetch…")

    # 1️⃣ Fetch S&P500 tickers
    tickers = fetch_sp500_tickers()

    # 2️⃣ Run async overview fetcher
    all_frames = asyncio.run(fetch_all_overviews(tickers))

    if not all_frames:
        logging.error("❌ No OVERVIEW data returned for any tickers.")
        return

    # 3️⃣ Combine and save CSV
    combined = pd.concat(all_frames)
    output_file = os.path.join(local_dir, "combined_company_overview.csv")
    combined.to_csv(output_file, index=False)

    logging.info(f"💾 Saved → {output_file}")

    # 4️⃣ Upload to S3
    s3_client.upload_file(output_file, s3_bucket_name, "combined_company_overview.csv")

    logging.info(f"🚀 Uploaded to S3 → s3://{s3_bucket_name}/combined_company_overview.csv")
    logging.info("🎉 Company overview script complete!")

# ------------------------------------
# Run
# ------------------------------------
if __name__ == "__main__":
    main()
