import ssl
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
import os
import boto3

# ---- AWS Credentials from Environment Variables ----
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")

# ---- Alpha Vantage API Key (new env var name = ALPHAVANTAGE_API_KEY) ----
api_key = os.getenv("ALPHAVANTAGE_API_KEY")

if not api_key:
    raise ValueError(
        "❌ Missing Alpha Vantage API Key. "
        "Please set ALPHAVANTAGE_API_KEY in your environment variables."
    )

# ---- Create S3 Client ----
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# ---- Output Directory ----
local_dir = "./stock_data"
os.makedirs(local_dir, exist_ok=True)

# =============================================================================
# 🔥 FUNCTION: GET ALL US EQUITY TICKERS FROM NASDAQ + NYSE (FREE ENDPOINT)
# =============================================================================

def fetch_us_equity_tickers():
    """
    Fetches and returns all US-listed equity tickers (NYSE + NASDAQ + AMEX).
    Source: NASDAQ Trader symbol directories (free & updated daily).
    """
    urls = {
        "nasdaq": "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "other":  "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    }

    dfs = []

    for name, url in urls.items():
        print(f"📥 Downloading {name} symbols...")

        df = pd.read_csv(url, sep="|")
        df = df[df["Symbol"].notnull()]  # remove blank/empy rows
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)

    # Remove test tickers (NASDAQ uses these for QA)
    exclusions = ["Test", "test", "TEST"]
    combined = combined[~combined["Security Name"].str.contains("|".join(exclusions), na=False)]

    tickers = combined["Symbol"].unique().tolist()

    print(f"✅ Total US equity tickers found: {len(tickers)}")

    return tickers


# =============================================================================
# MAIN SCRIPT LOGIC
# =============================================================================

# ---- Fetch dynamic ticker list ----
print("🔍 Fetching all US equity tickers (NYSE + NASDAQ)...")
all_tickers = fetch_us_equity_tickers()

print(f"📊 Using {len(all_tickers)} tickers for API calls...")

all_dataframes = []

for ticker in all_tickers:
    print(f"Fetching daily data for {ticker}...")

    url = (
        f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY"
        f"&symbol={ticker}&apikey={api_key}"
    )
    response = requests.get(url)

    if response.status_code == 200:
        json_data = response.json()
        time_series = json_data.get("Time Series (Daily)")

        if time_series:
            df = pd.DataFrame.from_dict(time_series, orient="index")
            df["ticker"] = ticker
            all_dataframes.append(df)
            print(f"✅ Retrieved {ticker}")
        else:
            print(f"⚠ No time series data for {ticker}")
    else:
        print(f"❌ Request failed for {ticker} (HTTP {response.status_code})")

# ---- Combine all stocks and upload ----
if all_dataframes:
    combined_df = pd.concat(all_dataframes)
    combined_df.index.name = "date"
    combined_df.reset_index(inplace=True)

    combined_file_path = os.path.join(local_dir, "combined_stock_data.csv")
    combined_df.to_csv(combined_file_path, index=False)
    print(f"\n✅ Saved locally: {combined_file_path}")

    s3_client.upload_file(combined_file_path, s3_bucket_name, "combined_stock_data.csv")
    print(f"🚀 Uploaded to S3: s3://{s3_bucket_name}/combined_stock_data.csv")

else:
    print("\n⚠ No data retrieved — nothing uploaded.")
