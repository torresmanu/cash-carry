import requests
import pandas as pd
import time
from datetime import datetime

# Config
START_STR = "2023-01-01 00:00:00"
END_STR   = "2023-11-28 23:59:59" # Use end of day for clarity
START_MS = int(pd.Timestamp(START_STR).timestamp() * 1000)
END_MS   = int(pd.Timestamp(END_STR).timestamp() * 1000)
LIMIT = 1000 # Max limit for funding rate history

# URLs and Symbols
FUND_URL     = "https://dapi.binance.com/dapi/v1/fundingRate"
MARKKL_URL   = "https://dapi.binance.com/dapi/v1/markPriceKlines"
SPOT_URL     = "https://api.binance.com/api/v3/klines"
SYMBOL_PERP  = "BTCUSD_PERP"
SYMBOL_SPOT  = "BTCUSDT"
KLINE_INTERVAL = "1h" # Interval for spot and mark price klines

def fetch_paginated_data(url, params, data_key_time='fundingTime'):
    """Fetches paginated data from a Binance endpoint."""
    all_data = []
    local_params = params.copy() # Avoid modifying the original params dict
    fetch_start_time = local_params.get("startTime")

    while True:
        try:
            print(f"Fetching data starting from: {pd.to_datetime(local_params.get('startTime'), unit='ms')}...")
            resp = requests.get(url, params=local_params)
            resp.raise_for_status() # Raise exception for bad status codes (4xx or 5xx)
            data = resp.json()

            if not data:
                print("No more data received.")
                break

            all_data.extend(data)
            last_record_time = data[-1][data_key_time]

            # Stop if the last record time exceeds the overall end time or if fewer records than limit received
            if last_record_time >= END_MS or len(data) < local_params.get("limit", LIMIT):
                 print(f"Finished fetching. Last timestamp: {pd.to_datetime(last_record_time, unit='ms')}")
                 break

            # Prepare for the next request: start from the timestamp of the last record + 1ms
            local_params["startTime"] = last_record_time + 1
            time.sleep(0.3) # Increased sleep slightly

        except requests.exceptions.RequestException as e:
            print(f"HTTP Request failed: {e}")
            time.sleep(5) # Wait longer after an error
            continue # Retry last request
        except Exception as e:
            print(f"An error occurred: {e}")
            break # Exit loop on other errors

    # Filter data to ensure it's within the original START_MS and END_MS range
    # This is important because the API might return records starting slightly before startTime
    # and the last batch might contain records after END_MS
    filtered_data = [d for d in all_data if fetch_start_time <= d[data_key_time] <= END_MS]
    return filtered_data


def fetch_klines(url, symbol, interval="1h", kline_start_time=START_MS, kline_end_time=END_MS):
    """Modified fetch_klines using the paginated helper."""
    params = {"symbol": symbol, "interval": interval, "startTime": kline_start_time, "endTime": kline_end_time, "limit": LIMIT}
    # Klines endpoint returns list of lists, time is the first element (index 0)
    klines_data = fetch_paginated_data(url, params, data_key_time=0)
    return pd.DataFrame(klines_data)


# --- Fetch Funding history (Paginated) ---
print("Fetching Funding Rate History...")
fund_params = {"symbol": SYMBOL_PERP, "startTime": START_MS, "endTime": END_MS, "limit": LIMIT}
fund_data = fetch_paginated_data(FUND_URL, fund_params, data_key_time='fundingTime')
df_fund = pd.DataFrame(fund_data)
if not df_fund.empty:
    df_fund["fundingTime"] = pd.to_datetime(df_fund["fundingTime"], unit="ms")
    df_fund = df_fund[["fundingTime", "symbol", "fundingRate"]] # Select desired columns
    print(f"Fetched {len(df_fund)} funding rate records.")
else:
    print("No funding rate data fetched.")
    # Handle case where no data is returned, maybe exit or create empty df
    df_fund = pd.DataFrame(columns=["fundingTime", "symbol", "fundingRate"])


# --- Fetch Spot price (Paginated) ---
print("\nFetching Spot Klines...")
df_spot = fetch_klines(SPOT_URL, SYMBOL_SPOT, KLINE_INTERVAL)
if not df_spot.empty:
    # Keep Open time [0] and Close Price [4]
    df_spot = df_spot[[0, 4]].rename(columns={0: "Open time", 4: "spotPrice"})
    df_spot["Time"] = pd.to_datetime(df_spot["Open time"], unit="ms")
    df_spot = df_spot.set_index("Time")[["spotPrice"]].astype(float) # Convert price to float
    print(f"Fetched {len(df_spot)} spot klines.")
else:
    print("No spot kline data fetched.")
    df_spot = pd.DataFrame(columns=["spotPrice"]) # Empty df with correct column


# --- Fetch Historical mark-price (Paginated) ---
# Note: Data likely only available from around 2023-11-01 onwards from this endpoint
print("\nFetching Mark Price Klines...")
mark_df = fetch_klines(MARKKL_URL, SYMBOL_PERP, KLINE_INTERVAL)
if not mark_df.empty:
    # Keep Open time [0] and Close Price [4]
    mark_df = mark_df[[0, 4]].rename(columns={0: "Open time", 4: "markPrice"})
    mark_df["Time"] = pd.to_datetime(mark_df["Open time"], unit="ms")
    mark_df = mark_df.set_index("Time")[["markPrice"]].astype(float) # Convert price to float
    print(f"Fetched {len(mark_df)} mark price klines.")
else:
    print("No mark price kline data fetched.")
    mark_df = pd.DataFrame(columns=["markPrice"]) # Empty df with correct column

# --- Merge Data ---
print("\nMerging data...")
if df_fund.empty:
    print("Funding data is empty, cannot merge.")
    merged = pd.DataFrame(columns=["fundingTime", "symbol", "fundingRate", "spotPrice", "markPrice"])
else:
    # Set fundingTime as index for joining
    merged = df_fund.set_index("fundingTime")

    # Join Spot Price: Use 'asof' join to find the closest kline *before* the funding time
    # Ensure indices are sorted for asof join
    df_spot_sorted = df_spot.sort_index()
    merged = pd.merge_asof(merged.sort_index(), df_spot_sorted, left_index=True, right_index=True, direction='backward')

    # Join Mark Price: Use 'asof' join similarly
    mark_df_sorted = mark_df.sort_index()
    merged = pd.merge_asof(merged.sort_index(), mark_df_sorted, left_index=True, right_index=True, direction='backward')

    # Reset index to get fundingTime back as a column
    merged = merged.reset_index()

    # Reorder columns for desired output
    merged = merged[["fundingTime", "symbol", "fundingRate", "markPrice", "spotPrice"]]

# --- Final Output ---
# Mark price before ~2023-11-01 will be NaN because the data wasn't available from the kline endpoint
# The incorrect filling loop has been removed.

print("\nSample of merged data:")
print(merged.head())
print("\nCheck for NaN values in markPrice (expected before Nov 2023):")
print(merged.isnull().sum())

# Export
output_filename = "coinm_full_data_corrected.csv"
merged.to_csv(output_filename, index=False, date_format='%Y-%m-%d %H:%M:%S.%f') # Ensure milliseconds are saved if needed
print(f"\n✅ Saved {len(merged)} rows to {output_filename}")