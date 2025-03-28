import requests, pandas as pd
import time

# Config
START = int(pd.Timestamp("2023-01-01 00:00:00").timestamp() * 1000)
END   = int(pd.Timestamp("2023-11-28 23:00:00").timestamp() * 1000)
LIMIT = 1000

FUND_URL     = "https://dapi.binance.com/dapi/v1/fundingRate"
MARKKL_URL   = "https://dapi.binance.com/dapi/v1/markPriceKlines"
PREMIUM_URL  = "https://dapi.binance.com/dapi/v1/premiumIndex"
SPOT_URL     = "https://api.binance.com/api/v3/klines"
SYMBOL_PERP  = "BTCUSD_PERP"
SYMBOL_SPOT  = "BTCUSDT"

def fetch_klines(url, symbol, interval="1h"):
    params = {"symbol": symbol, "interval": interval, "startTime": START, "endTime": END, "limit": LIMIT}
    all_data = []
    while True:
        resp = requests.get(url, params=params).json()
        if not resp: break
        all_data.extend(resp)
        last = resp[-1][0]
        if last >= END or len(resp) < LIMIT:
            break
        params["startTime"] = last + 1
        time.sleep(0.2)
    return pd.DataFrame(all_data)

# Funding history
fund = requests.get(FUND_URL, params={"symbol": SYMBOL_PERP, "startTime": START, "endTime": END, "limit": LIMIT}).json()
df_fund = pd.DataFrame(fund)
df_fund["fundingTime"] = pd.to_datetime(df_fund["fundingTime"], unit="ms")

# Spot price
df_spot = fetch_klines(SPOT_URL, SYMBOL_SPOT)
df_spot = df_spot[[0,4]].rename(columns={0:"Open time",4:"spotPrice"})
df_spot["Time"] = pd.to_datetime(df_spot["Open time"], unit="ms")
df_spot = df_spot.set_index("Time")[["spotPrice"]]

# Historical mark-price (>=2023-11-01)
mark_df = fetch_klines(MARKKL_URL, SYMBOL_PERP)
mark_df = mark_df[[0,4]].rename(columns={0:"Open time",4:"markPrice"})
mark_df["Time"] = pd.to_datetime(mark_df["Open time"], unit="ms")
mark_df = mark_df.set_index("Time")[["markPrice"]]

# --- Merge spot + funding ---
merged = df_fund.set_index("fundingTime").join(df_spot, how="left").reset_index()

# Drop any existing markPrice column to avoid overlap
if "markPrice" in merged.columns:
    merged = merged.drop(columns="markPrice")

# --- Merge mark-price where available ---
merged = merged.set_index("fundingTime").join(mark_df, how="left").reset_index()

# Fill missing markPrice before 2023-11-01 via premiumIndex
cutoff = pd.Timestamp("2023-11-01 00:00:00")
for idx, row in merged[merged["fundingTime"] < cutoff].iterrows():
    resp = requests.get(PREMIUM_URL, params={"symbol": SYMBOL_PERP})
    rec = resp.json()[0] if isinstance(resp.json(), list) else resp.json()
    merged.at[idx, "markPrice"] = float(rec["markPrice"])
    time.sleep(0.1)

# Export
merged.to_csv("coinm_full_data.csv", index=False)
print(f"✅ Saved {len(merged)} rows to coinm_full_data.csv")