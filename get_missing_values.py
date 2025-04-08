import pandas as pd
import requests
import time
from datetime import datetime, timedelta

# --- Parámetros ---
CSV_PATH = "coinm_full_data_2024.csv"
OUTPUT_PATH = "coinm_full_data_2024_filled.csv"
SYMBOL = "BTCUSDT"
MAX_OFFSET_SECONDS = 300  # máximo buscar hasta 5 minutos (60*5)

# --- Función para obtener el precio spot más cercano ---
def get_spot_price_nearest(timestamp_ms):
    """
    Dado un timestamp en milisegundos, intenta obtener el precio spot más cercano
    desde la API de Binance con incrementos de 5 segundos.
    """
    base_url = "https://api.binance.com/api/v3/klines"
    interval = "1s"  # granularidad máxima permitida

    for offset in range(0, MAX_OFFSET_SECONDS + 1, 5):
        try_ts = timestamp_ms + (offset * 1000)
        params = {
            "symbol": SYMBOL,
            "interval": "1s",
            "startTime": try_ts,
            "limit": 1
        }
        response = requests.get(base_url, params=params)
        data = response.json()

        if isinstance(data, list) and len(data) > 0:
            price = float(data[0][4])  # precio de cierre
            found_time = pd.to_datetime(data[0][0], unit='ms')
            return price, found_time
    return None, None

# --- Cargar el CSV ---
df = pd.read_csv(CSV_PATH, parse_dates=["fundingTime"])

# --- Llenar valores vacíos ---
filled_count = 0

for idx, row in df.iterrows():
    if pd.isna(row["spotPrice"]):
        ts = int(row["fundingTime"].timestamp() * 1000)
        price, found_time = get_spot_price_nearest(ts)
        if price:
            df.at[idx, "spotPrice"] = price
            print(f"✅ Fecha buscada: {row['fundingTime']} | Encontrada: {found_time} | Spot Price: {price}")
            filled_count += 1
        else:
            print(f"❌ No se pudo encontrar spot para {row['fundingTime']}")

        time.sleep(0.25)  # respetar rate limits

print(f"\n🟢 Spot prices completados: {filled_count}")
df.to_csv(OUTPUT_PATH, index=False)
print(f"📁 Archivo guardado como: {OUTPUT_PATH}")