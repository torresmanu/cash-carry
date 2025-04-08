import pandas as pd

# Parámetros
INPUT_CSV = "coinm_full_data_2024.csv"
OUTPUT_CSV = "basis_empirical_results.csv"
INITIAL_CASH = 1000.0
THRESHOLD = 0.005   # 0.5%
HOLD_STEPS = 3      # 24h
SPOT_FEE = 0.001
PERP_FEE = 0.0005

# Cargar data
df = pd.read_csv(INPUT_CSV, parse_dates=["fundingTime"]).sort_values("fundingTime").reset_index(drop=True)
df["spotPrice"] = df["spotPrice"].fillna(df["markPrice"])  # fallback
df["basis"] = df["spotPrice"] - df["markPrice"]
df["basis_pct"] = df["basis"] / df["spotPrice"]

# Simulación
cash = INITIAL_CASH
results = []
i = 0

while i < len(df) - HOLD_STEPS:
    row = df.iloc[i]
    spot_entry = row["spotPrice"]
    mark_entry = row["markPrice"]
    rate_entry = row["fundingRate"]
    basis_pct = row["basis_pct"]
    entry_time = row["fundingTime"]
    pnl = 0.0
    action = "Hold"

    # Validación
    if pd.isna(spot_entry) or pd.isna(mark_entry) or spot_entry <= 0 or mark_entry <= 0:
        i += 1
        continue

    if abs(basis_pct) >= THRESHOLD:
        # Salida
        exit_row = df.iloc[i + HOLD_STEPS]
        spot_exit = exit_row["spotPrice"]
        mark_exit = exit_row["markPrice"]
        funding_rates = df.iloc[i+1:i+HOLD_STEPS+1]["fundingRate"].tolist()

        if pd.isna(spot_exit) or pd.isna(mark_exit) or spot_exit <= 0 or mark_exit <= 0:
            i += 1
            continue

        direction = "spot_gt_mark" if basis_pct > 0 else "mark_gt_spot"

        notional = cash
        spot_fee_entry = notional * SPOT_FEE
        perp_fee_entry = notional * PERP_FEE
        cash -= (spot_fee_entry + perp_fee_entry)

        size = notional / spot_entry if direction == "spot_gt_mark" else notional / mark_entry
        if size <= 0 or pd.isna(size):
            i += 1
            continue

        if direction == "spot_gt_mark":
            spot_pnl = (spot_exit - spot_entry) * size
            perp_pnl = (mark_entry - mark_exit) * size
        else:
            spot_pnl = (spot_entry - spot_exit) * size
            perp_pnl = (mark_exit - mark_entry) * size

        funding_pnl = sum([
            rate * size * (mark_entry if direction == "spot_gt_mark" else spot_entry)
            for rate in funding_rates if pd.notna(rate)
        ])

        close_notional = size * (spot_exit + mark_exit) / 2
        spot_fee_exit = close_notional * SPOT_FEE
        perp_fee_exit = close_notional * PERP_FEE

        pnl = spot_pnl + perp_pnl + funding_pnl - spot_fee_exit - perp_fee_exit
        cash += pnl
        action = f"TRADE {direction.upper()}"
        i += HOLD_STEPS
    else:
        i += 1

    results.append({
        "Time": entry_time,
        "Basis %": basis_pct,
        "Action": action,
        "PnL": pnl,
        "Cash Balance": cash
    })

# Guardar resultados
out = pd.DataFrame(results)
out.to_csv(OUTPUT_CSV, index=False)

# Resumen
print(f"\n✅ Backtest finalizado")
print(f"Start cash: ${INITIAL_CASH:.2f}")
print(f"End cash:   ${cash:.2f}")
print(f"Total trades: {out['Action'].str.contains('TRADE').sum()}")
print(f"Archivo exportado: {OUTPUT_CSV}")

# Top y bottom trades
trades = out[out["Action"].str.contains("TRADE")]
print("\n🔝 Mejores trades:")
print(trades.sort_values("PnL", ascending=False).head(5)[["Time", "PnL", "Cash Balance"]].to_string(index=False))

print("\n🔻 Peores trades:")
print(trades.sort_values("PnL").head(5)[["Time", "PnL", "Cash Balance"]].to_string(index=False))