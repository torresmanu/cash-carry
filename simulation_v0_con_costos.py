import pandas as pd

INPUT_CSV = "coinm_full_data_2024.csv"
OUTPUT_CSV = "strategy_results.csv"
INITIAL_CASH = 1000.0

df = pd.read_csv(INPUT_CSV, parse_dates=["fundingTime"]).sort_values("fundingTime")
cash = INITIAL_CASH
position = None
entry = {}
results = []
prev_rate = None  # Almacena el funding rate del ciclo anterior

for _, r in df.iterrows():
    current_rate = float(r["fundingRate"])
    t = r["fundingTime"]
    spot = r["spotPrice"] if pd.notna(r["spotPrice"]) else float(r["markPrice"])
    perp = float(r["markPrice"])
    spot = float(spot) if pd.notna(spot) else perp
    pnl = 0.0

    # Procesar funding basado en prev_rate si hay posición abierta
    if position == "ARB":
        if prev_rate is not None:
            funding_gain = cash * prev_rate
            funding_cost = funding_gain * 0.0015
            net_funding = funding_gain - funding_cost
            pnl += net_funding

        # Cerrar posición si el current_rate es ≤ 0
        if current_rate <= 0:
            basis_move = (spot - perp) - (entry["spot"] - entry["perp"])
            basis_gain = entry["size"] * basis_move
            closing_notional = entry["size"] * perp
            closing_cost = closing_notional * 0.0015
            pnl += (basis_gain - closing_cost)
            position = None

    # Abrir posición si el current_rate es positivo y no hay posición
    if current_rate > 0 and position is None:
        open_cost = cash * 0.0015
        cash -= open_cost
        position = "ARB"
        entry = {"spot": spot, "perp": perp, "size": cash / perp}

    cash += pnl
    results.append({
        "fundingTime": t,
        "Funding Rate": current_rate,
        "Position": position or "Cash",
        "Funding+Basis P&L": pnl,
        "Cash Balance": cash
    })
    prev_rate = current_rate  # Actualizar para la próxima iteración

out = pd.DataFrame(results)
out.to_csv(OUTPUT_CSV, index=False)

print(f"Start cash: ${INITIAL_CASH:.2f}")
print(f"End cash:   ${cash:.2f}")
print(f"Results saved to {OUTPUT_CSV}")