import pandas as pd

INPUT_CSV  = "coinm_full_data_2024.csv"
OUTPUT_CSV = "strategy_results.csv"
INITIAL_CASH = 1000.0

df = pd.read_csv(INPUT_CSV, parse_dates=["fundingTime"]).sort_values("fundingTime")
cash = INITIAL_CASH
position = None
entry = {}
results = []

for _, r in df.iterrows():
    rate = float(r["fundingRate"])
    t = r["fundingTime"]
    spot = r["spotPrice"]
    perp = float(r["markPrice"])
    spot = float(spot) if pd.notna(spot) else perp

    pnl = 0.0

    # Open arbitrage when funding > 0
    if rate > 0 and position is None:
        position = "ARB"
        entry = {"spot": spot, "perp": perp}
        entry["size"] = cash / perp  # BTC notional

    # Collect funding payments while position is open
    if position == "ARB":
        pnl += cash * rate

    # Close arbitrage when funding <= 0
    if rate <= 0 and position == "ARB":
        basis_move = (spot - perp) - (entry["spot"] - entry["perp"])
        pnl += entry["size"] * basis_move
        position = None

    cash += pnl
    results.append({
        "fundingTime": t,
        "Funding Rate": rate,
        "Position": position or "Cash",
        "Funding+Basis P&L": pnl,
        "Cash Balance": cash
    })

out = pd.DataFrame(results)
out.to_csv(OUTPUT_CSV, index=False)

print(f"Start cash: ${INITIAL_CASH:.2f}")
print(f"End cash:   ${cash:.2f}")
print(f"Results saved to {OUTPUT_CSV}")