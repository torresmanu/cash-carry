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
    spot = r["spotPrice"] if pd.notna(r["spotPrice"]) else float(r["markPrice"])
    perp = float(r["markPrice"])
    basis = spot - perp
    pnl = 0.0

    # Open SHORT_PERP_LONG_SPOT if funding > 0
    if rate > 0 and position is None:
        position = "SHORT_PERP_LONG_SPOT"
        entry = {"spot": spot, "perp": perp, "size": cash / perp}

    # Open LONG_PERP_SHORT_SPOT if funding < 0 and basis > 0
    if rate < 0 and position is None and basis > 0:
        position = "LONG_PERP_SHORT_SPOT"
        entry = {"spot": spot, "perp": perp, "size": cash / perp}

    # Collect funding payment while in position
    if position in ("SHORT_PERP_LONG_SPOT", "LONG_PERP_SHORT_SPOT"):
        pnl += cash * rate

    # Close SHORT_PERP_LONG_SPOT when funding ≤ 0
    if position == "SHORT_PERP_LONG_SPOT" and rate <= 0:
        basis_move = (spot - perp) - (entry["spot"] - entry["perp"])
        pnl += entry["size"] * basis_move
        position = None

    # Close LONG_PERP_SHORT_SPOT when funding ≥ 0
    if position == "LONG_PERP_SHORT_SPOT" and rate >= 0:
        basis_move = (entry["spot"] - entry["perp"]) - (spot - perp)
        pnl += entry["size"] * basis_move
        position = None

    cash += pnl
    results.append({
        "fundingTime": t,
        "Funding Rate": rate,
        "Basis": basis,
        "Position": position or "CASH",
        "Funding+Basis P&L": pnl,
        "Cash Balance": cash
    })

out = pd.DataFrame(results)
out.to_csv(OUTPUT_CSV, index=False)

# APY Calculation
total_hours = (df["fundingTime"].max() - df["fundingTime"].min()).total_seconds() / 3600
total_days = total_hours / 24
apy = (cash / INITIAL_CASH) ** (365 / total_days) - 1

print(f"Start cash: ${INITIAL_CASH:.2f}")
print(f"End cash:   ${cash:.2f}")
print(f"Duration days: {total_days:.1f}")
print(f"Annual Percentage Yield (APY): {apy*100:.2f}%")
print(f"Results saved to {OUTPUT_CSV}")