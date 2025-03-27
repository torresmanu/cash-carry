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
    pnl = 0.0

    # Open long‑spot/short‑perp arb if funding > 0
    if rate > 0 and position is None:
        position = "ARB_LONG"
        entry = {"spot": spot, "perp": perp, "size": cash / perp}

    # Open short‑spot/long‑perp arb if funding < 0
    if rate < 0 and position is None:
        position = "ARB_SHORT"
        entry = {"spot": spot, "perp": perp, "size": cash / perp}

    # Collect funding payments while position open
    if position in ("ARB_LONG","ARB_SHORT"):
        pnl += cash * rate

    # Close any arb when sign flips to zero or opposite
    if position == "ARB_LONG" and rate <= 0:
        basis_move = (spot - perp) - (entry["spot"] - entry["perp"])
        pnl += entry["size"] * basis_move
        position = None

    if position == "ARB_SHORT" and rate >= 0:
        # For reverse arb basis: flip sign
        basis_move = (entry["spot"] - entry["perp"]) - (spot - perp)
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

# APY
total_hours = (df["fundingTime"].max() - df["fundingTime"].min()).total_seconds()/3600
apy = (cash/INITIAL_CASH)**(365/(total_hours/24)) - 1

print(f"Start cash: ${INITIAL_CASH:.2f}")
print(f"End cash:   ${cash:.2f}")
print(f"Duration days: {total_hours/24:.1f}")
print(f"APY: {apy*100:.2f}%")
print(f"Results → {OUTPUT_CSV}")