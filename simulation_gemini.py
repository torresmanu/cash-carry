# Guarda este archivo como simulacion_cash_carry_completa.py
# Luego corrélo con: python simulacion_cash_carry_completa.py

import pandas as pd
import numpy as np

# Parámetros de simulación
INPUT_FILE = "coinm_full_data_2024_filled.csv"
OUTPUT_FILE = "simulacion_completa.csv"
INITIAL_CAPITAL_USD = 1000.0
CAPITAL_ALLOCATION_PCT = 0.95
SPOT_FEE_RATE = 0.001
FUTURES_FEE_RATE = 0.0005
MIN_TRADE_SIZE_BTC = 0.0001
MIN_FR_ENTRY = 0.0001
BASIS_ENTRY = 0.0015
FR_EXIT = 0.0
BASIS_EXIT = 0.0005

# Inicialización
df = pd.read_csv(INPUT_FILE, parse_dates=["fundingTime"])
df = df.sort_values("fundingTime")
df = df.set_index("fundingTime")

cash = INITIAL_CAPITAL_USD
btc_spot = 0.0
btc_funding = 0.0
in_position = False
cumulative_pnl = 0.0
entry_spot = entry_mark = 0.0
position_size = 0.0
entry_cost = 0.0

rows = []

for timestamp, row in df.iterrows():
    spot = row["spotPrice"]
    mark = row["markPrice"]
    fr = row["fundingRate"]
    basis_pct = (mark / spot) - 1 if spot else 0

    # Equity
    total_btc = btc_spot + btc_funding
    equity = cash + total_btc * spot

    funding_pnl = basis_pnl = 0.0
    funding_formula = basis_formula = ""
    costs = 0.0
    operacion = "nada"

    if in_position:
        # Funding
        funding_btc = position_size * fr
        btc_funding += funding_btc
        funding_pnl = funding_btc * spot
        funding_formula = f"{funding_btc:.6f} * {spot:.2f} = {funding_pnl:.2f}"

        # Condición de salida
        if fr <= FR_EXIT or basis_pct < BASIS_EXIT:
            reason = "FR" if fr <= FR_EXIT else "Basis"
            operacion = "cierre"
            total_btc_to_sell = btc_spot + btc_funding
            proceeds = total_btc_to_sell * spot * (1 - SPOT_FEE_RATE)
            futures_fee = position_size * mark * FUTURES_FEE_RATE
            cash += proceeds - futures_fee
            costs = entry_cost + (total_btc_to_sell * spot * SPOT_FEE_RATE) + futures_fee

            basis_move = (spot - mark) - (entry_spot - entry_mark)
            basis_pnl = position_size * basis_move
            basis_formula = f"({spot:.2f} - {mark:.2f}) - ({entry_spot:.2f} - {entry_mark:.2f}) = {basis_move:.2f}; {basis_move:.2f} * {position_size:.6f}"

            btc_spot = btc_funding = 0.0
            in_position = False
            position_size = 0.0
            entry_cost = 0.0

    else:
        # Entrada
        if fr > MIN_FR_ENTRY and basis_pct > BASIS_ENTRY:
            usd_to_spend = cash * CAPITAL_ALLOCATION_PCT
            potential_btc = usd_to_spend / spot
            if potential_btc >= MIN_TRADE_SIZE_BTC:
                entry_spot = spot
                entry_mark = mark
                position_size = potential_btc
                spot_cost = position_size * spot
                spot_fee = spot_cost * SPOT_FEE_RATE
                futures_fee = position_size * mark * FUTURES_FEE_RATE
                entry_cost = spot_fee + futures_fee
                total_needed = spot_cost + entry_cost

                if cash >= total_needed:
                    operacion = "apertura"
                    cash -= total_needed
                    btc_spot = position_size
                    btc_funding = 0.0
                    in_position = True

    # Calcular PnL
    total_pnl = funding_pnl + basis_pnl
    cumulative_pnl += total_pnl

    # Guardar fila
    rows.append({
        "timestamp": timestamp,
        "operacion": operacion,
        "spotPrice": spot,
        "markPrice": mark,
        "fundingRate": fr,
        "equity_usd": equity,
        "funding_pnl_usd": funding_pnl,
        "funding_pnl_formula": funding_formula,
        "basis_pnl_usd": basis_pnl,
        "basis_pnl_formula": basis_formula,
        "costs_usd": costs,
        "total_pnl_usd": total_pnl,
        "cumulative_pnl_usd": cumulative_pnl,
    })

# Exportar CSV
pd.DataFrame(rows).to_csv(OUTPUT_FILE, index=False)
print(f"✅ Archivo generado: {OUTPUT_FILE}")