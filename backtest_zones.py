"""
backtest_zones_c2_entry.py
────────────────────────────────────────────────────────────
Improved backtest:
✔ Entry based on C2 candle logic
✔ SL based on C2 high/low
✔ ATR buffer added
✔ Realistic fill check (no fake entries)
✔ Keeps original structure stable
"""

import pandas as pd
from pathlib import Path


# ─────────────────────────────────────────────────────────────
# 🔧 CONFIG
# ─────────────────────────────────────────────────────────────

DATA_FOLDER = "data/enriched"
ZONE_FILE = "data/reports/zone_report.csv"
REPORT_FILE = "data/reports/backtest_detailed.csv"

RR_TARGET = 3
MAX_LOOKAHEAD = 150
MAX_ENTRY_DELAY = 150

TOLERANCE_ATR_RATIO = 0.1


# ─────────────────────────────────────────────────────────────
# 🔍 HELPERS
# ─────────────────────────────────────────────────────────────

def find_nearest_index(df, target_time):
    target_time = pd.to_datetime(target_time)
    return (df["datetime_ist"] - target_time).abs().idxmin()


def check_entry_touch(candle, zone_type, zone_high, zone_low, tolerance):
    """Check if price touched zone"""
    if zone_type == "Demand":
        return candle["low"] <= zone_high + tolerance
    else:
        return candle["high"] >= zone_low - tolerance


# ─────────────────────────────────────────────────────────────
# 🚀 BACKTEST
# ─────────────────────────────────────────────────────────────

def backtest():

    zones = pd.read_csv(ZONE_FILE)
    results = []

    for _, z in zones.iterrows():

        file_path = Path(DATA_FOLDER) / z["source_file"]
        df = pd.read_csv(file_path)

        df["datetime_ist"] = pd.to_datetime(df["datetime_ist"])
        df = df.sort_values("datetime_ist").reset_index(drop=True)

        start_idx = find_nearest_index(df, z["c3_datetime"])

        zone_high = z["zone_high"]
        zone_low = z["zone_low"]
        atr = z["atr_at_signal"]

        tolerance = TOLERANCE_ATR_RATIO * atr
        sl_buffer = 0.1 * atr

        # ───────── C2 DATA ─────────
        c2_open = z["c2_open"]
        c2_close = z["c2_close"]
        c2_high = z.get("c2_high", max(c2_open, c2_close))
        c2_low = z.get("c2_low", min(c2_open, c2_close))

        c2_is_green = c2_close > c2_open

        entry_price = None
        sl = None
        tp = None
        entry_time = None
        exit_time = None
        result = "no_entry"
        holding_candles = 0

        for i in range(start_idx + 1, min(start_idx + MAX_LOOKAHEAD, len(df))):

            candle = df.iloc[i]

            # ───────── ENTRY ─────────
            if entry_price is None:

                if i - start_idx > MAX_ENTRY_DELAY:
                    break

                # zone touch required
                if not check_entry_touch(candle, z["zone_type"], zone_high, zone_low, tolerance):
                    continue

                # 🎯 ENTRY BASED ON C2
                if z["zone_type"] == "Demand":

                    entry_price = c2_close if c2_is_green else c2_open
                    sl = c2_low - sl_buffer
                    risk = entry_price - sl
                    tp = entry_price + RR_TARGET * risk

                else:  # Supply

                    entry_price = c2_close if not c2_is_green else c2_open
                    sl = c2_high + sl_buffer
                    risk = sl - entry_price
                    tp = entry_price - RR_TARGET * risk

                # ✅ REAL FILL CHECK (IMPORTANT)
                if not (candle["low"] <= entry_price <= candle["high"]):
                    entry_price = None
                    sl = None
                    tp = None
                    continue

                entry_time = candle["datetime_ist"]

            # ───────── EXIT ─────────
            else:

                holding_candles += 1

                high = candle["high"]
                low = candle["low"]

                if z["zone_type"] == "Demand":

                    if low <= sl:
                        result = "loss"
                        exit_time = candle["datetime_ist"]
                        break

                    if high >= tp:
                        result = "win"
                        exit_time = candle["datetime_ist"]
                        break

                else:

                    if high >= sl:
                        result = "loss"
                        exit_time = candle["datetime_ist"]
                        break

                    if low <= tp:
                        result = "win"
                        exit_time = candle["datetime_ist"]
                        break

        # ───────── PNL ─────────
        pnl_r = 0

        if entry_price is not None and result != "no_entry":

            if z["zone_type"] == "Demand":
                risk = entry_price - sl
                reward = tp - entry_price
            else:
                risk = sl - entry_price
                reward = entry_price - tp

            pnl_r = reward / risk if result == "win" else -1

        results.append({
            "symbol": z["symbol"],
            "zone_type": z["zone_type"],
            "strength": z["strength"],
            "score": z["score"],
            "entry_time": entry_time,
            "exit_time": exit_time,
            "entry_price": entry_price,
            "sl": sl,
            "tp": tp,
            "holding_candles": holding_candles,
            "result": result,
            "pnl_R": round(pnl_r, 2)
        })

    columns = [
        "symbol", "zone_type", "strength", "score",
        "entry_time", "exit_time",
        "entry_price", "sl", "tp",
        "holding_candles", "result", "pnl_R"
    ]

    df = pd.DataFrame(results).reindex(columns=columns)
    return df


# ─────────────────────────────────────────────────────────────
# 📊 RUN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":

    df = backtest()

    print("\n===== BACKTEST RESULT =====\n")

    if df.empty:
        print("⚠️ No trades found. Try relaxing filters.")
        exit()

    total = len(df)
    wins = len(df[df["result"] == "win"])
    losses = len(df[df["result"] == "loss"])
    no_entry = len(df[df["result"] == "no_entry"])

    print(f"Total Trades: {total}")
    print(f"Wins: {wins}")
    print(f"Losses: {losses}")
    print(f"No Entry: {no_entry}")

    if wins + losses > 0:
        winrate = wins / (wins + losses) * 100
        print(f"Win Rate: {winrate:.2f}%")

    print("\nBreakdown by Strength:")
    print(df.groupby("strength")["result"].value_counts())

    Path("data/reports").mkdir(parents=True, exist_ok=True)
    df.to_csv(REPORT_FILE, index=False)

    print(f"\nDetailed report saved at: {REPORT_FILE}")