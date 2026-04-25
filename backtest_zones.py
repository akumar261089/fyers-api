import pandas as pd
from pathlib import Path

DATA_FOLDER = "data/enriched"
ZONE_FILE = "data/reports/zone_report.csv"
REPORT_FILE = "data/reports/backtest_detailed.csv"

RR_TARGET = 5
MAX_LOOKAHEAD = 50


def backtest():

    zones = pd.read_csv(ZONE_FILE)
    results = []

    for _, z in zones.iterrows():

        file_path = Path(DATA_FOLDER) / z["source_file"]
        df = pd.read_csv(file_path)

        df["datetime_ist"] = pd.to_datetime(df["datetime_ist"])
        df = df.sort_values("datetime_ist").reset_index(drop=True)

        start_idx = df.index[df["datetime_ist"] == pd.to_datetime(z["c3_datetime"])]

        if len(start_idx) == 0:
            continue

        start_idx = start_idx[0]

        zone_high = z["zone_high"]
        zone_low = z["zone_low"]
        atr = z["atr_at_signal"]

        tolerance = 0.1 * atr

        entry_price = None
        sl = None
        tp = None
        entry_time = None
        exit_time = None
        result = "no_entry"
        holding_candles = 0

        for i in range(start_idx + 1, min(start_idx + MAX_LOOKAHEAD, len(df))):

            candle = df.iloc[i]
            high = candle["high"]
            low = candle["low"]

            # ───────── ENTRY ─────────
            if entry_price is None:

                if z["zone_type"] == "Demand":
                    if low <= zone_high + tolerance:
                        entry_price = zone_high
                        sl = zone_low - tolerance
                        risk = entry_price - sl
                        tp = entry_price + RR_TARGET * risk
                        entry_time = candle["datetime_ist"]

                else:
                    if high >= zone_low - tolerance:
                        entry_price = zone_low
                        sl = zone_high + tolerance
                        risk = sl - entry_price
                        tp = entry_price - RR_TARGET * risk
                        entry_time = candle["datetime_ist"]

            # ───────── EXIT ─────────
            else:

                holding_candles += 1

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

        # ───────── PNL CALCULATION ─────────
        pnl_r = 0

        if entry_price is not None and result != "no_entry":

            if z["zone_type"] == "Demand":
                risk = entry_price - sl
                reward = tp - entry_price
            else:
                risk = sl - entry_price
                reward = entry_price - tp

            if result == "win":
                pnl_r = reward / risk
            elif result == "loss":
                pnl_r = -1

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

    return pd.DataFrame(results)


if __name__ == "__main__":

    df = backtest()

    print("\n===== BACKTEST RESULT =====\n")

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

    # ✅ SAVE REPORT
    Path("data/reports").mkdir(parents=True, exist_ok=True)
    df.to_csv(REPORT_FILE, index=False)

    print(f"\nDetailed report saved at: {REPORT_FILE}")