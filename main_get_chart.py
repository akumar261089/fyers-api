"""
main.py
-------
Entry-point: authenticate, then pull per-symbol historical data.
"""

import os
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fyers_auth import authenticate, is_token_valid
from fyers_history import fetch_history
today = datetime.now()
three_months_ago = today - relativedelta(months=3)
load_dotenv()

# ── 1. Ensure a valid token exists ───────────────────────────────────────────

if not is_token_valid():
    print("Token missing or expired – starting authentication …")
    authenticate(browser="chrome")   # "firefox" / None for system default
else:
    print("Token is valid ✓")


# ── 2. Fetch historical data ─────────────────────────────────────────────────

results = fetch_history(
    stock_file = "stocks.txt",   # one Fyers symbol per line
    range_from=three_months_ago.strftime("%Y-%m-%d"),  # last 3 months
    range_to=today.strftime("%Y-%m-%d"),               # today
    resolution = "120",            # see resolution table below
    output_dir = f"data/output-{datetime.now().strftime('%d-%m-%Y-%H-%M')}",      # each symbol → output/NSE_SBIN-EQ.csv
                                 # set to None to skip saving files
)

# results → dict { symbol: pd.DataFrame }
# Columns : symbol | datetime_ist | open | high | low | close | volume
for symbol, df in results.items():
    print(f"\n── {symbol} ({len(df)} candles) ──────────────────────────────")
    print(df.head(3).to_string(index=False))


# ── Resolution reference ─────────────────────────────────────────────────────
#
#  Seconds  : "5S"  "10S"  "15S"  "30S"  "45S"
#  Minutes  : "1"   "2"    "3"    "5"    "10"   "15"  "20"  "30"  "60"  "120"  "240"
#  Daily    : "D"   (alias "1D")
#  Weekly   : "1W"
#  Monthly  : "1M"

# ── CLI equivalent ────────────────────────────────────────────────────────────
#
#  python -m fyers_history.history \
#      --file stocks.txt \
#      --from 2026-01-01 \
#      --to   2026-03-31 \
#      --res  D \
#      --out  output/
