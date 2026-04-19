"""
fyers_history/history.py
------------------------
Fetch OHLCV historical candle data for a list of symbols from Fyers API v3.

Changes:
  - Timestamps are converted to IST (UTC+05:30) instead of UTC.
  - Each symbol is saved to its own CSV file inside the output directory.

Usage (as a module):
    from fyers_history.history import fetch_history

    results = fetch_history(
        stock_file = "stocks.txt",
        range_from = "2026-01-01",
        range_to   = "2026-03-31",
        resolution = "D",           # "D" | "1" | "5" | "15" | "60" | "1W" …
        output_dir = "output/",     # each symbol → output/NSE_SBIN-EQ.csv
    )
    # results is a dict  { "NSE:SBIN-EQ": DataFrame, … }

Usage (CLI):
    python -m fyers_history.history \\
        --file stocks.txt \\
        --from 2026-01-01 \\
        --to   2026-03-31 \\
        --res  D \\
        --out  output/
"""

import argparse
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from time import sleep
import pandas as pd
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

# ── constants ─────────────────────────────────────────────────────────────────

IST = timezone(timedelta(hours=5, minutes=30))   # UTC+05:30, no pytz needed

VALID_RESOLUTIONS = {
    # seconds
    "5S", "10S", "15S", "30S", "45S",
    # minutes
    "1", "2", "3", "5", "10", "15", "20", "30", "60", "120", "240",
    # day / week / month
    "D", "1D", "1W", "1M",
}

CANDLE_COLUMNS = ["datetime_ist", "open", "high", "low", "close", "volume"]


# ── helpers ───────────────────────────────────────────────────────────────────

def _build_fyers() -> fyersModel.FyersModel:
    """Initialise FyersModel from environment variables."""
    load_dotenv(override=True)
    client_id    = os.getenv("client_id")
    access_token = os.getenv("access_token")
    if not client_id or not access_token:
        raise EnvironmentError(
            "Both 'client_id' and 'access_token' must be set in .env"
        )
    return fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path="",
    )


def _load_symbols(stock_file: str | Path) -> list[str]:
    """
    Read symbols from *stock_file* – one per line.
    Blank lines and lines starting with '#' are ignored.
    """
    path = Path(stock_file)
    if not path.exists():
        raise FileNotFoundError(f"Stock file not found: {path.resolve()}")

    symbols = [
        line.strip().upper()
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    if not symbols:
        raise ValueError(f"No symbols found in '{path}'")

    return symbols


def _epoch_to_ist(epoch: int) -> str:
    """
    Convert a Unix epoch (seconds, UTC) to an IST datetime string.
    Example:  1772409600  →  "2026-03-01 05:30:00"
    """
    dt_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
    dt_ist = dt_utc.astimezone(IST)
    return dt_ist.strftime("%Y-%m-%d %H:%M:%S")


def _symbol_to_filename(symbol: str) -> str:
    """
    Turn a Fyers symbol into a safe filename.
    NSE:SBIN-EQ  →  NSE_SBIN-EQ.csv
    """
    return symbol.replace(":", "_") + ".csv"


def _fetch_symbol(
    fyers: fyersModel.FyersModel,
    symbol: str,
    resolution: str,
    range_from: str,
    range_to: str,
) -> pd.DataFrame:
    """
    Fetch candles for a single *symbol* and return a tidy DataFrame
    with IST-converted timestamps.  Returns an empty DataFrame on error.
    """
    payload = {
        "symbol":      symbol,
        "resolution":  resolution,
        "date_format": "1",
        "range_from":  range_from,
        "range_to":    range_to,
        "cont_flag":   "1",
        "oi_flag":     "1",
    }

    response = fyers.history(data=payload)
    sleep(5)

    if response.get("s") != "ok":
        print(f"  [WARN] {symbol}: API error – {response.get('message', response)}")
        return pd.DataFrame()

    candles = response.get("candles", [])
    if not candles:
        print(f"  [INFO] {symbol}: no data returned for the given range.")
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=CANDLE_COLUMNS)

    # ── epoch  →  IST string ──────────────────────────────────────────────────
    df["datetime_ist"] = df["datetime_ist"].apply(_epoch_to_ist)

    # ── tag with symbol ───────────────────────────────────────────────────────
    df.insert(0, "symbol", symbol)

    return df


def _save_symbol(df: pd.DataFrame, symbol: str, output_dir: Path) -> Path:
    """Write *df* to  <output_dir>/<safe_symbol>.csv  and return the path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / _symbol_to_filename(symbol)
    df.to_csv(out_path, index=False)
    return out_path


def _print_preview(df: pd.DataFrame, label: str, rows: int = 3) -> None:
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 130)
    pd.set_option("display.float_format", "{:,.2f}".format)
    print(f"\n  ┌─ {label} ({len(df)} candles) ─────────────────────────────────")
    print(df.head(rows).to_string(index=False, col_space=12))
    if len(df) > rows * 2:
        print("  │  …")
        print(df.tail(rows).to_string(index=False, col_space=12, header=False))
    print("  └─────────────────────────────────────────────────────────────")


# ── public API ────────────────────────────────────────────────────────────────

def fetch_history(
    stock_file: str | Path,
    range_from: str,
    range_to: str,
    resolution: str = "D",
    output_dir: str | Path | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Fetch OHLCV candle data for every symbol in *stock_file*.

    Parameters
    ----------
    stock_file  : Path to a plain-text file with one Fyers symbol per line.
                  Example line:  NSE:SBIN-EQ
    range_from  : Start date  ``YYYY-MM-DD``  e.g. ``"2026-01-01"``
    range_to    : End date    ``YYYY-MM-DD``  e.g. ``"2026-03-31"``
    resolution  : Candle interval. Supported values:

                  Seconds  → "5S" "10S" "15S" "30S" "45S"
                  Minutes  → "1" "2" "3" "5" "10" "15" "20" "30" "60" "120" "240"
                  Daily    → "D"  (alias "1D")
                  Weekly   → "1W"
                  Monthly  → "1M"

    output_dir  : Directory where per-symbol CSV files are saved.
                  e.g. ``"output/"``  →  ``output/NSE_SBIN-EQ.csv``
                  Pass ``None`` to skip saving.

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping of  symbol  →  DataFrame with columns:
        symbol | datetime_ist | open | high | low | close | volume

        ``datetime_ist`` is in IST (UTC+05:30), format: ``YYYY-MM-DD HH:MM:SS``
    """
    # ── validation ────────────────────────────────────────────────────────────
    if resolution not in VALID_RESOLUTIONS:
        raise ValueError(
            f"Invalid resolution '{resolution}'. "
            f"Valid values: {sorted(VALID_RESOLUTIONS)}"
        )
    try:
        datetime.strptime(range_from, "%Y-%m-%d")
        datetime.strptime(range_to,   "%Y-%m-%d")
    except ValueError:
        raise ValueError("range_from and range_to must be in YYYY-MM-DD format.")

    # ── setup ─────────────────────────────────────────────────────────────────
    symbols   = _load_symbols(stock_file)
    fyers     = _build_fyers()
    out_dir   = Path(output_dir) if output_dir else None
    results: dict[str, pd.DataFrame] = {}

    print(
        f"\n{'='*62}\n"
        f"  Fyers History Fetch\n"
        f"  Symbols    : {len(symbols)}\n"
        f"  Resolution : {resolution}\n"
        f"  Range      : {range_from}  →  {range_to}  (IST)\n"
        f"  Output dir : {out_dir.resolve() if out_dir else 'not saving'}\n"
        f"{'='*62}"
    )

    # ── fetch each symbol ─────────────────────────────────────────────────────
    saved_files: list[Path] = []

    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] {symbol}", end="  … ", flush=True)
        df = _fetch_symbol(fyers, symbol, resolution, range_from, range_to)

        if df.empty:
            continue

        results[symbol] = df
        print(f"{len(df)} candles  |  "
              f"{df['datetime_ist'].iloc[0]}  →  {df['datetime_ist'].iloc[-1]}  IST")

        _print_preview(df, symbol)

        if out_dir:
            saved = _save_symbol(df, symbol, out_dir)
            saved_files.append(saved)
            print(f"  [saved] → {saved}")

    # ── summary ───────────────────────────────────────────────────────────────
    total = sum(len(v) for v in results.values())
    print(f"\n{'='*62}")
    print(f"  Done.  {len(results)}/{len(symbols)} symbols fetched  |  {total:,} total candles")
    if saved_files:
        print(f"  Files saved to: {out_dir.resolve()}/")
        for f in saved_files:
            print(f"    {f.name}")
    print(f"{'='*62}\n")

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch Fyers OHLCV history for a list of symbols.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--file", default="stocks.txt",
                   help="Path to symbol list file  (default: stocks.txt)")
    p.add_argument("--from", dest="range_from", required=True,
                   metavar="YYYY-MM-DD", help="Start date")
    p.add_argument("--to",   dest="range_to",   required=True,
                   metavar="YYYY-MM-DD", help="End date")
    p.add_argument("--res",  dest="resolution", default="D",
                   help="Candle resolution  (default: D)\n"
                        "Examples: D  1W  1M  1  5  15  60  120  240  5S  15S")
    p.add_argument("--out",  dest="output_dir", default=None,
                   metavar="DIR",
                   help="Directory to save per-symbol CSV files")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    fetch_history(
        stock_file = args.file,
        range_from = args.range_from,
        range_to   = args.range_to,
        resolution = args.resolution,
        output_dir = args.output_dir,
    )
