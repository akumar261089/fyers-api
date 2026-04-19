"""
technical_indicators.py
-----------------------
A reusable module that reads every CSV file in an input folder,
appends a set of technical-indicator columns, and writes the
enriched files to an output folder.

Indicators added
----------------
Candle
  red_candle          bool   close < open
  green_candle        bool   close >= open
  candle_body         float  abs(close - open)
  upper_shadow        float  high - max(open, close)
  lower_shadow        float  min(open, close) - low

Volatility
  true_range          float  TR = max(H-L, |H-Cp|, |L-Cp|)
  atr_14              float  14-period EWM ATR (Wilder's method)

Trend / MA
  sma_20              float  20-period simple moving average of close
  ema_20              float  20-period exponential moving average of close

Momentum
  rsi_14              float  14-period RSI
  macd                float  MACD line  (EMA12 - EMA26)
  macd_signal         float  9-period EMA of MACD line
  macd_hist           float  macd - macd_signal

Volume
  volume_sma_20       float  20-period SMA of volume
  relative_volume     float  volume / volume_sma_20

Bollinger Bands (20, 2)
  bb_upper            float
  bb_middle           float  (same as sma_20)
  bb_lower            float
  bb_width            float  (bb_upper - bb_lower) / bb_middle

Usage (from main.py)
--------------------
    from technical_indicators import add_indicators_to_folder

    add_indicators_to_folder(
        input_folder  = "data/raw",
        output_folder = "data/enriched",   # created automatically if absent
        atr_period    = 14,                # optional, default 14
        sma_period    = 20,                # optional, default 20
        ema_period    = 20,                # optional, default 20
        rsi_period    = 14,                # optional, default 14
        bb_period     = 20,                # optional, default 20
        bb_std        = 2.0,               # optional, default 2.0
    )
"""

from __future__ import annotations

import os
import glob
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
#  Low-level indicator functions
#  Each function accepts a DataFrame and returns a new column (Series).
# ═════════════════════════════════════════════════════════════════════════════

def calc_red_candle(df: pd.DataFrame) -> pd.Series:
    """True when the candle closed lower than it opened."""
    return (df["close"] < df["open"]).rename("red_candle")


def calc_green_candle(df: pd.DataFrame) -> pd.Series:
    """True when the candle closed at or above its open."""
    return (df["close"] >= df["open"]).rename("green_candle")


def calc_candle_body(df: pd.DataFrame) -> pd.Series:
    return (df["close"] - df["open"]).abs().rename("candle_body")


def calc_upper_shadow(df: pd.DataFrame) -> pd.Series:
    return (df["high"] - df[["open", "close"]].max(axis=1)).rename("upper_shadow")


def calc_lower_shadow(df: pd.DataFrame) -> pd.Series:
    return (df[["open", "close"]].min(axis=1) - df["low"]).rename("lower_shadow")


def calc_true_range(df: pd.DataFrame) -> pd.Series:
    """
    True Range = max(H - L,  |H - prev_close|,  |L - prev_close|)
    First row uses only H - L (no previous close available).
    """
    high  = df["high"]
    low   = df["low"]
    prev_close = df["close"].shift(1)

    hl  = high - low
    hpc = (high - prev_close).abs()
    lpc = (low  - prev_close).abs()

    tr = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    tr.iloc[0] = high.iloc[0] - low.iloc[0]   # seed first row
    return tr.rename("true_range")


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Wilder's ATR: exponential smoothing with alpha = 1/period.
    Seeded with the simple mean of the first `period` True Range values.
    """
    tr = calc_true_range(df)
    atr = tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    return atr.rename(f"atr_{period}")


def calc_sma(df: pd.DataFrame, period: int = 20, col: str = "close") -> pd.Series:
    return df[col].rolling(window=period).mean().rename(f"sma_{period}")


def calc_ema(df: pd.DataFrame, period: int = 20, col: str = "close") -> pd.Series:
    return df[col].ewm(span=period, adjust=False).mean().rename(f"ema_{period}")


def calc_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Wilder's RSI using EWM smoothing."""
    delta = df["close"].diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)

    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs  = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.rename(f"rsi_{period}")


def calc_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """Returns a 3-column DataFrame: macd, macd_signal, macd_hist."""
    ema_fast   = df["close"].ewm(span=fast,   adjust=False).mean()
    ema_slow   = df["close"].ewm(span=slow,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist        = macd_line - signal_line

    return pd.DataFrame({
        "macd":        macd_line,
        "macd_signal": signal_line,
        "macd_hist":   hist,
    }, index=df.index)


def calc_bollinger_bands(
    df: pd.DataFrame,
    period: int = 20,
    num_std: float = 2.0,
) -> pd.DataFrame:
    """Returns bb_upper, bb_middle, bb_lower, bb_width."""
    middle = df["close"].rolling(window=period).mean()
    std    = df["close"].rolling(window=period).std(ddof=0)
    upper  = middle + num_std * std
    lower  = middle - num_std * std
    width  = (upper - lower) / middle.replace(0, np.nan)

    return pd.DataFrame({
        "bb_upper":  upper,
        "bb_middle": middle,
        "bb_lower":  lower,
        "bb_width":  width,
    }, index=df.index)


def calc_volume_sma(df: pd.DataFrame, period: int = 20) -> pd.Series:
    return df["volume"].rolling(window=period).mean().rename(f"volume_sma_{period}")


def calc_relative_volume(df: pd.DataFrame, period: int = 20) -> pd.Series:
    vol_sma = calc_volume_sma(df, period)
    return (df["volume"] / vol_sma.replace(0, np.nan)).rename("relative_volume")


# ═════════════════════════════════════════════════════════════════════════════
#  Core enrichment function – operates on a single DataFrame
# ═════════════════════════════════════════════════════════════════════════════

def enrich_dataframe(
    df: pd.DataFrame,
    atr_period: int   = 14,
    sma_period: int   = 20,
    ema_period: int   = 20,
    rsi_period: int   = 14,
    bb_period:  int   = 20,
    bb_std:     float = 2.0,
) -> pd.DataFrame:
    """
    Append all technical-indicator columns to *df* and return the result.
    The input DataFrame must have columns: open, high, low, close, volume.
    Rows are assumed to be sorted in ascending chronological order.
    """
    df = df.copy()

    # ── candle geometry ──────────────────────────────────────────────────────
    df["red_candle"]   = calc_red_candle(df)
    df["green_candle"] = calc_green_candle(df)
    df["candle_body"]  = calc_candle_body(df)
    df["upper_shadow"] = calc_upper_shadow(df)
    df["lower_shadow"] = calc_lower_shadow(df)

    # ── volatility ───────────────────────────────────────────────────────────
    df["true_range"]          = calc_true_range(df)
    df[f"atr_{atr_period}"]   = calc_atr(df, atr_period)

    # ── trend / moving averages ──────────────────────────────────────────────
    df[f"sma_{sma_period}"]   = calc_sma(df, sma_period)
    df[f"ema_{ema_period}"]   = calc_ema(df, ema_period)

    # ── momentum ─────────────────────────────────────────────────────────────
    df[f"rsi_{rsi_period}"]   = calc_rsi(df, rsi_period)
    macd_df                   = calc_macd(df)
    df = pd.concat([df, macd_df], axis=1)

    # ── bollinger bands ──────────────────────────────────────────────────────
    bb_df = calc_bollinger_bands(df, bb_period, bb_std)
    df    = pd.concat([df, bb_df], axis=1)

    # ── volume ───────────────────────────────────────────────────────────────
    df[f"volume_sma_{sma_period}"] = calc_volume_sma(df, sma_period)
    df["relative_volume"]          = calc_relative_volume(df, sma_period)
    df["prev_close"] = df["close"].shift(1)
    df["gap"] = df["open"] - df["prev_close"]
    df["gap_pct"] = df["gap"] / df["prev_close"]
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  Public entry point – called from main.py
# ═════════════════════════════════════════════════════════════════════════════

def add_indicators_to_folder(
    input_folder:  str,
    output_folder: str,
    atr_period:    int   = 14,
    sma_period:    int   = 20,
    ema_period:    int   = 20,
    rsi_period:    int   = 14,
    bb_period:     int   = 20,
    bb_std:        float = 2.0,
    glob_pattern:  str   = "*.csv",
) -> None:
    """
    Process every CSV in *input_folder*, add indicator columns,
    and save enriched files to *output_folder* (created if needed).

    Parameters
    ----------
    input_folder  : folder containing raw CSV files
    output_folder : destination folder for enriched CSV files
    atr_period    : ATR look-back period (default 14)
    sma_period    : SMA / Bollinger period (default 20)
    ema_period    : EMA period (default 20)
    rsi_period    : RSI period (default 14)
    bb_period     : Bollinger Bands period (default 20)
    bb_std        : Bollinger Bands standard-deviation multiplier (default 2.0)
    glob_pattern  : file filter inside input_folder (default "*.csv")
    """
    input_path  = Path(input_folder)
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
#make input folder if it doesn't exist, to avoid confusion on first run
    if not input_path.exists():
        input_path.mkdir(parents=True, exist_ok=True)
        log.info("Created input folder '%s'. Please add CSV files and run again.", input_folder)
    csv_files = sorted(input_path.glob(glob_pattern))
    if not csv_files:
        latest_output = sorted(Path("data").glob("output-*"), reverse=True)
        log.info("list of output folders: %s", [str(p) for p in latest_output])
        if latest_output:
            latest_folder = latest_output[0]
            print(f"Copying template files from '{latest_folder}' to '{input_folder}'...")
            for file in latest_folder.glob("*.csv"):
                dest = input_path / file.name
                if not dest.exists():
                    dest.write_bytes(file.read_bytes())
            log.info("Copied template files from '%s' to '%s'", latest_folder, input_folder)   
        log.warning("No files matched '%s' in '%s'", glob_pattern, input_folder)
        #copy latest output-dd-mm-yyyy-hh-mm folder to input_folder, to provide a template for users
        latest_output = sorted(Path("data").glob("output-*"), reverse=True)
        if latest_output:
            latest_folder = latest_output[0]
            print(f"Copying template files from '{latest_folder}' to '{input_folder}'...")
            for file in latest_folder.glob("*.csv"):
                dest = input_path / file.name
                if not dest.exists():
                    dest.write_bytes(file.read_bytes())
            log.info("Copied template files from '%s' to '%s'", latest_folder, input_folder)    


    log.info("Found %d file(s) in '%s'", len(csv_files), input_folder)

    ok_count  = 0
    err_count = 0

    for csv_file in csv_files:
        try:
            log.info("Processing  →  %s", csv_file.name)

            # ── load ─────────────────────────────────────────────────────────
            df = pd.read_csv(csv_file)

            # normalise column names (strip spaces, lowercase)
            df.columns = [c.strip().lower() for c in df.columns]

            required = {"open", "high", "low", "close", "volume"}
            missing  = required - set(df.columns)
            if missing:
                log.error("  Skipping – missing columns: %s", missing)
                err_count += 1
                continue

            # sort by datetime if present
            if "datetime_ist" in df.columns:
                df["datetime_ist"] = pd.to_datetime(df["datetime_ist"])
                df = df.sort_values("datetime_ist").reset_index(drop=True)

            # ── enrich ───────────────────────────────────────────────────────
            df_enriched = enrich_dataframe(
                df,
                atr_period = atr_period,
                sma_period = sma_period,
                ema_period = ema_period,
                rsi_period = rsi_period,
                bb_period  = bb_period,
                bb_std     = bb_std,
            )

            # ── save ─────────────────────────────────────────────────────────
            out_file = output_path / csv_file.name
            df_enriched.to_csv(out_file, index=False)
            log.info("  Saved  →  %s  (%d rows, %d cols)",
                     out_file.name, len(df_enriched), len(df_enriched.columns))
            ok_count += 1

        except Exception as exc:
            log.error("  Error processing '%s': %s", csv_file.name, exc)
            err_count += 1

    log.info("Done.  Success: %d  |  Errors: %d", ok_count, err_count)
