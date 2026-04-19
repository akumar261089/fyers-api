"""
pattern_detector.py
--------------------
Scans enriched CSV files (output of technical_indicators.py) for
candlestick patterns and produces a consolidated DataFrame report.

Pattern: "NR-Inside-Expansion" (3-candle squeeze-and-expand)
═══════════════════════════════════════════════════════════════
Fires when THREE consecutive candles satisfy ALL conditions below.

  Condition 1 – Volatility shape (contraction then expansion)
  ─────────────────────────────────────────────────────────────
      C1 : true_range > atr_14   → large / above-average candle
      C2 : true_range < atr_14   → tight / inside / NR candle
      C3 : true_range > atr_14   → large / above-average candle again

      ATR is read from C3's row (the most current value at signal time).

  Condition 2 – Body-size cascade
  ─────────────────────────────────────────────────────────────
      body(C1) > multiplier × body(C2)   (C1 at least 2× bigger than C2)
      body(C3) > multiplier × body(C1)   (C3 at least 2× bigger than C1)

      body = abs(close - open) for each candle.
      Zero-body doji candles for C2 are skipped (division guard).

  Condition 3 – C3 body clears C2 body completely (no shadow-over)
  ─────────────────────────────────────────────────────────────────
      Define C2 body zone:
          c2_top = max(C2.open, C2.close)
          c2_bot = min(C2.open, C2.close)

      C3 must satisfy ONE of:
          BREAKOUT UP   : C3.open > c2_top  AND  C3.close > c2_top
          BREAKOUT DOWN : C3.open < c2_bot  AND  C3.close < c2_bot

      Any overlap (C3 body straddling or inside C2 body) is rejected.

      Example (from spec):
          C2 open=100, C2 close=103  →  c2_bot=100, c2_top=103
          Valid UP   : C3 open=104, close=107  (both > 103) ✓
          Valid DOWN : C3 open=99,  close=96   (both < 100) ✓
          Rejected   : C3 open=101, close=108  (open inside zone) ✗

Pattern types
─────────────
  breakout_up    C3 body sits entirely above C2 body
  breakout_down  C3 body sits entirely below C2 body

Output columns
──────────────
  symbol           ticker / symbol value from the source CSV
  pattern_type     "breakout_up" | "breakout_down"
  c1_datetime      datetime of candle 1
  c2_datetime      datetime of candle 2
  c3_datetime      datetime of candle 3  (signal / trigger candle)
  c1_open          open  price of C1
  c1_close         close price of C1
  c1_body          abs(C1.close - C1.open)
  c1_tr            true range of C1
  c2_open          open  price of C2
  c2_close         close price of C2
  c2_body          abs(C2.close - C2.open)
  c2_tr            true range of C2
  c2_top           max(C2.open, C2.close)  – upper body edge
  c2_bot           min(C2.open, C2.close)  – lower body edge
  c3_open          open  price of C3
  c3_close         close price of C3
  c3_body          abs(C3.close - C3.open)
  c3_tr            true range of C3
  atr_at_signal    ATR value on C3's row
  b1_vs_b2_x       body(C1) / body(C2)  – size ratio for C1 over C2
  b3_vs_b1_x       body(C3) / body(C1)  – size ratio for C3 over C1
  source_file      filename the match came from

Usage (from main.py)
────────────────────
    from pattern_detector import scan_patterns

    report_df = scan_patterns(
        enriched_folder = "data/enriched",
        atr_col         = "atr_14",   # must match the column in your CSVs
        body_multiplier = 2.0,        # threshold for Condition 2
    )
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _classify_c3_vs_c2(
    c2_open: float, c2_close: float,
    c3_open: float, c3_close: float,
) -> str | None:
    """
    Condition 3 check.

    Returns
    -------
    "breakout_up"   – C3 body entirely above C2 body
    "breakout_down" – C3 body entirely below C2 body
    None            – C3 body overlaps C2 body (pattern rejected)
    """
    c2_top = max(c2_open, c2_close)
    c2_bot = min(c2_open, c2_close)

    c3_body_top = max(c3_open, c3_close)
    c3_body_bot = min(c3_open, c3_close)

    if c3_body_bot > c2_top:        # entire C3 body is above C2 body
        return "breakout_up"
    if c3_body_top < c2_bot:        # entire C3 body is below C2 body
        return "breakout_down"
    return None                     # overlap – reject


def _dt(row: pd.Series) -> str:
    """Return datetime string for a candle row."""
    v = row.get("datetime_ist")
    return str(v) if v is not None else str(row.name)


# ─────────────────────────────────────────────────────────────────────────────
#  Core scanner – single enriched DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def _scan_file(
    df: pd.DataFrame,
    source_name: str,
    atr_col: str,
    body_multiplier: float,
) -> list[dict]:

    required = {"open", "high", "low", "close", "true_range", atr_col, "candle_body"}
    missing  = required - set(df.columns)
    if missing:
        log.warning("  [%s] Missing columns %s – skipped.", source_name, missing)
        return []

    df = df.reset_index(drop=True)
    records = []

    LOOKBACK = 20   # for white space
    STRONG_TR_MULTIPLIER = 1.2

    for i in range(len(df) - 2):

        if i < LOOKBACK:
            continue

        c1 = df.iloc[i]
        c2 = df.iloc[i + 1]
        c3 = df.iloc[i + 2]

        # ─────────────────────────────────────────────
        # ATR per candle (FIXED)
        # ─────────────────────────────────────────────
        atr1 = float(c1[atr_col])
        atr2 = float(c2[atr_col])
        atr3 = float(c3[atr_col])

        if pd.isna(atr1) or pd.isna(atr2) or pd.isna(atr3):
            continue

        # ─────────────────────────────────────────────
        # Condition 1 – Volatility Shape (Improved)
        # ─────────────────────────────────────────────
        if not (
            c1["true_range"] > atr1 and
            c2["true_range"] < atr2 and
            c3["true_range"] > 1.2 * atr3
        ):
            continue

        # Strong breakout filter (NEW)
        if c3["true_range"] < STRONG_TR_MULTIPLIER * atr3:
            continue
        # WHITE AREA (C2 must be clean)
        upper_shadow = c2["high"] - max(c2["open"], c2["close"])
        lower_shadow = min(c2["open"], c2["close"]) - c2["low"]

        body = c2["candle_body"]

        if body == 0:
            shadow_ratio = 0   # or np.nan
        else:
            shadow_ratio = (upper_shadow + lower_shadow) / body

        if shadow_ratio > 0.5:
            continue
        # ─────────────────────────────────────────────
        # Condition 2 – Body Cascade
        # ─────────────────────────────────────────────
        b1 = float(c1["candle_body"])
        b2 = float(c2["candle_body"])
        b3 = float(c3["candle_body"])

        if b2 == 0:
            continue

        if not (b1 > body_multiplier * b2 and b3 > body_multiplier * b1):
            continue

        # ─────────────────────────────────────────────
        # Condition 3 – Clean Breakout
        # ─────────────────────────────────────────────
        ptype = _classify_c3_vs_c2(
            c2_open  = float(c2["open"]),
            c2_close = float(c2["close"]),
            c3_open  = float(c3["open"]),
            c3_close = float(c3["close"]),
        )

        if ptype is None:
            continue

        # ─────────────────────────────────────────────
        # Direction Confirmation (NEW)
        # ─────────────────────────────────────────────
        c1_bull = c1["close"] > c1["open"]
        c3_bull = c3["close"] > c3["open"]

        if ptype == "breakout_up" and not (c1_bull and c3_bull):
            continue

        if ptype == "breakout_down" and not ((not c1_bull) and (not c3_bull)):
            continue

        # ─────────────────────────────────────────────
        # WHITE SPACE (CRITICAL EDGE)
        # ─────────────────────────────────────────────
        recent_data = df.iloc[i-LOOKBACK:i]

        recent_high = recent_data["high"].max()
        recent_low  = recent_data["low"].min()

        white_space_score = 0

        if ptype == "breakout_up":
            if c3["close"] <= recent_high:
                continue
            white_space_score = (c3["close"] - recent_high) / atr3

        if ptype == "breakout_down":
            if c3["close"] >= recent_low:
                continue
            white_space_score = (recent_low - c3["close"]) / atr3

        if white_space_score < 0.5:
            continue

        # ─────────────────────────────────────────────
        # SCORING SYSTEM (NEW - VERY POWERFUL)
        # ─────────────────────────────────────────────
        score = 0

        # strong breakout
        if c3["true_range"] > 1.5 * atr3:
            score += 2
        else:
            score += 1

        # body strength
        if b3 > 2.5 * b1:
            score += 2
        else:
            score += 1

        # white space strength
        if white_space_score > 1:
            score += 2
        else:
            score += 1

        strength = "weak"
        if score >= 5:
            strength = "strong"
        elif score >= 4:
            strength = "moderate"

        # ─────────────────────────────────────────────
        # RECORDco
        # ─────────────────────────────────────────────
        c2_top = max(float(c2["open"]), float(c2["close"]))
        c2_bot = min(float(c2["open"]), float(c2["close"]))

        records.append({
            "symbol": c1.get("symbol", source_name),
            "pattern_type": ptype,
            "strength": strength,
            "score": score,

            "c1_datetime": _dt(c1),
            "c2_datetime": _dt(c2),
            "c3_datetime": _dt(c3),

            "c1_body": round(b1, 2),
            "c2_body": round(b2, 2),
            "c3_body": round(b3, 2),

            "c3_tr": round(float(c3["true_range"]), 2),
            "atr_at_signal": round(atr3, 4),

            "white_space_score": round(white_space_score, 2),

            "b1_vs_b2_x": round(b1 / b2, 2),
            "b3_vs_b1_x": round(b3 / b1, 2),

            "source_file": source_name,
            "zone_low": round(float(c2["low"]), 2),
            "zone_high": round(float(c2["high"]), 2),
        })

    return records

# ─────────────────────────────────────────────────────────────────────────────
#  Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def scan_patterns(
    enriched_folder: str,
    atr_col:         str   = "atr_14",
    body_multiplier: float = 2.0,
    glob_pattern:    str   = "*.csv",
) -> pd.DataFrame:
    """
    Scan every enriched CSV in *enriched_folder* and return a consolidated
    DataFrame of all pattern occurrences.

    Parameters
    ----------
    enriched_folder  : folder produced by technical_indicators.add_indicators_to_folder
    atr_col          : ATR column name to use (default "atr_14")
    body_multiplier  : size-cascade multiplier for Condition 2 (default 2.0)
    glob_pattern     : file filter inside the folder (default "*.csv")

    Returns
    -------
    pd.DataFrame  – one row per pattern hit, sorted by c3_datetime.
                    Empty DataFrame if no patterns are found.
    """
    folder = Path(enriched_folder)
    files  = sorted(folder.glob(glob_pattern))

    if not files:
        log.warning("No files matched '%s' in '%s'", glob_pattern, enriched_folder)
        return pd.DataFrame()

    log.info("Scanning %d enriched file(s) for patterns …", len(files))

    all_records: list[dict] = []

    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip().lower() for c in df.columns]

            if "datetime_ist" in df.columns:
                df["datetime_ist"] = pd.to_datetime(df["datetime_ist"])
                df = df.sort_values("datetime_ist").reset_index(drop=True)

            hits = _scan_file(df, f.name, atr_col, body_multiplier)

            if hits:
                log.info("  %-30s  →  %d pattern(s) found", f.name, len(hits))
            else:
                log.info("  %-30s  →  no patterns", f.name)

            all_records.extend(hits)

        except Exception as exc:
            log.error("  Error reading '%s': %s", f.name, exc)

    if not all_records:
        log.info("No patterns found across all files.")
        return pd.DataFrame()

    report = (
        pd.DataFrame(all_records)
        .sort_values(["c3_datetime", "symbol"])
        .reset_index(drop=True)
    )

    n_up   = (report["pattern_type"] == "breakout_up").sum()
    n_down = (report["pattern_type"] == "breakout_down").sum()
    log.info(
        "Total patterns found: %d  (%d breakout_up  |  %d breakout_down)",
        len(report), n_up, n_down,
    )

    return report
