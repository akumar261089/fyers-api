"""
pattern_detector.py
--------------------
Scans enriched CSV files (output of technical_indicators.py) for
Supply / Demand zone patterns and produces a consolidated DataFrame report.

Pattern: "NR-Inside-Expansion" (GEL – 3-candle structure)
═══════════════════════════════════════════════════════════════

  C1  =  Leg-In candle    → TR > ATR  (filled / large candle)
  C2  =  UOC candle       → TR < ATR  (boring / tight candle, dual-side wicks)
  C3  =  Leg-Out candle   → TR > ATR  (filled / large candle, strong move)

  ZONE is drawn from C2 body:
      zone_high = max(C2.open, C2.close)
      zone_low  = min(C2.open, C2.close)

  Pattern types
  ─────────────
  Supply Zone (DBD / RBD)  →  C3 is a bearish leg-out (breakout_down)
      zone = C2 body → price will RETURN to this zone to SELL

  Demand Zone (DBR / RBR)  →  C3 is a bullish leg-out (breakout_up)
      zone = C2 body → price will RETURN to this zone to BUY

  Additional rules per notes:
  - C2 (UOC) must have BOTH upper and lower wicks (dual-side wick)
  - C2 body must be significantly smaller than C1 body
  - C3 body must be significantly larger than C2 body (strong leg-out)
  - C3 must clear C2 body completely (no overlap)
  - Space (gap) between C2 and C3 is good (not required but scored)
  - NO gap should exist between C1 and C2 (they should be adjacent)

Output columns
──────────────
  symbol, pattern_type, strength, score
  c1_datetime, c2_datetime, c3_datetime
  c1_open, c1_close, c1_body, c1_tr
  c2_open, c2_close, c2_body, c2_tr
  c3_open, c3_close, c3_body, c3_tr
  zone_high  ← max(C2.open, C2.close)  ← THE ZONE TOP
  zone_low   ← min(C2.open, C2.close)  ← THE ZONE BOTTOM
  atr_at_signal, b1_vs_b2_x, b3_vs_b2_x
  source_file
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
    Condition 3 check – C3 body must fully clear C2 body.

    Returns
    -------
    "breakout_up"   – C3 body entirely above C2 body  → Demand Zone
    "breakout_down" – C3 body entirely below C2 body  → Supply Zone
    None            – C3 body overlaps C2 body (pattern rejected)
    """
    c2_top = max(c2_open, c2_close)
    c2_bot = min(c2_open, c2_close)

    c3_body_top = max(c3_open, c3_close)
    c3_body_bot = min(c3_open, c3_close)

    if c3_close > c2_top:
        return "breakout_up"
    if c3_close < c2_bot:
        return "breakout_down"
    return None                      # overlap – reject


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
    body_multiplier_second: float,
) -> list[dict]:

    required = {"open", "high", "low", "close", "true_range", atr_col, "candle_body"}
    missing  = required - set(df.columns)
    if missing:
        log.warning("  [%s] Missing columns %s – skipped.", source_name, missing)
        return []

    df = df.reset_index(drop=True)
    records = []

    # Skip until we have enough rows for ATR to be valid
    LOOKBACK = 20

    for i in range(LOOKBACK, len(df) - 2):

        c1 = df.iloc[i]
        c2 = df.iloc[i + 1]
        c3 = df.iloc[i + 2]
        # c4 = df.iloc[i + 3]
        # c5 = df.iloc[i + 4]
        # c6 = df.iloc[i + 5]


        atr1 = float(c1[atr_col])
        atr2 = float(c2[atr_col])
        atr3 = float(c3[atr_col])
        # atr4 = float(c4[atr_col])
        # atr5 = float(c5[atr_col])
        # atr6 = float(c6[atr_col])
        # leg_in = []
        # boring = []
        # leg_out =[]
        if pd.isna(atr1) or pd.isna(atr2) or pd.isna(atr3):
            continue
        # ─────────────────────────────────────────────────────
        # CONDITION 1 – TR vs ATR
        #   C1: Leg-In  → TR > ATR  (filled candle)
        #   C2: UOC     → TR < ATR  (boring / tight candle)
        #   C3: Leg-Out → TR > ATR  (filled candle)
        # ─────────────────────────────────────────────────────
        c1_tr = float(c1["true_range"])
        c2_tr = float(c2["true_range"])
        c3_tr = float(c3["true_range"])
        # c4_tr = float(c4["true_range"])
        # c5_tr = float(c5["true_range"])
        # c6_tr = float(c6["true_range"])

        # #Leg in is C1
        # if not (c1_tr > atr1):
        #     continue
        # leg_in = [c1]
        # # Boring candle
        # if not c2_tr <  atr2:
        #     continue
        # elif not c3_tr < atr3:
        #     boring = [c2]
        #     leg_out = [c3]
        # elif not c4_tr < atr4:
        #     continue
        # else:
        #     boring = [c2,c3]
        #     leg_out = [c4]

        # #legout
        # if leg_out contains c3 and c4 starts from where c3 ends in same direction
        # then leg_out =[c3,c4]
        # else leg_out = c3 only 

        # if leg_out contains c4 and c5 starts from where c4 ends in same direction
        # then leg_out =[c4,c5]
        # else leg_out = c4 only


    

        # check if c2 c3 are boring candle or only c2

        #cehck if c3 c4 c5 c6 are Legout
        # More realistic thresholds
        if not (c1_tr > atr1 and c2_tr <  atr2 and c3_tr > atr3):

            continue
        # ─────────────────────────────────────────────────────
        # CONDITION 0 – C1 should have small or no wicks
        #   Strong leg-in = mostly body, very small wicks
        # ─────────────────────────────────────────────────────

        c1_open  = float(c1["open"])
        c1_close = float(c1["close"])
        c1_high  = float(c1["high"])
        c1_low   = float(c1["low"])

        c1_upper_wick = c1_high - max(c1_open, c1_close)
        c1_lower_wick = min(c1_open, c1_close) - c1_low

        # total wick vs TR
        c1_total_wick = c1_upper_wick + c1_lower_wick

        # Allow very small wick (tunable)
        MAX_WICK_RATIO = 0.2   # 20% of TR (you can try 0.1–0.25)

        # bullish leg-in → upper wick should be tiny
        if c1_close > c1_open and c1_upper_wick > 0.2 * (c1_close-c1_open):
            continue

        # bearish leg-in → lower wick should be tiny
        if c1_close < c1_open and c1_lower_wick > 0.2 *  (c1_open-c1_close):
            continue

        # ─────────────────────────────────────────────────────
        # CONDITION 2 – UOC must have BOTH wicks (dual-side wick)
        #   Upper wick: high - max(open, close)  > 0
        #   Lower wick: min(open, close) - low   > 0
        # ─────────────────────────────────────────────────────
        upper_wick_c2 = float(c2["high"]) - max(float(c2["open"]), float(c2["close"]))
        lower_wick_c2 = min(float(c2["open"]), float(c2["close"])) - float(c2["low"])

        # Require meaningful dual wicks (not zero, not noise)
        if upper_wick_c2 < 0.05 * c2_tr and lower_wick_c2 < 0.05 * c2_tr:
            continue

        # ─────────────────────────────────────────────────────
        # CONDITION 3 – BODY SIZE CHECK
        #   b1 (Leg-In body) must be larger than b2 (UOC body)
        #   b3 (Leg-Out body) must be larger than b2 (UOC body)
        #   Both must be at least body_multiplier times bigger
        # ─────────────────────────────────────────────────────
        b1 = float(c1["candle_body"])
        b2 = float(c2["candle_body"])
        b3 = float(c3["candle_body"])

        # Skip doji C2 (zero body) to avoid divide-by-zero
        if b2 == 0:
            continue

        # C1 (leg-in) must be bigger than C2 (UOC)
        if b1 <= body_multiplier * b2:
            continue

        # C3 (leg-out) must be bigger than C1 Leg in
        if b3 <= body_multiplier_second * b1:
            continue

        # ─────────────────────────────────────────────────────
        # CONDITION 4 – C3 body must fully clear C2 body
        #   breakout_up   → Demand Zone (price will return to buy)
        #   breakout_down → Supply Zone (price will return to sell)
        # ─────────────────────────────────────────────────────
        ptype = _classify_c3_vs_c2(
            float(c2["open"]), float(c2["close"]),
            float(c3["open"]), float(c3["close"])
        )

        if ptype is None:
            continue
        # ─────────────────────────────────────────────────────
        # CONDITION 5 – GAP BETWEEN C2 AND C3 (IMPORTANT)
        # ─────────────────────────────────────────────────────

        c2_top = max(float(c2["open"]), float(c2["close"]))
        c2_bot = min(float(c2["open"]), float(c2["close"]))

        c3_high = float(c3["high"])
        c3_low  = float(c3["low"])

        if ptype == "breakout_up":
            gap = c3_low - c2_top
        else:
            gap = c2_bot - c3_high

        # normalize
        gap = max(0.0, gap)

        # minimum gap threshold (VERY IMPORTANT)
        MIN_GAP = 0.1 * atr3   # you can tune this

        if gap < MIN_GAP:
            continue

        # ─────────────────────────────────────────────────────
        # CONDITION 6 – Match candle direction with breakout type
        # ─────────────────────────────────────────────────────

        c3_open  = float(c3["open"])
        c3_close = float(c3["close"])

        # Reject doji
        if c3_close == c3_open:
            continue

        if ptype == "breakout_up" and c3_close <= c3_open:
            continue  # breakout up must be bullish

        if ptype == "breakout_down" and c3_close >= c3_open:
            continue  # breakout down must be bearish
        # ─────────────────────────────────────────────────────
        # ZONE DEFINITION
        #   The zone IS the C2 (UOC / Boring Candle) body.
        #   zone_high = max(C2.open, C2.close)
        #   zone_low  = min(C2.open, C2.close)
        #
        #   For Supply Zone: zone is where price returns to SELL
        #   For Demand Zone: zone is where price returns to BUY
        # ─────────────────────────────────────────────────────
        zone_high = max(float(c2["open"]), float(c2["close"]))
        zone_low  = min(float(c2["open"]), float(c2["close"]))

        # ─────────────────────────────────────────────────────
        # OPTIONAL: Check no gap between C1 and C2
        #   (Leg-In and UOC should be adjacent – no space)
        #   gap_c1_c2 = 0 ideally; allow small tolerance
        # ─────────────────────────────────────────────────────
        # c1_top = max(float(c1["open"]), float(c1["close"]))
        # c1_bot = min(float(c1["open"]), float(c1["close"]))
        # gap_c1_c2 = abs(float(c2["open"]) - float(c1["close"]))
        # has_c1_c2_gap = gap_c1_c2 > 0.2 * atr1

        # ─────────────────────────────────────────────────────
        # OPTIONAL: Check space (gap) between C2 and C3 bodies
        #   "Space between Boring Candle and Leg-Out is good"
        # ─────────────────────────────────────────────────────
        if ptype == "breakout_up":
            gap_c2_c3 = float(c3["low"]) - zone_high
        else:
            gap_c2_c3 = zone_low - float(c3["high"])

        gap_c2_c3 = max(0.0, gap_c2_c3)

        has_c2_c3_space = gap_c2_c3 > 0

        # ─────────────────────────────────────────────────────
        # SCORING
        # ─────────────────────────────────────────────────────
        score = 0

        # Strong leg-out
        if c3_tr > 1.5 * atr3:
            score += 2
        else:
            score += 1

        # Large body ratio C3 vs C2
        if b3 > 1.5 * b1:
            score += 2
        else:
            score += 1

        # # Space between C2 and C3 (good sign per notes)
        # if has_c2_c3_space:
        #     score += 1

        # # No gap between C1 and C2 (ideal)
        # if not has_c1_c2_gap:
        #     score += 1

        # Large C1 vs C2 ratio
        if b1 > 2.0 * b2:
            score += 1

        strength = "weak"
        if score >= 6:
            strength = "strong"
        elif score >= 4:
            strength = "moderate"

        # ─────────────────────────────────────────────────────
        # DETERMINE ZONE PATTERN SUB-TYPE
        #   Supply: DBD (Drop-Base-Drop) or RBD (Rally-Base-Drop)
        #   Demand: DBR (Drop-Base-Rally) or RBR (Rally-Base-Rally)
        # ─────────────────────────────────────────────────────
        c1_is_bearish = float(c1["close"]) < float(c1["open"])
        c1_is_bullish = float(c1["close"]) > float(c1["open"])

        if ptype == "breakout_down":
            zone_type = "Supply"
            sub_type = "DBD" if c1_is_bearish else "RBD"
        else:
            zone_type = "Demand"
            sub_type = "DBR" if c1_is_bearish else "RBR"

        pattern_label = f"{zone_type}_{sub_type}"

        # ─────────────────────────────────────────────────────
        # RECORD
        # ─────────────────────────────────────────────────────
        records.append({
            "symbol":        c1.get("symbol", source_name),
            "pattern_type":  ptype,               # "breakout_up" or "breakout_down"
            "zone_type":     zone_type,            # "Supply" or "Demand"
            "sub_type":      sub_type,             # "DBD", "RBD", "DBR", "RBR"
            "pattern_label": pattern_label,        # e.g. "Supply_DBD"
            "strength":      strength,
            "score":         score,

            "c1_datetime":   _dt(c1),
            "c2_datetime":   _dt(c2),
            "c3_datetime":   _dt(c3),

            "c1_open":       round(float(c1["open"]),  4),
            "c1_close":      round(float(c1["close"]), 4),
            "c1_body":       round(b1, 4),
            "c1_tr":         round(c1_tr, 4),

            "c2_open":       round(float(c2["open"]),  4),
            "c2_close":      round(float(c2["close"]), 4),
            "c2_body":       round(b2, 4),
            "c2_tr":         round(c2_tr, 4),
            "c2_upper_wick": round(upper_wick_c2, 4),
            "c2_lower_wick": round(lower_wick_c2, 4),

            "c3_open":       round(float(c3["open"]),  4),
            "c3_close":      round(float(c3["close"]), 4),
            "c3_body":       round(b3, 4),
            "c3_tr":         round(c3_tr, 4),

            # ── THE ZONE (from C2 body) ─────────────────────
            "zone_high":     round(zone_high, 4),   # max(C2.open, C2.close)
            "zone_low":      round(zone_low,  4),   # min(C2.open, C2.close)
            # ─────────────────────────────────────────────────

            "atr_at_signal": round(atr3, 4),

            "b1_vs_b2_x":    round(b1 / b2, 2),   # C1 body / C2 body
            "b3_vs_b2_x":    round(b3 / b2, 2),   # C3 body / C2 body

            "gap_c2_c3":     round(gap_c2_c3, 4), # space between UOC and Leg-Out

            "source_file":   source_name,
        })

    return records


# ─────────────────────────────────────────────────────────────────────────────
#  Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def scan_patterns(
    enriched_folder: str,
    atr_col:         str   = "atr_14",
    body_multiplier: float = 3,
    body_multiplier_second: float = 1.3,
    
    glob_pattern:    str   = "*.csv",
) -> pd.DataFrame:
    """
    Scan every enriched CSV in *enriched_folder* and return a consolidated
    DataFrame of all Supply/Demand zone occurrences.

    Parameters
    ----------
    enriched_folder  : folder produced by technical_indicators.add_indicators_to_folder
    atr_col          : ATR column name to use (default "atr_14")
    body_multiplier  : size multiplier for body cascade check (default 2.0)
                       C1 body > multiplier × C2 body
                       C3 body > multiplier × C2 body
    glob_pattern     : file filter inside the folder (default "*.csv")

    Returns
    -------
    pd.DataFrame  – one row per pattern hit.
                    Empty DataFrame if no patterns are found.

    Zone columns in output
    ----------------------
    zone_high  : max(C2.open, C2.close)  ← SELL/BUY zone top
    zone_low   : min(C2.open, C2.close)  ← SELL/BUY zone bottom
    zone_type  : "Supply" or "Demand"
    sub_type   : "DBD" | "RBD" | "DBR" | "RBR"
    """
    folder = Path(enriched_folder)
    files  = sorted(folder.glob(glob_pattern))

    if not files:
        log.warning("No files matched '%s' in '%s'", glob_pattern, enriched_folder)
        return pd.DataFrame()

    log.info("Scanning %d enriched file(s) for Supply/Demand zones …", len(files))

    all_records: list[dict] = []

    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip().lower() for c in df.columns]

            if "datetime_ist" in df.columns:
                df["datetime_ist"] = pd.to_datetime(df["datetime_ist"])
                df = df.sort_values("datetime_ist").reset_index(drop=True)

            hits = _scan_file(df, f.name, atr_col, body_multiplier, body_multiplier_second)

            if hits:
                log.info("  %-30s  →  %d zone(s) found", f.name, len(hits))
            else:
                log.info("  %-30s  →  no zones", f.name)

            all_records.extend(hits)

        except Exception as exc:
            log.error("  Error reading '%s': %s", f.name, exc)

    if not all_records:
        log.info("No zones found across all files.")
        return pd.DataFrame()

    report = (
        pd.DataFrame(all_records)
        .sort_values(["c3_datetime", "symbol"])
        .reset_index(drop=True)
    )

    n_supply = (report["zone_type"] == "Supply").sum()
    n_demand = (report["zone_type"] == "Demand").sum()
    log.info(
        "Total zones found: %d  (%d Supply  |  %d Demand)",
        len(report), n_supply, n_demand,
    )

    return report