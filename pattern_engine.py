"""
SKILL: pattern-engine
Cup & Handle and Head & Shoulders pattern detection.
Used by the Patterns tab in StockVision.
"""
import logging
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# SWING POINT DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def find_swings(close: pd.Series, order: int = 5):
    """
    Find swing highs and lows using local extrema.
    order: number of candles on each side to qualify as swing point.
    Returns (highs_idx, lows_idx) as arrays of integer positions.
    """
    arr = close.values
    highs = argrelextrema(arr, np.greater_equal, order=order)[0]
    lows  = argrelextrema(arr, np.less_equal,    order=order)[0]
    return highs, lows


# ─────────────────────────────────────────────────────────────────────────────
# CUP & HANDLE DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def detect_cup_and_handle(df: pd.DataFrame, lookback: int = 252) -> dict | None:
    """
    Detect Cup & Handle pattern.

    Cup criteria:
    - Left rim high → deep low → right rim high (within 15% of left rim)
    - Cup depth: 15–50% from rim to bottom
    - Cup width: 30–150 candles
    - Right rim within 5% of left rim height

    Handle criteria:
    - Pullback after right rim: 5–20% of cup depth
    - Handle duration: 5–30 candles
    - Handle stays above cup midpoint

    Returns dict with pattern details or None.
    """
    close = df["close"].iloc[-lookback:].reset_index(drop=True)
    n     = len(close)

    if n < 60:
        return None

    highs_idx, lows_idx = find_swings(close, order=7)

    best = None
    best_score = 0

    # Try all combinations of left_rim → bottom → right_rim
    for li in range(len(highs_idx) - 1):
        left_rim_i   = highs_idx[li]
        left_rim_val = close.iloc[left_rim_i]

        for ri in range(li + 1, len(highs_idx)):
            right_rim_i   = highs_idx[ri]
            right_rim_val = close.iloc[right_rim_i]

            cup_width = right_rim_i - left_rim_i
            if cup_width < 30 or cup_width > 150:
                continue

            # Right rim within 5% of left rim
            if abs(right_rim_val - left_rim_val) / left_rim_val > 0.05:
                continue

            # Find lowest point between the two rims
            cup_segment = close.iloc[left_rim_i:right_rim_i + 1]
            bottom_i    = left_rim_i + int(cup_segment.values.argmin())
            bottom_val  = close.iloc[bottom_i]

            # Bottom must be roughly centered (between 25%–75% of cup width)
            relative_pos = (bottom_i - left_rim_i) / cup_width
            if not 0.25 <= relative_pos <= 0.75:
                continue

            # Cup depth 15–50%
            cup_depth = (left_rim_val - bottom_val) / left_rim_val
            if not 0.15 <= cup_depth <= 0.50:
                continue

            # Check cup shape — U not V
            # Left half and right half should be roughly symmetric
            left_half  = close.iloc[left_rim_i:bottom_i + 1]
            right_half = close.iloc[bottom_i:right_rim_i + 1]
            if len(left_half) < 5 or len(right_half) < 5:
                continue

            # Handle detection — candles after right rim
            handle_start = right_rim_i
            handle_end   = min(right_rim_i + 30, n - 1)
            if handle_end - handle_start < 5:
                continue

            handle_seg    = close.iloc[handle_start:handle_end + 1]
            handle_low    = handle_seg.min()
            handle_high   = handle_seg.max()

            # Handle pullback 5–25% of cup depth in price terms
            cup_depth_abs = left_rim_val - bottom_val
            pullback      = right_rim_val - handle_low
            pullback_pct  = pullback / cup_depth_abs if cup_depth_abs > 0 else 0

            if not 0.05 <= pullback_pct <= 0.35:
                continue

            # Handle must stay above cup midpoint
            cup_midpoint = bottom_val + cup_depth_abs * 0.5
            if handle_low < cup_midpoint:
                continue

            # Score: reward deep cup, tight handle, centered bottom
            score = (cup_depth * 2
                     + (1 - abs(relative_pos - 0.5) * 2)
                     + (1 - pullback_pct))

            if score > best_score:
                best_score = score
                # Price target = right rim + cup depth
                target = right_rim_val + cup_depth_abs

                # Map back to original df index
                offset = len(df) - lookback
                best = {
                    "pattern":       "Cup & Handle",
                    "direction":     "BULLISH",
                    "left_rim_i":    offset + left_rim_i,
                    "left_rim_val":  round(left_rim_val, 2),
                    "bottom_i":      offset + bottom_i,
                    "bottom_val":    round(bottom_val, 2),
                    "right_rim_i":   offset + right_rim_i,
                    "right_rim_val": round(right_rim_val, 2),
                    "handle_end_i":  offset + handle_end,
                    "handle_low":    round(handle_low, 2),
                    "cup_depth_pct": round(cup_depth * 100, 1),
                    "target":        round(target, 2),
                    "current":       round(close.iloc[-1], 2),
                    "pct_to_target": round((target - close.iloc[-1]) / close.iloc[-1] * 100, 1),
                    "confidence":    round(min(score / 3 * 100, 99), 0),
                    "score":         round(score, 3),
                }

    return best


# ─────────────────────────────────────────────────────────────────────────────
# HEAD & SHOULDERS DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def detect_head_and_shoulders(df: pd.DataFrame, lookback: int = 252, inverse: bool = False) -> dict | None:
    """
    Detect Head & Shoulders (bearish) or Inverse H&S (bullish).

    H&S criteria:
    - Three peaks: left shoulder < head > right shoulder
    - Shoulders within 8% of each other
    - Head at least 3% above shoulders
    - Neckline: line connecting two troughs between shoulder-head-shoulder
    - Neckline slope: max ±15%

    Inverse H&S: same logic on lows (bullish reversal).
    """
    close = df["close"].iloc[-lookback:].reset_index(drop=True)
    n     = len(close)

    if n < 50:
        return None

    if inverse:
        # For inverse H&S, work on negated series to find valleys as peaks
        series    = -close
        direction = "BULLISH"
        pat_name  = "Inverse H&S"
    else:
        series    = close
        direction = "BEARISH"
        pat_name  = "Head & Shoulders"

    highs_idx, lows_idx = find_swings(series, order=6)

    best = None
    best_score = 0

    # Need at least 3 highs and 2 lows between them
    for hi in range(len(highs_idx) - 2):
        ls_i   = highs_idx[hi]       # left shoulder
        head_i = highs_idx[hi + 1]   # head
        rs_i   = highs_idx[hi + 2]   # right shoulder

        ls_val   = series.iloc[ls_i]
        head_val = series.iloc[head_i]
        rs_val   = series.iloc[rs_i]

        # Head must be highest
        if not (head_val > ls_val and head_val > rs_val):
            continue

        # Shoulders within 8% of each other
        if abs(ls_val - rs_val) / max(ls_val, rs_val) > 0.08:
            continue

        # Head at least 3% above shoulders
        avg_shoulder = (ls_val + rs_val) / 2
        if (head_val - avg_shoulder) / avg_shoulder < 0.03:
            continue

        # Find troughs between shoulders
        t1_candidates = [i for i in lows_idx if ls_i < i < head_i]
        t2_candidates = [i for i in lows_idx if head_i < i < rs_i]

        if not t1_candidates or not t2_candidates:
            continue

        t1_i   = min(t1_candidates, key=lambda i: series.iloc[i])
        t2_i   = min(t2_candidates, key=lambda i: series.iloc[i])
        t1_val = series.iloc[t1_i]
        t2_val = series.iloc[t2_i]

        # Neckline slope check
        neckline_slope = (t2_val - t1_val) / (t2_i - t1_i) if t2_i != t1_i else 0
        neckline_range = abs(t2_val - t1_val) / max(t1_val, t2_val)
        if neckline_range > 0.15:
            continue

        # Pattern width
        width = rs_i - ls_i
        if width < 20 or width > 180:
            continue

        # Neckline value at right shoulder
        neckline_at_rs = t1_val + neckline_slope * (rs_i - t1_i)

        # Head height above neckline
        neckline_at_head = t1_val + neckline_slope * (head_i - t1_i)
        head_height = head_val - neckline_at_head

        if head_height <= 0:
            continue

        # Price target = neckline - head_height (H&S) or neckline + head_height (IH&S)
        current_neckline = t1_val + neckline_slope * (n - 1 - t1_i)

        if inverse:
            target_series = -current_neckline + head_height
            target        = -target_series
            current_close = close.iloc[-1]
        else:
            target_series = current_neckline - head_height
            target        = target_series
            current_close = close.iloc[-1]

        # Symmetry score
        sym_score = 1 - abs(ls_val - rs_val) / avg_shoulder
        hgt_score = min((head_val - avg_shoulder) / avg_shoulder, 0.20) / 0.20
        score     = sym_score + hgt_score

        if score > best_score:
            best_score = score
            offset = len(df) - lookback

            if inverse:
                ls_val_real   = close.iloc[ls_i]
                head_val_real = close.iloc[head_i]
                rs_val_real   = close.iloc[rs_i]
                t1_val_real   = close.iloc[t1_i]
                t2_val_real   = close.iloc[t2_i]
                nl_at_rs_real = close.iloc[t1_i] - neckline_slope * (t2_i - t1_i)
            else:
                ls_val_real   = close.iloc[ls_i]
                head_val_real = close.iloc[head_i]
                rs_val_real   = close.iloc[rs_i]
                t1_val_real   = close.iloc[t1_i]
                t2_val_real   = close.iloc[t2_i]
                nl_at_rs_real = float(neckline_at_rs)

            pct_to_target = (target - current_close) / current_close * 100

            best = {
                "pattern":        pat_name,
                "direction":      direction,
                "ls_i":           offset + ls_i,
                "ls_val":         round(ls_val_real, 2),
                "head_i":         offset + head_i,
                "head_val":       round(head_val_real, 2),
                "rs_i":           offset + rs_i,
                "rs_val":         round(rs_val_real, 2),
                "t1_i":           offset + t1_i,
                "t1_val":         round(t1_val_real, 2),
                "t2_i":           offset + t2_i,
                "t2_val":         round(t2_val_real, 2),
                "neckline_at_rs": round(nl_at_rs_real, 2),
                "head_height":    round(abs(head_height), 2),
                "target":         round(target, 2),
                "current":        round(current_close, 2),
                "pct_to_target":  round(pct_to_target, 1),
                "confidence":     round(min(score / 2 * 100, 99), 0),
                "score":          round(score, 3),
            }

    return best


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def detect_patterns(df: pd.DataFrame, lookback: int = 252) -> dict:
    """
    Run all pattern detections on a DataFrame with OHLCV data.
    Returns best detected pattern or None.
    df must have lowercase columns: open, high, low, close, volume.
    """
    results = []

    cup = detect_cup_and_handle(df, lookback)
    if cup:
        results.append(cup)

    hs = detect_head_and_shoulders(df, lookback, inverse=False)
    if hs:
        results.append(hs)

    ihs = detect_head_and_shoulders(df, lookback, inverse=True)
    if ihs:
        results.append(ihs)

    if not results:
        return {"pattern": None}

    # Return highest confidence pattern
    best = max(results, key=lambda x: x["confidence"])
    return best
