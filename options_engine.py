"""
SKILL: options-engine
Black-Scholes Greeks calculator + options strategy recommender.
Used by the Options tab in StockVision.
"""
import math
import logging
from datetime import datetime, date
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm

logger = logging.getLogger(__name__)

# ── Risk-free rate (approximate — update periodically) ────────────────────────
RISK_FREE_RATE = 0.045  # 4.5% — US 3-month T-bill approx

# ── Expiry buckets ────────────────────────────────────────────────────────────
EXPIRY_BUCKETS = {
    "60-90d":  (60,  90),
    "LEAPS":   (180, 730),
}

# ── Strategy map ─────────────────────────────────────────────────────────────
# alert → list of (strategy_name, option_type, position)
STRATEGY_MAP = {
    "BUY":     ["Long Call", "Bull Call Spread"],
    "CAUTION": ["Covered Call", "Bull Call Spread"],
    "WATCH":   ["Covered Call", "Cash-Secured Put"],
    "NEUTRAL": ["Cash-Secured Put", "Covered Call"],
}
BEARISH_STRATEGIES = ["Long Put", "Bear Put Spread", "Protective Put"]


# ─────────────────────────────────────────────────────────────────────────────
# BLACK-SCHOLES
# ─────────────────────────────────────────────────────────────────────────────

def _d1_d2(S: float, K: float, T: float, r: float, sigma: float):
    """Return d1, d2 for Black-Scholes. T in years."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return None, None
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def black_scholes_price(S: float, K: float, T: float, r: float, sigma: float, opt_type: str = "call") -> Optional[float]:
    d1, d2 = _d1_d2(S, K, T, r, sigma)
    if d1 is None:
        return None
    if opt_type == "call":
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def calc_greeks(S: float, K: float, T: float, r: float, sigma: float, opt_type: str = "call") -> dict:
    """Return Delta, Gamma, Theta, Vega, Rho and theoretical price."""
    d1, d2 = _d1_d2(S, K, T, r, sigma)
    if d1 is None:
        return {}

    pdf_d1 = norm.pdf(d1)
    sqrt_T  = math.sqrt(T)

    if opt_type == "call":
        delta = norm.cdf(d1)
        theta = (
            -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            - r * K * math.exp(-r * T) * norm.cdf(d2)
        ) / 365
        rho = K * T * math.exp(-r * T) * norm.cdf(d2) / 100
    else:
        delta = norm.cdf(d1) - 1
        theta = (
            -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            + r * K * math.exp(-r * T) * norm.cdf(-d2)
        ) / 365
        rho = -K * T * math.exp(-r * T) * norm.cdf(-d2) / 100

    gamma = pdf_d1 / (S * sigma * sqrt_T)
    vega  = S * pdf_d1 * sqrt_T / 100

    price = black_scholes_price(S, K, T, r, sigma, opt_type)

    return {
        "delta":  round(delta, 4),
        "gamma":  round(gamma, 6),
        "theta":  round(theta, 4),
        "vega":   round(vega,  4),
        "rho":    round(rho,   4),
        "bs_price": round(price, 2) if price else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# IV RANK
# ─────────────────────────────────────────────────────────────────────────────

def calc_iv_rank(hist_iv: list[float], current_iv: float) -> Optional[float]:
    """IV Rank: where current IV sits within the past year's IV range. 0–100."""
    if not hist_iv or current_iv is None:
        return None
    iv_min = min(hist_iv)
    iv_max = max(hist_iv)
    if iv_max == iv_min:
        return 50.0
    return round((current_iv - iv_min) / (iv_max - iv_min) * 100, 1)


# ─────────────────────────────────────────────────────────────────────────────
# FETCH OPTION CHAIN
# ─────────────────────────────────────────────────────────────────────────────

def _days_to_expiry(exp_str: str) -> int:
    exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
    return (exp_date - date.today()).days


def _filter_expirations(all_exps: tuple, buckets: list[str]) -> list[str]:
    """Return expirations that fall within the requested DTE buckets."""
    result = []
    for exp in all_exps:
        dte = _days_to_expiry(exp)
        for bucket in buckets:
            lo, hi = EXPIRY_BUCKETS[bucket]
            if lo <= dte <= hi:
                result.append(exp)
                break
    return result


def fetch_option_chain(ticker: str, buckets: list[str]) -> dict:
    """
    Fetch option chain for ticker filtered by expiry buckets.
    Returns:
      {
        "spot": float,
        "iv_current": float,       # ATM IV from nearest expiry
        "iv_rank": float | None,
        "expirations": [
          {
            "expiry": "2025-06-20",
            "dte": 85,
            "calls": DataFrame,
            "puts":  DataFrame,
          }, ...
        ]
      }
    """
    try:
        tkr = yf.Ticker(ticker)
        info = tkr.fast_info
        spot = float(info.last_price) if info.last_price else None
        if not spot:
            hist = tkr.history(period="2d")
            if hist.empty:
                return {"error": f"No price data for {ticker}"}
            spot = float(hist["Close"].iloc[-1])

        all_exps = tkr.options
        if not all_exps:
            return {"error": f"No options available for {ticker}"}

        filtered_exps = _filter_expirations(all_exps, buckets)
        if not filtered_exps:
            return {"error": f"No expirations found in selected buckets for {ticker}"}

        # Estimate current IV from nearest expiry ATM options
        nearest_exp = all_exps[0]
        try:
            chain_near = tkr.option_chain(nearest_exp)
            calls_near = chain_near.calls
            atm_calls = calls_near.iloc[(calls_near["strike"] - spot).abs().argsort()[:3]]
            iv_current = float(atm_calls["impliedVolatility"].mean())
        except Exception:
            iv_current = None

        # Estimate IV rank from historical volatility proxy
        iv_rank = None
        try:
            hist_1y = tkr.history(period="1y")
            if len(hist_1y) > 20:
                log_ret = np.log(hist_1y["Close"] / hist_1y["Close"].shift(1)).dropna()
                # Rolling 30-day realized vol as IV proxy
                rolling_vol = log_ret.rolling(30).std() * math.sqrt(252)
                hist_iv_list = rolling_vol.dropna().tolist()
                if iv_current:
                    iv_rank = calc_iv_rank(hist_iv_list, iv_current)
        except Exception:
            pass

        # Fetch chains for filtered expirations
        expirations = []
        for exp in filtered_exps:
            try:
                chain = tkr.option_chain(exp)
                calls = chain.calls.copy()
                puts  = chain.puts.copy()

                # Keep only near-ATM strikes (±20% from spot)
                for df in [calls, puts]:
                    df.drop(columns=[c for c in df.columns if c not in [
                        "strike", "lastPrice", "bid", "ask", "volume",
                        "openInterest", "impliedVolatility", "inTheMoney"
                    ]], inplace=True, errors="ignore")

                atm_range = (spot * 0.80, spot * 1.20)
                calls = calls[calls["strike"].between(*atm_range)].reset_index(drop=True)
                puts  = puts[puts["strike"].between(*atm_range)].reset_index(drop=True)

                expirations.append({
                    "expiry": exp,
                    "dte":    _days_to_expiry(exp),
                    "calls":  calls,
                    "puts":   puts,
                })
            except Exception as e:
                logger.warning(f"[{ticker}] Could not fetch chain for {exp}: {e}")
                continue

        return {
            "spot":        spot,
            "iv_current":  round(iv_current * 100, 1) if iv_current else None,
            "iv_rank":     iv_rank,
            "expirations": expirations,
        }

    except Exception as e:
        logger.error(f"fetch_option_chain [{ticker}]: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY RECOMMENDER
# ─────────────────────────────────────────────────────────────────────────────

def recommend_strategy(alert: str, iv_rank: Optional[float]) -> tuple[str, str]:
    """
    Return (primary_strategy, rationale) based on alert + IV Rank.
    High IV Rank (>50) → favor premium selling strategies.
    Low IV Rank (<50)  → favor premium buying strategies.
    """
    alert = (alert or "NEUTRAL").upper()
    high_iv = iv_rank is not None and iv_rank > 50

    # Bearish signals always → bearish strategies regardless of IV
    if alert in ("BEARISH", "SELL"):
        strat = "Bear Put Spread" if high_iv else "Long Put"
        rationale = (
            "High IV — spread limits premium cost" if high_iv
            else "Low IV — cheap long put premium"
        )
        return strat, rationale

    candidates = STRATEGY_MAP.get(alert, STRATEGY_MAP["NEUTRAL"])

    if high_iv:
        # Prefer selling strategies
        selling = [s for s in candidates if s in ("Covered Call", "Cash-Secured Put", "Bear Put Spread")]
        strat = selling[0] if selling else candidates[0]
        rationale = f"IV Rank {iv_rank:.0f} — elevated IV favors premium selling"
    else:
        # Prefer buying strategies
        buying = [s for s in candidates if s in ("Long Call", "Long Put", "Bull Call Spread")]
        strat = buying[0] if buying else candidates[0]
        rationale = f"IV Rank {iv_rank:.0f} — low IV favors buying premium" if iv_rank is not None else "IV Rank unavailable — defaulting to directional buy"

    return strat, rationale


def strategy_risk_profile(strategy: str, spot: float, premium: float) -> dict:
    """Return max_profit, max_risk, breakeven for common strategies."""
    if strategy == "Long Call":
        return {
            "max_profit": "Unlimited",
            "max_risk":   f"${premium:.2f} (premium paid)",
            "breakeven":  f"Strike + ${premium:.2f}",
        }
    elif strategy == "Long Put":
        return {
            "max_profit": f"Strike - ${premium:.2f}",
            "max_risk":   f"${premium:.2f} (premium paid)",
            "breakeven":  f"Strike - ${premium:.2f}",
        }
    elif strategy == "Covered Call":
        return {
            "max_profit": f"${premium:.2f} premium collected",
            "max_risk":   f"Stock drops to $0 (offset by premium)",
            "breakeven":  f"Cost basis - ${premium:.2f}",
        }
    elif strategy == "Cash-Secured Put":
        return {
            "max_profit": f"${premium:.2f} premium collected",
            "max_risk":   f"Strike - ${premium:.2f} (if assigned)",
            "breakeven":  f"Strike - ${premium:.2f}",
        }
    elif strategy == "Bull Call Spread":
        return {
            "max_profit": f"Spread width - net debit",
            "max_risk":   f"Net debit paid",
            "breakeven":  f"Lower strike + net debit",
        }
    elif strategy == "Bear Put Spread":
        return {
            "max_profit": f"Spread width - net debit",
            "max_risk":   f"Net debit paid",
            "breakeven":  f"Upper strike - net debit",
        }
    elif strategy == "Protective Put":
        return {
            "max_profit": "Unlimited (stock upside)",
            "max_risk":   f"${premium:.2f} + (spot - strike) if any",
            "breakeven":  f"Spot + ${premium:.2f}",
        }
    return {"max_profit": "—", "max_risk": "—", "breakeven": "—"}


# ─────────────────────────────────────────────────────────────────────────────
# BEST CONTRACT PICKER
# ─────────────────────────────────────────────────────────────────────────────

def pick_best_contracts(chain_data: dict, strategy: str, n: int = 3) -> list[dict]:
    """
    From all expiry chains, pick the N best contracts for the strategy.
    Scoring criteria:
      - Delta ~0.35–0.50 for directional buys (sweet spot)
      - High open interest (liquidity)
      - Tight bid/ask spread
    Returns list of contract dicts with Greeks computed.
    """
    spot      = chain_data["spot"]
    iv_curr   = (chain_data["iv_current"] or 30) / 100  # fallback 30%
    candidates = []

    opt_type = "call" if strategy in ("Long Call", "Covered Call", "Bull Call Spread") else "put"

    for exp_data in chain_data.get("expirations", []):
        expiry = exp_data["expiry"]
        dte    = exp_data["dte"]
        T      = dte / 365.0

        df = exp_data["calls"] if opt_type == "call" else exp_data["puts"]
        if df is None or df.empty:
            continue

        for _, row in df.iterrows():
            strike = float(row["strike"])
            iv     = float(row.get("impliedVolatility", iv_curr))
            if iv <= 0 or iv > 5:
                iv = iv_curr

            greeks = calc_greeks(spot, strike, T, RISK_FREE_RATE, iv, opt_type)
            if not greeks:
                continue

            bid = float(row.get("bid", 0) or 0)
            ask = float(row.get("ask", 0) or 0)
            mid = (bid + ask) / 2 if ask > 0 else float(row.get("lastPrice", 0) or 0)
            oi  = int(row.get("openInterest", 0) or 0)
            vol = int(row.get("volume", 0) or 0)
            spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 999

            delta = abs(greeks["delta"])

            # Score: reward 0.30–0.50 delta, high OI, tight spread
            delta_score = max(0, 1 - abs(delta - 0.40) * 5)
            oi_score    = min(oi / 5000, 1.0)
            spread_score = max(0, 1 - spread_pct / 20)
            score = delta_score * 0.5 + oi_score * 0.3 + spread_score * 0.2

            candidates.append({
                "expiry":   expiry,
                "dte":      dte,
                "strike":   strike,
                "type":     opt_type.upper(),
                "bid":      round(bid, 2),
                "ask":      round(ask, 2),
                "mid":      round(mid, 2),
                "iv_pct":   round(iv * 100, 1),
                "oi":       oi,
                "volume":   vol,
                "spread_pct": round(spread_pct, 1),
                "delta":    greeks["delta"],
                "gamma":    greeks["gamma"],
                "theta":    greeks["theta"],
                "vega":     greeks["vega"],
                "bs_price": greeks["bs_price"],
                "_score":   score,
            })

    candidates.sort(key=lambda x: -x["_score"])
    for c in candidates:
        c.pop("_score", None)
    return candidates[:n]


# ─────────────────────────────────────────────────────────────────────────────
# FULL ANALYSIS — entry point called from Streamlit
# ─────────────────────────────────────────────────────────────────────────────

def analyze_ticker_options(
    ticker: str,
    alert: str,
    buckets: list[str],
) -> dict:
    """
    Full options analysis for a single ticker.
    Returns a dict ready for display in Streamlit.
    """
    chain_data = fetch_option_chain(ticker, buckets)

    if "error" in chain_data:
        return {
            "ticker":   ticker,
            "alert":    alert,
            "error":    chain_data["error"],
        }

    spot     = chain_data["spot"]
    iv_curr  = chain_data["iv_current"]
    iv_rank  = chain_data["iv_rank"]

    strategy, rationale = recommend_strategy(alert, iv_rank)
    contracts = pick_best_contracts(chain_data, strategy, n=3)

    # Risk profile uses first contract premium as reference
    premium = contracts[0]["mid"] if contracts else 0
    risk_profile = strategy_risk_profile(strategy, spot, premium)

    return {
        "ticker":       ticker,
        "alert":        alert,
        "spot":         spot,
        "iv_current":   iv_curr,
        "iv_rank":      iv_rank,
        "strategy":     strategy,
        "rationale":    rationale,
        "risk_profile": risk_profile,
        "contracts":    contracts,
        "error":        None,
    }
