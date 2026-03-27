"""
options_to_sheets.py
Reads tickers from tickers_export.txt -> fetches options data -> writes to Google Sheets.
Run on Windows via Task Scheduler.
"""
import os
import sys
import math
import logging
from datetime import datetime, date

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Config
TICKERS_FILE   = r"C:\Users\yocha\stockbot\tickers_export.txt"
CREDENTIALS    = r"C:\Users\yocha\stockbot\credentials.json"
TOKEN_FILE     = r"C:\Users\yocha\stockbot\token_sheets.json"
SHEET_TITLE    = f"Options Analysis {datetime.today().strftime('%Y-%m-%d')}"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive.file"]
RISK_FREE_RATE = 0.045
EXPIRY_BUCKETS = {"60-90d": (60, 90), "LEAPS": (180, 730)}
MAX_TICKERS    = 50
STOCK_DB_URL   = "http://192.168.1.2:8000"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(r"C:\Users\yocha\stockbot\options_sheets.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)


# Google Sheets

def get_sheets_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return build("sheets", "v4", credentials=creds)


def create_sheet(service, title):
    body = {
        "properties": {"title": title},
        "sheets": [
            {"properties": {"title": "Summary"}},
            {"properties": {"title": "Contracts"}},
        ]
    }
    resp = service.spreadsheets().create(body=body, fields="spreadsheetId").execute()
    sheet_id = resp["spreadsheetId"]
    log.info(f"Created sheet: https://docs.google.com/spreadsheets/d/{sheet_id}")
    return sheet_id


def write_to_sheet(service, sheet_id, tab, data):
    body = {"values": data}
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body=body,
    ).execute()


def format_sheet(service, sheet_id, sheet_gid, num_cols):
    requests = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": 1,
                           "startColumnIndex": 0, "endColumnIndex": num_cols},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True, "foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}},
                    "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85}
                }},
                "fields": "userEnteredFormat(textFormat,backgroundColor)"
            }
        },
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_gid, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        },
        {
            "autoResizeDimensions": {
                "dimensions": {"sheetId": sheet_gid, "dimension": "COLUMNS",
                               "startIndex": 0, "endIndex": num_cols}
            }
        },
    ]
    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id, body={"requests": requests}
    ).execute()


# TA Alert (standalone — no DB needed)

def _ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_ta_alert(ticker):
    """
    Compute BUY/WATCH/NEUTRAL/BEARISH alert from yfinance daily data.
    Logic:
      - Above EMA150 + MACD bullish + volume rising -> BUY
      - Above EMA150 + MACD bullish                 -> WATCH
      - Below EMA150 + MACD bearish                 -> BEARISH
      - Everything else                              -> NEUTRAL
    Returns (alert, score, signals_list)
    """
    try:
        hist = yf.Ticker(ticker).history(period="1y")
        if hist is None or len(hist) < 160:
            return "NEUTRAL", 0, ["insufficient data"]

        close  = hist["Close"]
        volume = hist["Volume"]

        ema150      = _ema(close, 150)
        above_ema   = close.iloc[-1] > ema150.iloc[-1]

        ema12       = _ema(close, 12)
        ema26       = _ema(close, 26)
        macd_line   = ema12 - ema26
        signal_line = _ema(macd_line, 9)
        macd_bull   = macd_line.iloc[-1] > signal_line.iloc[-1]
        macd_cross  = (macd_line.iloc[-2] <= signal_line.iloc[-2] and
                       macd_line.iloc[-1] > signal_line.iloc[-1])

        vol_avg20   = volume.iloc[-20:].mean()
        vol_rising  = volume.iloc[-1] > vol_avg20 * 1.2

        score = 0
        signals = []

        if above_ema:
            score += 10
            signals.append("above_ema150")
        else:
            score -= 10
            signals.append("below_ema150")

        if macd_bull:
            score += 10
            signals.append("macd_bullish")
        else:
            score -= 10
            signals.append("macd_bearish")

        if macd_cross:
            score += 10
            signals.append("macd_crossover")

        if vol_rising:
            score += 10
            signals.append("volume_rising")

        if score >= 30:
            alert = "BUY"
        elif score >= 10:
            alert = "WATCH"
        elif score <= -10:
            alert = "BEARISH"
        else:
            alert = "NEUTRAL"

        return alert, score, signals

    except Exception as e:
        log.warning(f"  [{ticker}] TA error: {e}")
        return "NEUTRAL", 0, ["error"]


# Black-Scholes

def _d1_d2(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return None, None
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def calc_greeks(S, K, T, r, sigma, opt_type="call"):
    d1, d2 = _d1_d2(S, K, T, r, sigma)
    if d1 is None:
        return {}
    pdf_d1 = norm.pdf(d1)
    sqrt_T = math.sqrt(T)
    if opt_type == "call":
        delta    = norm.cdf(d1)
        theta    = (-(S * pdf_d1 * sigma) / (2 * sqrt_T)
                    - r * K * math.exp(-r * T) * norm.cdf(d2)) / 365
        bs_price = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        delta    = norm.cdf(d1) - 1
        theta    = (-(S * pdf_d1 * sigma) / (2 * sqrt_T)
                    + r * K * math.exp(-r * T) * norm.cdf(-d2)) / 365
        bs_price = K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    gamma = pdf_d1 / (S * sigma * sqrt_T)
    vega  = S * pdf_d1 * sqrt_T / 100
    return {
        "delta":    round(delta, 4),
        "gamma":    round(gamma, 6),
        "theta":    round(theta, 4),
        "vega":     round(vega, 4),
        "bs_price": round(bs_price, 2),
    }


def calc_iv_rank(hist_iv, current_iv):
    if not hist_iv or not current_iv:
        return None
    iv_min, iv_max = min(hist_iv), max(hist_iv)
    if iv_max == iv_min:
        return 50.0
    return round(max(0.0, min(100.0, (current_iv - iv_min) / (iv_max - iv_min) * 100)), 1)


def _days_to_expiry(exp_str):
    return (datetime.strptime(exp_str, "%Y-%m-%d").date() - date.today()).days


def _filter_expirations(all_exps, buckets):
    result = []
    for exp in all_exps:
        dte = _days_to_expiry(exp)
        for b in buckets:
            lo, hi = EXPIRY_BUCKETS[b]
            if lo <= dte <= hi:
                result.append(exp)
                break
    return result


def recommend_strategy(alert, iv_rank):
    alert = (alert or "NEUTRAL").upper()
    high_iv = iv_rank is not None and iv_rank > 50
    STRATEGY_MAP = {
        "BUY":     ["Long Call", "Bull Call Spread"],
        "CAUTION": ["Covered Call", "Bull Call Spread"],
        "WATCH":   ["Covered Call", "Cash-Secured Put"],
        "NEUTRAL": ["Cash-Secured Put", "Covered Call"],
    }
    if alert in ("BEARISH", "SELL"):
        strat = "Bear Put Spread" if high_iv else "Long Put"
        return strat, f"Bearish + IV Rank {iv_rank or '?'}"
    candidates = STRATEGY_MAP.get(alert, STRATEGY_MAP["NEUTRAL"])
    if high_iv:
        selling = [s for s in candidates if s in ("Covered Call", "Cash-Secured Put")]
        strat = selling[0] if selling else candidates[0]
        return strat, f"IV Rank {iv_rank:.0f} - high IV, sell premium"
    buying = [s for s in candidates if s in ("Long Call", "Bull Call Spread")]
    strat = buying[0] if buying else candidates[0]
    rationale = (f"IV Rank {iv_rank:.0f} - low IV, buy premium"
                 if iv_rank else f"{strat} - IV unavailable")
    return strat, rationale


def analyze_ticker(ticker, alert="NEUTRAL"):
    try:
        tkr  = yf.Ticker(ticker)
        info = tkr.fast_info
        spot = float(info.last_price) if info.last_price else None
        if not spot:
            hist = tkr.history(period="2d")
            if hist.empty:
                return None
            spot = float(hist["Close"].iloc[-1])

        all_exps = tkr.options
        if not all_exps:
            return None

        filtered = _filter_expirations(all_exps, list(EXPIRY_BUCKETS.keys()))
        if not filtered:
            return None

        iv_current = None
        try:
            chain_near = tkr.option_chain(all_exps[0])
            atm = chain_near.calls.iloc[
                (chain_near.calls["strike"] - spot).abs().argsort()[:3]]
            iv_current = float(atm["impliedVolatility"].mean())
        except Exception:
            pass

        iv_rank = None
        try:
            hist_1y  = tkr.history(period="1y")
            log_ret  = np.log(hist_1y["Close"] / hist_1y["Close"].shift(1)).dropna()
            roll_vol = log_ret.rolling(30).std() * math.sqrt(252)
            iv_list  = roll_vol.dropna().tolist()
            if iv_current and iv_list:
                iv_rank = calc_iv_rank(iv_list, iv_current)
        except Exception:
            pass

        strategy, rationale = recommend_strategy(alert, iv_rank)
        opt_type = "call" if strategy in ("Long Call", "Covered Call", "Bull Call Spread") else "put"

        best_contracts = []
        for exp in filtered[:4]:
            dte = _days_to_expiry(exp)
            T   = dte / 365.0
            try:
                chain = tkr.option_chain(exp)
                df    = chain.calls if opt_type == "call" else chain.puts
                df    = df[df["strike"].between(spot * 0.80, spot * 1.20)]
                iv_fallback = iv_current or 0.30

                best       = None
                best_score = -1
                for _, row in df.iterrows():
                    iv  = float(row.get("impliedVolatility", iv_fallback) or iv_fallback)
                    if iv <= 0 or iv > 5:
                        iv = iv_fallback
                    bid = float(row.get("bid", 0) or 0)
                    ask = float(row.get("ask", 0) or 0)
                    mid = (bid + ask) / 2 if ask > 0 else float(row.get("lastPrice", 0) or 0)
                    if mid <= 0:
                        continue
                    oi_raw  = row.get("openInterest", 0)
                    oi      = int(oi_raw) if pd.notna(oi_raw) else 0
                    vol_raw = row.get("volume", 0)
                    vol     = int(vol_raw) if pd.notna(vol_raw) else 0
                    spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 999
                    greeks = calc_greeks(spot, float(row["strike"]), T,
                                         RISK_FREE_RATE, iv, opt_type)
                    if not greeks:
                        continue
                    delta = abs(greeks["delta"])
                    score = (max(0, 1 - abs(delta - 0.40) * 5) * 0.5
                             + min(oi / 5000, 1.0) * 0.3
                             + max(0, 1 - spread_pct / 20) * 0.2)
                    if score > best_score:
                        best_score = score
                        best = {
                            "expiry":     exp,
                            "dte":        dte,
                            "strike":     float(row["strike"]),
                            "type":       opt_type.upper(),
                            "bid":        round(bid, 2),
                            "ask":        round(ask, 2),
                            "mid":        round(mid, 2),
                            "iv_pct":     round(iv * 100, 1),
                            "oi":         oi,
                            "volume":     vol,
                            "spread_pct": round(spread_pct, 1),
                            **greeks,
                        }
                if best:
                    best_contracts.append(best)
            except Exception as e:
                log.warning(f"  [{ticker}] chain error for {exp}: {e}")
                continue

        return {
            "ticker":    ticker,
            "alert":     alert,
            "spot":      round(spot, 2),
            "iv_pct":    round(iv_current * 100, 1) if iv_current else None,
            "iv_rank":   iv_rank,
            "strategy":  strategy,
            "rationale": rationale,
            "contracts": best_contracts,
            "run_date":  datetime.today().strftime("%Y-%m-%d"),
        }

    except Exception as e:
        log.error(f"[{ticker}] analyze error: {e}")
        return None


# Main

def main():
    log.info("=" * 60)
    log.info("Options to Google Sheets - starting")
    log.info("=" * 60)

    if not os.path.exists(TICKERS_FILE):
        log.error(f"Tickers file not found: {TICKERS_FILE}")
        sys.exit(1)

    with open(TICKERS_FILE) as f:
        tickers = [line.strip().upper() for line in f
                   if line.strip() and not line.strip().startswith("#")]

    tickers = tickers[:MAX_TICKERS]
    log.info(f"Loaded {len(tickers)} tickers from {TICKERS_FILE}")

    summary_rows  = []
    contract_rows = []

    for i, ticker in enumerate(tickers):
        log.info(f"  [{i+1}/{len(tickers)}] {ticker} - computing TA...")

        # Compute TA alert directly from yfinance
        alert, ta_score, ta_signals = compute_ta_alert(ticker)
        log.info(f"    TA: {alert} (score={ta_score}, signals={ta_signals})")

        result = analyze_ticker(ticker, alert=alert)
        if not result:
            log.warning(f"  [{ticker}] skipped - no options data")
            continue

        summary_rows.append([
            result["run_date"],
            result["ticker"],
            result["alert"],
            ta_score,
            ", ".join(ta_signals),
            result["spot"],
            f"{result['iv_pct']}%" if result["iv_pct"] else "-",
            result["iv_rank"] if result["iv_rank"] is not None else "-",
            result["strategy"],
            result["rationale"],
        ])

        for c in result["contracts"]:
            contract_rows.append([
                result["run_date"],
                result["ticker"],
                result["alert"],
                result["strategy"],
                c["expiry"],
                c["dte"],
                c["strike"],
                c["type"],
                c["bid"],
                c["ask"],
                c["mid"],
                f"{c['iv_pct']}%",
                c["oi"],
                f"{c['spread_pct']}%",
                c["delta"],
                c["gamma"],
                c["theta"],
                c["vega"],
                c["bs_price"],
            ])

    if not summary_rows:
        log.error("No results - exiting")
        sys.exit(1)

    log.info("Authenticating with Google Sheets...")
    service  = get_sheets_service()
    sheet_id = create_sheet(service, SHEET_TITLE)

    summary_header = [
        "Date", "Ticker", "Alert", "TA Score", "TA Signals",
        "Spot", "IV%", "IV Rank", "Strategy", "Rationale"
    ]
    write_to_sheet(service, sheet_id, "Summary", [summary_header] + summary_rows)

    contracts_header = [
        "Date", "Ticker", "Alert", "Strategy", "Expiry", "DTE", "Strike", "Type",
        "Bid", "Ask", "Mid", "IV%", "OI", "Spread%",
        "Delta", "Gamma", "Theta", "Vega", "BS Price"
    ]
    write_to_sheet(service, sheet_id, "Contracts", [contracts_header] + contract_rows)

    sheets_meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    for sheet in sheets_meta["sheets"]:
        gid   = sheet["properties"]["sheetId"]
        title = sheet["properties"]["title"]
        cols  = len(summary_header) if title == "Summary" else len(contracts_header)
        format_sheet(service, sheet_id, gid, cols)

    log.info("=" * 60)
    log.info(f"Done - {len(summary_rows)} tickers, {len(contract_rows)} contracts")
    log.info(f"Sheet: https://docs.google.com/spreadsheets/d/{sheet_id}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
