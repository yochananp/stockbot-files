"""
SKILL: sheets-exporter
Exports options analysis results to Google Sheets using Service Account.
No OAuth/browser needed — works from NAS, iPad, remote.
"""
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_FILE = "/app/service_account.json"


def get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    return build("sheets", "v4", credentials=creds), build("drive", "v3", credentials=creds)


def create_sheet(sheets_svc, drive_svc, title: str, owner_email: str = None) -> str:
    """Create a new Google Sheet and return its URL."""
    body = {
        "properties": {"title": title},
        "sheets": [
            {"properties": {"title": "Summary"}},
            {"properties": {"title": "Contracts"}},
        ]
    }
    resp = sheets_svc.spreadsheets().create(body=body, fields="spreadsheetId").execute()
    sheet_id = resp["spreadsheetId"]

    # Share with owner email so they can see it
    if owner_email and drive_svc:
        try:
            drive_svc.permissions().create(
                fileId=sheet_id,
                body={"type": "user", "role": "writer", "emailAddress": owner_email},
                sendNotificationEmail=False,
            ).execute()
        except Exception as e:
            logger.warning(f"Could not share sheet: {e}")

    return sheet_id


def write_to_sheet(sheets_svc, sheet_id: str, tab: str, data: list):
    body = {"values": data}
    sheets_svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body=body,
    ).execute()


def format_sheet(sheets_svc, sheet_id: str, sheet_gid: int, num_cols: int):
    requests = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": 1,
                           "startColumnIndex": 0, "endColumnIndex": num_cols},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True,
                                   "foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}},
                    "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85}
                }},
                "fields": "userEnteredFormat(textFormat,backgroundColor)"
            }
        },
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_gid,
                               "gridProperties": {"frozenRowCount": 1}},
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
    sheets_svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id, body={"requests": requests}
    ).execute()


def export_options_to_sheets(results: list, owner_email: str = None) -> str:
    """
    Export options analysis results to a new Google Sheet.
    results: list of dicts from analyze_ticker_options()
    Returns Sheet URL.
    """
    title = f"Options Analysis {datetime.today().strftime('%Y-%m-%d %H:%M')}"

    sheets_svc, drive_svc = get_sheets_service()
    sheet_id = create_sheet(sheets_svc, drive_svc, title, owner_email)

    # Build summary rows
    summary_header = [
        "Date", "Ticker", "Alert", "Spot", "IV%", "IV Rank", "Strategy", "Rationale",
        "Max Profit", "Max Risk", "Breakeven"
    ]
    summary_rows = []
    contract_rows = []
    today = datetime.today().strftime("%Y-%m-%d")

    for r in results:
        if r.get("error"):
            continue
        rp = r.get("risk_profile", {})
        summary_rows.append([
            today,
            r["ticker"],
            r.get("alert", "—"),
            r.get("spot"),
            f"{r['iv_current']:.1f}%" if r.get("iv_current") else "—",
            r.get("iv_rank") if r.get("iv_rank") is not None else "—",
            r.get("strategy", "—"),
            r.get("rationale", "—"),
            rp.get("max_profit", "—"),
            rp.get("max_risk", "—"),
            rp.get("breakeven", "—"),
        ])

        for c in r.get("contracts", []):
            contract_rows.append([
                today,
                r["ticker"],
                r.get("alert", "—"),
                r.get("strategy", "—"),
                c.get("expiry"),
                c.get("dte"),
                c.get("strike"),
                c.get("type"),
                c.get("bid"),
                c.get("ask"),
                c.get("mid"),
                f"{c['iv_pct']:.1f}%" if c.get("iv_pct") else "—",
                c.get("oi"),
                f"{c['spread_pct']:.1f}%" if c.get("spread_pct") else "—",
                c.get("delta"),
                c.get("gamma"),
                c.get("theta"),
                c.get("vega"),
                c.get("bs_price"),
            ])

    contracts_header = [
        "Date", "Ticker", "Alert", "Strategy", "Expiry", "DTE", "Strike", "Type",
        "Bid", "Ask", "Mid", "IV%", "OI", "Spread%",
        "Delta", "Gamma", "Theta", "Vega", "BS Price"
    ]

    write_to_sheet(sheets_svc, sheet_id, "Summary",   [summary_header]   + summary_rows)
    write_to_sheet(sheets_svc, sheet_id, "Contracts", [contracts_header] + contract_rows)

    # Format both tabs
    meta = sheets_svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    for sheet in meta["sheets"]:
        gid   = sheet["properties"]["sheetId"]
        tab   = sheet["properties"]["title"]
        cols  = len(summary_header) if tab == "Summary" else len(contracts_header)
        format_sheet(sheets_svc, sheet_id, gid, cols)

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    logger.info(f"Sheet created: {url}")
    return url
