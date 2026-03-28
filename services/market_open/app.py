# app.py
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from zoneinfo import ZoneInfo

from libs.utils import access_secret, get_project_id
from libs.utils_jquants import is_market_open_date

app = FastAPI(title="market-open-service", version="2.0.0")

_api_key: Optional[str] = None


@app.get("/health")
def health():
    return {"status": "ok"}


def _validate_date_yyyy_mm_dd(date_str: str) -> str:
    # 厳密に YYYY-MM-DD のみ許可
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD") from e
    return date_str


def _get_api_key() -> str:
    global _api_key
    if _api_key is None:
        project_id = get_project_id()
        _api_key = access_secret("jquants-api-key", project_id)
    return _api_key


@app.get("/market-open")
def market_open(
    date: Optional[str] = Query(
        default=None,
        description="Target date in YYYY-MM-DD. If omitted, uses today's JST date.",
        examples=["2026-01-29"],
    )
):
    """
    Returns whether the market is open on the given date.
    Returns HTTP 200 on success, HTTP 500 on error.
    Callers should treat non-2xx as an infrastructure failure, not as market_open=false.
    """
    today_jst = datetime.now(ZoneInfo("Asia/Tokyo")).date()

    if date is None:
        logical_date = today_jst
        logical_date_str = logical_date.isoformat()
    else:
        logical_date_str = _validate_date_yyyy_mm_dd(date)
        logical_date = datetime.strptime(logical_date_str, "%Y-%m-%d").date()

    if logical_date > today_jst:
        raise ValueError(
            f"logical_date {logical_date} is in the future (today is {today_jst} JST). Aborting."
        )

    try:
        api_key = _get_api_key()
        market_open_flag = is_market_open_date(logical_date_str, api_key)

        return {
            "logical_date": logical_date_str,
            "market_open": market_open_flag,
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "logical_date": logical_date_str,
                "error": str(e),
            },
        )
