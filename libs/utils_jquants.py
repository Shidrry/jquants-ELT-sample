from typing import Iterable, Dict, Any, Optional

import requests

from libs.config import JQUANTS_API_BASE


def fetch_jquants_bars_daily(date_str: str, api_key: str) -> Iterable[Dict[str, Any]]:
    url = f"{JQUANTS_API_BASE}/equities/bars/daily"
    headers = {"x-api-key": api_key}

    pagination_key: Optional[str] = None
    while True:
        params = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        records = data.get("data", [])
        for r in records:
            yield r

        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break


def fetch_jquants_earnings_calendar(api_key: str) -> Iterable[Dict[str, Any]]:
    """
    /v2/equities/earnings-calendar から翌営業日の決算発表予定銘柄を取得する。
    3月期・9月期決算会社のみ対象。毎日19時頃に更新される。
    Date が空文字のレコード（発表日未定）も返すため、呼び出し元でフィルタすること。
    """
    url = f"{JQUANTS_API_BASE}/equities/earnings-calendar"
    headers = {"x-api-key": api_key}

    pagination_key: Optional[str] = None
    while True:
        params = {}
        if pagination_key:
            params["pagination_key"] = pagination_key

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        records = data.get("data", [])
        for r in records:
            yield r

        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break


def fetch_jquants_master(date_str: str, api_key: str) -> Iterable[Dict[str, Any]]:
    """
    /v2/equities/master から指定日付時点の全上場銘柄情報を取得する。
    指定日が休業日の場合は翌営業日時点のデータが返却される。
    """
    url = f"{JQUANTS_API_BASE}/equities/master"
    headers = {"x-api-key": api_key}

    pagination_key: Optional[str] = None
    while True:
        params = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        records = data.get("data", [])
        for r in records:
            yield r

        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break


def fetch_jquants_fins_summary(date_str: str, api_key: str) -> Iterable[Dict[str, Any]]:
    """
    /v2/fins/summary から指定日付の全銘柄の財務情報サマリーを取得する。
    休日や財務発表なしの日は空リストが返ることがある。
    """
    url = f"{JQUANTS_API_BASE}/fins/summary"
    headers = {"x-api-key": api_key}

    pagination_key: Optional[str] = None
    while True:
        params = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        records = data.get("data", [])
        for r in records:
            yield r

        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break


def is_market_open_date(date_str: str, api_key: str) -> bool:
    """
    Check if the given date is a market open date (business day) using jquants /v2/markets/calendar.

    Args:
        date_str: Date string in format "YYYY-MM-DD" or "YYYYMMDD"
        api_key: J-Quants API key

    Returns:
        True if the market is open on the given date, False otherwise
    """
    # J-Quants API expects YYYYMMDD; normalize from YYYY-MM-DD if needed
    date_normalized = date_str.replace("-", "")

    url = f"{JQUANTS_API_BASE}/markets/calendar"
    headers = {"x-api-key": api_key}
    # hol_div "1" = business day. Fetch only business day data for the specified date
    params = {"hol_div": "1", "from": date_normalized, "to": date_normalized}

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    calendar = data.get("data", [])

    # hol_div=1 で絞り込んでいるため、休日・週末は data=[] が返る（正常）。
    # 認証エラーや不正リクエストは raise_for_status() で検出済み。
    for entry in calendar:
        if entry.get("Date", "").replace("-", "") == date_normalized:
            return True

    # Date not found → market is closed
    return False
