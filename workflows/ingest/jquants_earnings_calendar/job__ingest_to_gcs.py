import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from libs.config import jquants_lake_bucket
from libs.utils import access_secret, dataframe_to_parquet_bytes, get_env_name, get_project_id, upload_bytes_to_gcs
from libs.utils_jquants import fetch_jquants_earnings_calendar

# ---- 新しいパイプライン作成時にここだけ変更する -------------------------
GCS_OBJECT = "earnings_calendar/dt={date}/earnings_calendar.parquet"
BUCKET_FN  = jquants_lake_bucket  # (project_id, env_name) -> str

def fetch_records(project_id: str) -> list[dict]:
    api_key = access_secret("jquants-api-key", project_id)
    # Date が空文字（発表日未定）のレコードは除外する
    return [r for r in fetch_jquants_earnings_calendar(api_key) if r.get("Date")]
# -------------------------------------------------------------------------


# earnings_calendar は翌営業日分のみ返すため、当日の発表がない場合はレコード数が 0 になり得る。
# OHLCV と異なり 0 件は正常ケースのため、エラーにせず空の Parquet をアップロードする。
_EXPECTED_COLUMNS = ["Date", "Code", "CoName", "FY", "SectorNm", "FQ", "Section"]


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    env_name = get_env_name()
    project_id = get_project_id()
    execution_date = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    logging.info("Job executed at %s", execution_date.isoformat())

    records = fetch_records(project_id)
    df = pd.DataFrame(records) if records else pd.DataFrame(columns=_EXPECTED_COLUMNS)

    bucket = BUCKET_FN(project_id, env_name)
    object_path = GCS_OBJECT.format(date=execution_date.isoformat())
    upload_bytes_to_gcs(bucket, object_path, dataframe_to_parquet_bytes(df))

    logging.info("Fetched earnings calendar %s", {
        "execution_date": execution_date.isoformat(),
        "records": len(records),
        "bucket": bucket,
        "object_path": object_path,
    })


if __name__ == "__main__":
    main()
