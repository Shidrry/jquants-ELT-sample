from pathlib import Path

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import (
    check_prev_day_row_count,
    compute_null_rates,
    quality_check_context,
    run_anomaly_check,
)

PIPELINE_NAME = Path(__file__).parent.name

# 部分取り込み・仕様変更の影響を受けやすいコアカラムのみ対象とする。
# Premium プランのみで提供される前場・後場カラムは含めない。
CORE_COLUMNS = [
    "Open", "High", "Low", "Close", "Volume", "TurnoverValue",
    "AdjustmentFactor", "AdjustmentClose", "AdjustmentVolume",
]


def main() -> None:
    with quality_check_context() as (client, project_id, env_name, logical_date):
        dataset = jquants_staging_dataset(env_name)
        count_rows = list(client.query(
            f"SELECT COUNT(*) AS cnt FROM `{project_id}.{dataset}.daily_quotes` "
            f"WHERE Date = DATE('{logical_date}')"
        ).result())
        current_count = count_rows[0]["cnt"]
        metrics = compute_null_rates(
            client, project_id, dataset, "daily_quotes",
            "Date", logical_date, CORE_COLUMNS,
        )
        warning = check_prev_day_row_count(
            client, project_id, env_name, PIPELINE_NAME, logical_date, current_count,
        )
        run_anomaly_check(
            client, project_id, env_name, PIPELINE_NAME, logical_date, metrics,
            extra_warnings=[warning] if warning else None,
        )


if __name__ == "__main__":
    main()
