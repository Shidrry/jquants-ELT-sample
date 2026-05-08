from pathlib import Path

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import compute_null_rates, quality_check_context, run_anomaly_check

PIPELINE_NAME = Path(__file__).parent.name

# master は月次更新のため lookback を長くとる
_CHECK_COLUMNS = ["Code", "CoName", "S17", "S17Nm", "S33", "S33Nm", "Mkt", "MktNm"]


def main() -> None:
    with quality_check_context() as (client, project_id, env_name, logical_date):
        metrics = compute_null_rates(
            client, project_id, jquants_staging_dataset(env_name), "master",
            "logical_date", logical_date, _CHECK_COLUMNS,
        )
        run_anomaly_check(client, project_id, env_name, PIPELINE_NAME, logical_date, metrics, lookback_days=400)


if __name__ == "__main__":
    main()
