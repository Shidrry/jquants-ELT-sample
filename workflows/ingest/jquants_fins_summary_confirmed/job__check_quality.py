from pathlib import Path

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import compute_null_rates, quality_check_context, run_anomaly_check

PIPELINE_NAME = Path(__file__).parent.name

# fins_summary と同じカラムセットで確報の NULL 率を独立管理する。
# ShOutFY / TrShFY は時価総額計算に直結するため最優先。
CHECK_COLUMNS = [
    "ShOutFY", "TrShFY",
    "EPS", "Sales",
    "FEPS", "NxFEPS", "FSales", "NxFSales",
    "OP", "NP", "TA", "Eq", "BPS", "FOP", "FNP",
]


def main() -> None:
    with quality_check_context() as (client, project_id, env_name, logical_date):
        metrics = compute_null_rates(
            client, project_id, jquants_staging_dataset(env_name), "fins_summary",
            "DiscDate", logical_date, CHECK_COLUMNS,
            dimension_col="DocType",
        )
        run_anomaly_check(client, project_id, env_name, PIPELINE_NAME, logical_date, metrics)


if __name__ == "__main__":
    main()
