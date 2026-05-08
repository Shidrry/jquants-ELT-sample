from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import compute_null_rates, quality_check_context, run_zero_tolerance_check

PIPELINE_NAME = Path(__file__).parent.name

# execution_date は SQL で直接セットされるため対象外。
_CHECK_COLUMNS = ["Date", "Code", "CoName", "FY", "SectorNm", "FQ", "Section"]


def main() -> None:
    with quality_check_context() as (client, project_id, env_name, logical_date):
        execution_date = datetime.now(ZoneInfo("Asia/Tokyo")).date()
        metrics = compute_null_rates(
            client, project_id, jquants_staging_dataset(env_name), "earnings_calendar",
            "execution_date", execution_date, _CHECK_COLUMNS,
        )
        run_zero_tolerance_check(
            client, project_id, env_name, PIPELINE_NAME, logical_date, metrics,
            check_date=execution_date,
        )


if __name__ == "__main__":
    main()
