from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import quality_check_context

PIPELINE_NAME = Path(__file__).parent.name

_TABLE = "earnings_calendar"
_DATE_COL = "execution_date"

# execution_date は SQL で直接セットされるため対象外。
_NOT_NULL_COLUMNS = ["Date", "Code", "CoName", "FY", "SectorNm", "FQ", "Section"]


def main() -> None:
    with quality_check_context(
        pipeline_name=PIPELINE_NAME,
        dataset_fn=jquants_staging_dataset,
        table=_TABLE,
        date_col=_DATE_COL,
        check_date_fn=lambda: datetime.now(ZoneInfo("Asia/Tokyo")).date(),
    ) as qc:
        qc.check_not_null_static(columns=_NOT_NULL_COLUMNS)


if __name__ == "__main__":
    main()
