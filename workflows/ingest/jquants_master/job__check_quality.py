from pathlib import Path

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import quality_check_context

PIPELINE_NAME = Path(__file__).parent.name

_TABLE = "master"
_DATE_COL = "logical_date"

# 上場銘柄マスタのコアフィールド。すべて必須項目で NULL は想定しない。
_NOT_NULL_COLUMNS = ["Code", "CoName", "S17", "S17Nm", "S33", "S33Nm", "Mkt", "MktNm"]


def main() -> None:
    with quality_check_context(
        pipeline_name=PIPELINE_NAME,
        dataset_fn=jquants_staging_dataset,
        table=_TABLE,
        date_col=_DATE_COL,
    ) as qc:
        qc.check_row_count(threshold=0.8, lookback_days=1)
        qc.check_not_null_static(columns=_NOT_NULL_COLUMNS)


if __name__ == "__main__":
    main()
