from pathlib import Path

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import quality_check_context

PIPELINE_NAME = Path(__file__).parent.name

_TABLE = "daily_quotes"
_DATE_COL = "Date"

# 部分取り込み・仕様変更の影響を受けやすいコアカラムのみ対象とする。
# Premium プランのみで提供される前場・後場カラムは含めない。
_CORE_COLUMNS = [
    "Open", "High", "Low", "Close", "Volume", "TurnoverValue",
    "AdjustmentFactor", "AdjustmentClose", "AdjustmentVolume",
]


def main() -> None:
    with quality_check_context(
        pipeline_name=PIPELINE_NAME,
        dataset_fn=jquants_staging_dataset,
        table=_TABLE,
        date_col=_DATE_COL,
    ) as qc:
        qc.check_row_count(threshold=0.8, lookback_days=1)
        qc.check_null_rate_anomaly(columns=_CORE_COLUMNS, delta_threshold=0.05, lookback_days=1)


if __name__ == "__main__":
    main()
