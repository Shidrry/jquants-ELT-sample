from pathlib import Path

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import quality_check_context

PIPELINE_NAME = Path(__file__).parent.name

_TABLE = "fins_summary"
_DATE_COL = "DiscDate"

# J-Quants API に定義された DocType の全値（2025年7月時点）
# 未知の値が混入した場合に検知するためのホワイトリスト
_KNOWN_DOC_TYPES = [
    "FYFinancialStatements_Consolidated_JP",
    "FYFinancialStatements_Consolidated_US",
    "FYFinancialStatements_NonConsolidated_JP",
    "FYFinancialStatements_Consolidated_JMIS",
    "FYFinancialStatements_NonConsolidated_IFRS",
    "FYFinancialStatements_Consolidated_IFRS",
    "FYFinancialStatements_NonConsolidated_Foreign",
    "FYFinancialStatements_Consolidated_Foreign",
    "FYFinancialStatements_Consolidated_REIT",
    "1QFinancialStatements_Consolidated_REIT",
    "2QFinancialStatements_Consolidated_REIT",
    "3QFinancialStatements_Consolidated_REIT",
    "OtherPeriodFinancialStatements_Consolidated_REIT",
    "1QFinancialStatements_Consolidated_JP",
    "1QFinancialStatements_Consolidated_US",
    "1QFinancialStatements_NonConsolidated_JP",
    "1QFinancialStatements_Consolidated_JMIS",
    "1QFinancialStatements_NonConsolidated_IFRS",
    "1QFinancialStatements_Consolidated_IFRS",
    "1QFinancialStatements_NonConsolidated_Foreign",
    "1QFinancialStatements_Consolidated_Foreign",
    "2QFinancialStatements_Consolidated_JP",
    "2QFinancialStatements_Consolidated_US",
    "2QFinancialStatements_NonConsolidated_JP",
    "2QFinancialStatements_Consolidated_JMIS",
    "2QFinancialStatements_NonConsolidated_IFRS",
    "2QFinancialStatements_Consolidated_IFRS",
    "2QFinancialStatements_NonConsolidated_Foreign",
    "2QFinancialStatements_Consolidated_Foreign",
    "3QFinancialStatements_Consolidated_JP",
    "3QFinancialStatements_Consolidated_US",
    "3QFinancialStatements_NonConsolidated_JP",
    "3QFinancialStatements_Consolidated_JMIS",
    "3QFinancialStatements_NonConsolidated_IFRS",
    "3QFinancialStatements_Consolidated_IFRS",
    "3QFinancialStatements_NonConsolidated_Foreign",
    "3QFinancialStatements_Consolidated_Foreign",
    "OtherPeriodFinancialStatements_Consolidated_JP",
    "OtherPeriodFinancialStatements_Consolidated_US",
    "OtherPeriodFinancialStatements_NonConsolidated_JP",
    "OtherPeriodFinancialStatements_Consolidated_JMIS",
    "OtherPeriodFinancialStatements_NonConsolidated_IFRS",
    "OtherPeriodFinancialStatements_Consolidated_IFRS",
    "OtherPeriodFinancialStatements_NonConsolidated_Foreign",
    "OtherPeriodFinancialStatements_Consolidated_Foreign",
    "DividendForecastRevision",
    "EarnForecastRevision",
    "REITDividendForecastRevision",
    "REITEarnForecastRevision",
]

def main() -> None:
    with quality_check_context(
        pipeline_name=PIPELINE_NAME,
        dataset_fn=jquants_staging_dataset,
        table=_TABLE,
        date_col=_DATE_COL,
    ) as qc:
        qc.check_allowed_values(column="DocType", allowed=_KNOWN_DOC_TYPES)
        # 全カラムを対象に DocType ごとの過去1年 NULL 率 ≤ 5% を必須カラムとして動的に検出
        qc.check_not_null_dynamic(
            dimension_col="DocType",
            baseline_days=365,
            baseline_null_threshold=0.05,
        )


if __name__ == "__main__":
    main()
