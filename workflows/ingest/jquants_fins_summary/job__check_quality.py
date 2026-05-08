from pathlib import Path

from libs.config import jquants_staging_dataset
from libs.utils_data_quality import compute_null_rates, quality_check_context, run_anomaly_check

PIPELINE_NAME = Path(__file__).parent.name

# 監視カラムの優先度:
#   高: ShOutFY / TrShFY → 時価総額算出（close_price × (ShOutFY - COALESCE(TrShFY, 0))）に
#       直結し、市場参加フィルタ（時価総額ベース集計）に影響する。
#   中: Sales / OP / NP 等 → 現時点の SQL では未参照だが、API 仕様変更によるサイレントな
#       NULL 増加を DocType 別に早期検知するためにあわせて記録する。
# DocType・DiscDate・Code は結合キーのため次元集計・フィルタで担保し、ここでは数値系のみ対象とする。
CHECK_COLUMNS = [
    # int_daily_stock_metrics: 時価総額算出に直結
    "ShOutFY", "TrShFY",
    # 前期・前々期の実績値
    "EPS", "Sales",
    # 今期予想（FEPS が null の FY 開示時は NxF* を使用）
    "FEPS", "NxFEPS", "FSales", "NxFSales",
    # 直接参照はないが API 仕様変更の早期検知のために記録
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
