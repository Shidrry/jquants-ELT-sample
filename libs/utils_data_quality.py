"""データ品質チェック共通ライブラリ。

各 job__check_quality.py から呼び出される。チェックの種類と行数比較の使い分け:

  run_anomaly_check        -- 過去 N 日の NULL 率平均と比較し、閾値超えを通知（統計的異常検知）
  run_zero_tolerance_check -- NULL が 1 件でもあれば即通知（パーサー検証済みデータ向け）

  check_table_row_count    -- マートテーブル向け: 同テーブル内の直前営業日行数と比較
  check_prev_day_row_count -- ingest パイプライン向け: data_loads テーブルの前日行数と比較
"""
import logging
from contextlib import contextmanager
from datetime import date, datetime, timezone

from google.cloud import bigquery

from libs.config import pipeline_metadata_dataset
from libs.utils import get_env_name, get_logical_date, get_project_id
from libs.utils_discord import notify_discord


@contextmanager
def quality_check_context():
    """job__check_quality.py 共通のセットアップを提供するコンテキストマネージャ。
    処理エラーは呼び出し元に伝播させてジョブを失敗させる（ワークフローエラーとして扱う）。
    """
    logging.basicConfig(level=logging.INFO)
    env_name = get_env_name()
    project_id = get_project_id()
    logical_date, _ = get_logical_date()
    client = bigquery.Client(project=project_id)
    yield client, project_id, env_name, logical_date

_DQ_TABLE = "data_quality_metrics"

_SCHEMA = [
    bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("logical_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dimension", "STRING"),
    bigquery.SchemaField("null_rate", "FLOAT64"),
    bigquery.SchemaField("row_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("checked_at", "TIMESTAMP", mode="REQUIRED"),
]


def compute_null_rates(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    date_col: str,
    check_date: date,
    columns: list[str],
    dimension_col: str | None = None,
) -> list[dict]:
    """列ごとのNULL率を返す。dimension_col 指定時はその値でグループ化する。"""
    null_exprs = ", ".join(f"COUNTIF(`{c}` IS NULL) AS nc{i}" for i, c in enumerate(columns))
    select_dim = f"`{dimension_col}` AS dimension," if dimension_col else "CAST(NULL AS STRING) AS dimension,"
    group_by = f"GROUP BY `{dimension_col}`" if dimension_col else ""

    query = f"""
    SELECT {select_dim} COUNT(*) AS row_count, {null_exprs}
    FROM `{project}.{dataset}.{table}`
    WHERE `{date_col}` = DATE('{check_date}')
    {group_by}
    """
    results = []
    for row in client.query(query).result():
        rc = row["row_count"]
        dim = row["dimension"]
        for i, col in enumerate(columns):
            nc = row[f"nc{i}"]
            results.append({
                "column_name": col,
                "null_rate": nc / rc if rc > 0 else None,
                "row_count": rc,
                "dimension": dim,
            })
    return results


def record_quality_metrics(
    client: bigquery.Client,
    project_id: str,
    env_name: str,
    pipeline_name: str,
    logical_date: date,
    metrics: list[dict],
) -> None:
    """data_quality_metrics テーブルに記録する。"""
    dataset_id = pipeline_metadata_dataset(env_name)
    table_ref = f"{project_id}.{dataset_id}.{_DQ_TABLE}"
    client.create_table(bigquery.Table(table_ref, schema=_SCHEMA), exists_ok=True)

    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "pipeline_name": pipeline_name,
            "logical_date": logical_date.isoformat(),
            "column_name": m["column_name"],
            "dimension": m.get("dimension"),
            "null_rate": m["null_rate"],
            "row_count": m["row_count"],
            "checked_at": now,
        }
        for m in metrics
    ]
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"Failed to insert quality metrics: {errors}")


def get_historical_avg_null_rates(
    client: bigquery.Client,
    project_id: str,
    env_name: str,
    pipeline_name: str,
    logical_date: date,
    lookback_days: int = 60,
) -> dict[tuple[str, str | None], float]:
    """過去 lookback_days 日の (column_name, dimension) ごとの NULL 率平均を返す。"""
    dataset_id = pipeline_metadata_dataset(env_name)
    query = f"""
    SELECT column_name, dimension, AVG(null_rate) AS avg_null_rate
    FROM `{project_id}.{dataset_id}.{_DQ_TABLE}`
    WHERE pipeline_name = '{pipeline_name}'
      AND logical_date BETWEEN
        DATE_SUB(DATE('{logical_date}'), INTERVAL {lookback_days} DAY)
        AND DATE_SUB(DATE('{logical_date}'), INTERVAL 1 DAY)
      AND null_rate IS NOT NULL
    GROUP BY column_name, dimension
    """
    return {
        (row["column_name"], row["dimension"]): row["avg_null_rate"]
        for row in client.query(query).result()
    }


def find_anomalies(
    current_metrics: list[dict],
    historical_avgs: dict[tuple[str, str | None], float],
    delta_threshold: float = 0.05,
) -> list[str]:
    """NULL 率が過去平均から delta_threshold 以上増加した列の説明文リストを返す。"""
    anomalies = []
    for m in current_metrics:
        if m["null_rate"] is None:
            continue
        key = (m["column_name"], m.get("dimension"))
        hist = historical_avgs.get(key)
        if hist is None:
            continue
        delta = m["null_rate"] - hist
        if delta >= delta_threshold:
            dim_info = f" (DocType={m['dimension']})" if m.get("dimension") else ""
            anomalies.append(
                f"{m['column_name']}{dim_info}: {hist:.1%} → {m['null_rate']:.1%} (+{delta:.1%})"
            )
    return anomalies


def _log_metrics(pipeline_name: str, check_date: date, metrics: list[dict]) -> None:
    if not metrics:
        logging.info("[%s] no metrics for %s", pipeline_name, check_date)
        return
    by_dim: dict[str | None, list[dict]] = {}
    for m in metrics:
        by_dim.setdefault(m.get("dimension"), []).append(m)
    for dim, ms in by_dim.items():
        rc = ms[0]["row_count"]
        label = f"(DocType={dim}) " if dim else ""
        rates = "  ".join(
            f"{m['column_name']}=" + ("N/A" if m['null_rate'] is None else f"{m['null_rate']:.1%}")
            for m in ms
        )
        logging.info("[%s] %snull rates for %s (%d rows): %s", pipeline_name, label, check_date, rc, rates)


# --- チェック実行エントリポイント ---
# run_anomaly_check        : メトリクス記録 → 過去比較 → 異常時 Discord 通知
# run_zero_tolerance_check : メトリクス記録 → NULL 存在時 Discord 通知


def run_anomaly_check(
    client: bigquery.Client,
    project_id: str,
    env_name: str,
    pipeline_name: str,
    logical_date: date,
    metrics: list[dict],
    lookback_days: int = 60,
    extra_warnings: list[str] | None = None,
) -> None:
    """メトリクスを記録し、過去比較で異常があれば Discord に通知する。"""
    _log_metrics(pipeline_name, logical_date, metrics)
    record_quality_metrics(client, project_id, env_name, pipeline_name, logical_date, metrics)
    historical = get_historical_avg_null_rates(
        client, project_id, env_name, pipeline_name, logical_date, lookback_days=lookback_days,
    )
    anomalies = find_anomalies(metrics, historical)

    warnings = list(extra_warnings or [])
    if anomalies:
        warnings.append("NULL率異常:\n" + "\n".join(f"  {a}" for a in anomalies))

    if warnings:
        notify_discord(
            f"⚠️ **{pipeline_name} データ品質警告** [{env_name}]\n"
            f"date: `{logical_date}`\n"
            + "\n".join(warnings)
        )
        logging.warning("Quality warnings: %s", warnings)
    else:
        logging.info("Quality check passed for %s.", logical_date)


def run_zero_tolerance_check(
    client: bigquery.Client,
    project_id: str,
    env_name: str,
    pipeline_name: str,
    logical_date: date,
    metrics: list[dict],
    check_date: date | None = None,
) -> None:
    """メトリクスを記録し、NULL が含まれるカラムがあれば Discord に通知する。"""
    display_date = check_date or logical_date
    _log_metrics(pipeline_name, display_date, metrics)
    record_quality_metrics(client, project_id, env_name, pipeline_name, logical_date, metrics)
    null_items = [m for m in metrics if m["null_rate"] and m["null_rate"] > 0]
    if null_items:
        null_detail = "\n".join(f"  {m['column_name']}: {m['null_rate']:.1%}" for m in null_items)
        notify_discord(
            f"⚠️ **{pipeline_name} データ品質警告** [{env_name}]\n"
            f"date: `{display_date}`\n"
            f"NULLが含まれるカラム:\n{null_detail}"
        )
        logging.warning("Null columns found: %s", [m["column_name"] for m in null_items])
    else:
        logging.info("Quality check passed: all columns non-null.")


# --- 行数チェック ---
# check_table_row_count    : data_loads を持たないマートテーブル向け（テーブル内の直前日と比較）
# check_prev_day_row_count : ingest パイプライン向け（data_loads テーブルの前日行数と比較）


def check_table_row_count(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    table: str,
    date_col: str,
    check_date: date,
    current_count: int,
    ratio_threshold: float = 0.8,
) -> str | None:
    """テーブル内の直前日行数と比較し、ratio_threshold 未満なら警告文を返す。data_loads を持たないマートテーブル向け。"""
    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{project_id}.{dataset}.{table}`
    WHERE `{date_col}` = (
      SELECT MAX(`{date_col}`)
      FROM `{project_id}.{dataset}.{table}`
      WHERE `{date_col}` >= DATE_SUB(DATE('{check_date}'), INTERVAL 14 DAY)
        AND `{date_col}` < DATE('{check_date}')
    )
    """
    rows = list(client.query(query).result())
    if not rows or rows[0]["row_count"] == 0:
        return None
    prev_count = rows[0]["row_count"]
    ratio = current_count / prev_count
    if ratio < ratio_threshold:
        return (
            f"行数が前営業日比 {ratio:.1%} に減少しています "
            f"（本日: {current_count:,}件 / 前営業日: {prev_count:,}件）"
        )
    return None


def check_prev_day_row_count(
    client: bigquery.Client,
    project_id: str,
    env_name: str,
    pipeline_name: str,
    logical_date: date,
    current_count: int,
    ratio_threshold: float = 0.8,
) -> str | None:
    """前営業日の行数と比較し、ratio_threshold 未満なら警告文を返す。なければ None。"""
    dataset_id = pipeline_metadata_dataset(env_name)
    query = f"""
    SELECT row_count
    FROM `{project_id}.{dataset_id}.data_loads`
    WHERE pipeline_name = '{pipeline_name}'
      AND logical_date < DATE('{logical_date}')
      AND logical_date >= DATE_SUB(DATE('{logical_date}'), INTERVAL 14 DAY)
    ORDER BY logical_date DESC
    LIMIT 1
    """
    rows = list(client.query(query).result())
    if not rows:
        return None
    prev_count = rows[0]["row_count"]
    if prev_count == 0:
        return None
    ratio = current_count / prev_count
    if ratio < ratio_threshold:
        return (
            f"行数が前営業日比 {ratio:.1%} に減少しています "
            f"（本日: {current_count:,}件 / 前営業日: {prev_count:,}件）"
        )
    return None
