"""データ品質チェック共通ライブラリ。

job__check_quality.py は `quality_check_context()` で QualityChecker を取得し、
ブロック内で必要なチェックを呼ぶ。ブロック退出時に蓄積された失敗の集約通知を
自動的に Discord エラー部屋へ 1 通送る（呼び忘れ防止）。

    with quality_check_context(
        pipeline_name=PIPELINE_NAME,
        dataset_fn=jquants_staging_dataset,
        table=_TABLE,
        date_col=_DATE_COL,
    ) as qc:
        qc.check_row_count()
        qc.check_null_rate_anomaly(columns=[...])
        qc.check_not_null_static(columns=[...])
        qc.check_not_null_dynamic(dimension_col="DocType")
        qc.check_value_range(column="Volume", min_val=0)

各チェックの詳細はチェック単位で WARNING ログに残し、Discord 通知はジョブ末尾の集約 1 通に集約する。
ジョブは exit 0 で終了し、workflow の成否には影響させない（外部 API 起因の品質問題は人手介入で直せないため、
気づき優先で workflow は止めない設計）。Discord 通知自体が失敗してもジョブは落とさず ERROR ログに留める。
"""
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date

from google.cloud import bigquery

from libs.utils import get_env_name, get_logical_date, get_project_id
from libs.utils_discord import notify_discord


@contextmanager
def quality_check_context(
    *,
    pipeline_name: str,
    dataset_fn: Callable[[str], str],
    table: str,
    date_col: str,
    check_date_fn: Callable[[], date] | None = None,
) -> Iterator["QualityChecker"]:
    """job__check_quality.py 共通のエントリ。

    QualityChecker を生成して yield し、ブロック退出時に必ず notify_if_failed() を呼ぶ。
    各ジョブで明示的に notify_if_failed() を呼ぶ必要はない（呼び忘れ防止）。

    dataset_fn は env_name を引数に取りデータセット名を返す関数（例: `jquants_staging_dataset`）。
    check_date_fn は logical_date 以外を check_date に使いたい場合に指定する callable。
    """
    logging.basicConfig(level=logging.INFO)
    env_name = get_env_name()
    project_id = get_project_id()
    logical_date, _ = get_logical_date()
    client = bigquery.Client(project=project_id)
    qc = QualityChecker(
        client, project_id, env_name, pipeline_name, logical_date,
        dataset=dataset_fn(env_name),
        table=table,
        date_col=date_col,
        check_date=check_date_fn() if check_date_fn else None,
    )
    try:
        yield qc
    finally:
        qc.notify_if_failed()


class QualityChecker:
    """パイプラインのデータ品質チェックを実行するクラス。

    check_date は省略時 logical_date を使用する。
    earnings_calendar のように実行日付でパーティションされるテーブルは
    check_date=execution_date を明示的に渡す。
    """

    def __init__(
        self,
        client: bigquery.Client,
        project_id: str,
        env_name: str,
        pipeline_name: str,
        logical_date: date,
        *,
        dataset: str,
        table: str,
        date_col: str,
        check_date: date | None = None,
    ):
        self._client = client
        self._project_id = project_id
        self._env_name = env_name
        self._pipeline_name = pipeline_name
        self._dataset = dataset
        self._table = table
        self._date_col = date_col
        self._check_date = check_date or logical_date
        self._failures: list[str] = []

    # --- チェックメソッド ---

    def check_row_count(self, threshold: float = 0.8, lookback_days: int = 1) -> None:
        """当日の行数が比較期間の平均に比べて threshold 未満なら Discord に通知する。
        lookback_days=1（デフォルト）は直前日のみ、N>1 は直近 N 日の平均と比較する。
        """
        current = self._count(self._check_date)
        baseline = self._avg_count_for_period(lookback_days)
        if baseline == 0:
            logging.info("[%s] row_count check skipped: no previous data", self._pipeline_name)
            return
        ratio = current / baseline
        if ratio < threshold:
            period_label = "前日" if lookback_days == 1 else f"直近{lookback_days}日平均"
            self._warn(
                f"行数が{period_label}比 {ratio:.1%} に減少しています "
                f"（本日: {current:,}件 / {period_label}: {baseline:,.0f}件）"
            )
        else:
            logging.info("[%s] row_count ok: %d rows (%.0f%% of baseline)", self._pipeline_name, current, ratio * 100)

    def check_null_rate_anomaly(
        self,
        columns: list[str],
        dimension_col: str | None = None,
        delta_threshold: float = 0.05,
        lookback_days: int = 1,
    ) -> None:
        """指定カラムの NULL 率が比較期間から delta_threshold 以上増加したら Discord に通知する。
        lookback_days=1（デフォルト）は直前日のみ、N>1 は直近 N 日の combined 値と比較する。
        """
        current = self._null_rates(columns, f"= DATE('{self._check_date}')", dimension_col=dimension_col)
        baseline = self._null_rates_for_period(columns, lookback_days, dimension_col=dimension_col)
        if not baseline:
            logging.info("[%s] null_rate_anomaly check skipped: no previous data", self._pipeline_name)
            return

        baseline_map = {
            (m["column_name"], m.get("dimension")): m["null_rate"]
            for m in baseline
            if m["null_rate"] is not None
        }
        anomalies = []
        for m in current:
            if m["null_rate"] is None:
                continue
            base_rate = baseline_map.get((m["column_name"], m.get("dimension")))
            if base_rate is None:
                continue
            delta = m["null_rate"] - base_rate
            if delta >= delta_threshold:
                dim_info = f" ({dimension_col}={m['dimension']})" if m.get("dimension") else ""
                anomalies.append(
                    f"{m['column_name']}{dim_info}: {base_rate:.1%} → {m['null_rate']:.1%} (+{delta:.1%})"
                )

        if anomalies:
            self._warn("NULL率異常:\n" + "\n".join(f"  {a}" for a in anomalies))
        else:
            logging.info("[%s] null_rate_anomaly check passed", self._pipeline_name)

    def check_not_null_static(self, columns: list[str], where: str | None = None) -> None:
        """明示した必須カラムに NULL が 1 件でもあれば Discord に通知する。
        where に SQL 条件を渡すと対象行を絞り込める（例: "DocType LIKE 'FYFinancialStatements%'"）。

        必須カラムが固定で分かっている場合に使う。動的に検出したい場合は check_not_null_dynamic を使う。
        """
        metrics = self._null_rates(columns, f"= DATE('{self._check_date}')", where_extra=where)
        null_items = [m for m in metrics if m["null_rate"] is not None and m["null_rate"] > 0]
        if null_items:
            filter_info = f"\nfilter: `{where}`" if where else ""
            detail = "\n".join(f"  {m['column_name']}: {m['null_rate']:.1%}" for m in null_items)
            self._warn(f"NULL が含まれるカラム:{filter_info}\n{detail}")
        else:
            logging.info("[%s] check_not_null_static passed: %s", self._pipeline_name, columns)

    def check_not_null_dynamic(
        self,
        dimension_col: str,
        columns: list[str] | None = None,
        baseline_days: int = 365,
        baseline_null_threshold: float = 0.05,
    ) -> None:
        """dimension_col 単位で過去 baseline_days 日の NULL 率が
        baseline_null_threshold 以下のカラムを必須カラムとみなし、
        当日データに NULL があれば Discord に通知する。

        必須カラムの集合をハードコードせず、過去実績から自動的に決定する。
        columns 省略時は INFORMATION_SCHEMA から全カラム（date_col・dimension_col を除く）を取得する。
        必須カラムが固定で分かっている場合は check_not_null_static を使う。
        """
        if columns is None:
            columns = self._fetch_table_columns(exclude=[self._date_col, dimension_col])
        null_exprs = ", ".join(f"COUNTIF(`{c}` IS NULL) AS nc{i}" for i, c in enumerate(columns))
        query = f"""
        SELECT
          `{dimension_col}` AS dim,
          CASE WHEN `{self._date_col}` = DATE('{self._check_date}') THEN 'current' ELSE 'baseline' END AS bucket,
          COUNT(*) AS row_count,
          {null_exprs}
        FROM `{self._project_id}.{self._dataset}.{self._table}`
        WHERE `{self._date_col}` BETWEEN DATE_SUB(DATE('{self._check_date}'), INTERVAL {baseline_days} DAY)
                                     AND DATE('{self._check_date}')
        GROUP BY dim, bucket
        """

        baseline: dict[tuple, float] = {}
        current: dict[tuple, tuple[int, int]] = {}
        for row in self._client.query(query).result():
            rc = row["row_count"]
            if rc == 0:
                continue
            for i, col in enumerate(columns):
                nc = row[f"nc{i}"]
                if row["bucket"] == "current":
                    current[(row["dim"], col)] = (nc, rc)
                else:
                    baseline[(row["dim"], col)] = nc / rc

        required = {k: v for k, v in baseline.items() if v <= baseline_null_threshold}
        if not required:
            logging.info("[%s] check_not_null_dynamic skipped: no baseline data", self._pipeline_name)
            return

        anomalies = []
        for (dim, col), base_rate in required.items():
            cur = current.get((dim, col))
            if cur is None:
                continue
            nc, rc = cur
            if nc > 0:
                anomalies.append(
                    f"{dim}.{col}: {nc:,}/{rc:,} ({nc / rc:.1%}, baseline {base_rate:.1%})"
                )

        if anomalies:
            self._warn(
                f"NULL が含まれるカラム ({dimension_col}単位、過去{baseline_days}日 NULL率 ≤ {baseline_null_threshold:.0%}):\n"
                + "\n".join(f"  {a}" for a in anomalies)
            )
        else:
            logging.info(
                "[%s] check_not_null_dynamic passed (%d targets)",
                self._pipeline_name,
                len(required),
            )

    def check_allowed_values(self, column: str, allowed: list[str], where: str | None = None) -> None:
        """指定カラムに allowed 以外の値が存在すれば Discord に通知する。"""
        allowed_literals = ", ".join(f"'{v}'" for v in allowed)
        extra = f"AND ({where})" if where else ""
        query = f"""
        SELECT `{column}` AS val, COUNT(*) AS cnt
        FROM `{self._project_id}.{self._dataset}.{self._table}`
        WHERE `{self._date_col}` = DATE('{self._check_date}')
          {extra}
          AND `{column}` NOT IN ({allowed_literals})
        GROUP BY `{column}`
        """
        rows = list(self._client.query(query).result())
        if rows:
            detail = "\n".join(f"  '{r['val']}': {r['cnt']:,}件" for r in rows)
            self._warn(f"未知の値が含まれています\ncolumn: `{column}`\n{detail}")
        else:
            logging.info("[%s] allowed_values check passed: %s", self._pipeline_name, column)

    def check_value_range(
        self,
        column: str,
        min_val: float | None = None,
        max_val: float | None = None,
        where: str | None = None,
    ) -> None:
        """指定カラムの値が [min_val, max_val] 範囲外の行があれば Discord に通知する。"""
        conditions = []
        if min_val is not None:
            conditions.append(f"`{column}` < {min_val}")
        if max_val is not None:
            conditions.append(f"`{column}` > {max_val}")
        if not conditions:
            return

        extra = f"AND ({where})" if where else ""
        query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{self._project_id}.{self._dataset}.{self._table}`
        WHERE `{self._date_col}` = DATE('{self._check_date}')
          {extra}
          AND ({" OR ".join(conditions)})
        """
        cnt = list(self._client.query(query).result())[0]["cnt"]
        if cnt > 0:
            lo = str(min_val) if min_val is not None else "−∞"
            hi = str(max_val) if max_val is not None else "+∞"
            self._warn(f"`{column}` の値が想定範囲 [{lo}, {hi}] 外の行が {cnt:,}件あります")
        else:
            logging.info("[%s] value_range check passed: %s", self._pipeline_name, column)

    def notify_if_failed(self) -> None:
        """蓄積された品質問題があれば Discord エラー部屋に集約通知を 1 通送る。
        通常 quality_check_context() がブロック退出時に自動で呼ぶ。失敗が無ければ何もしない。
        Discord 通知の例外は握りつぶして ERROR ログに残す（通知側障害でジョブを落とさない）。
        """
        if not self._failures:
            logging.info("[%s] all quality checks passed", self._pipeline_name)
            return
        body = "\n\n".join(self._failures)
        message = (
            f"❌ **{self._pipeline_name} データ品質懸念** [{self._env_name}]\n"
            f"date: `{self._check_date}`\n"
            f"検知件数: {len(self._failures)} 件\n\n"
            f"{body}"
        )
        try:
            notify_discord(message)
        except Exception as e:
            logging.error(
                "[%s] Discord notification failed: %s. %d quality issue(s) remain in WARNING logs",
                self._pipeline_name, e, len(self._failures),
            )

    # --- 内部ヘルパー ---

    def _fetch_table_columns(self, exclude: list[str] | None = None) -> list[str]:
        """INFORMATION_SCHEMA からテーブルの全カラム名を返す。"""
        query = f"""
        SELECT column_name
        FROM `{self._project_id}.{self._dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{self._table}'
        ORDER BY ordinal_position
        """
        excluded = set(exclude or [])
        return [r["column_name"] for r in self._client.query(query).result() if r["column_name"] not in excluded]

    def _count(self, check_date: date) -> int:
        query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{self._project_id}.{self._dataset}.{self._table}`
        WHERE `{self._date_col}` = DATE('{check_date}')
        """
        return list(self._client.query(query).result())[0]["cnt"]

    def _avg_count_for_period(self, lookback_days: int) -> float:
        """直近 lookback_days 日の日別行数の平均を返す。対象日がなければ 0.0。"""
        query = f"""
        SELECT AVG(cnt) AS avg_count
        FROM (
          SELECT COUNT(*) AS cnt
          FROM `{self._project_id}.{self._dataset}.{self._table}`
          WHERE `{self._date_col}` IN (
            SELECT DISTINCT `{self._date_col}`
            FROM `{self._project_id}.{self._dataset}.{self._table}`
            WHERE `{self._date_col}` < DATE('{self._check_date}')
            ORDER BY `{self._date_col}` DESC
            LIMIT {lookback_days}
          )
          GROUP BY `{self._date_col}`
        )
        """
        rows = list(self._client.query(query).result())
        v = rows[0]["avg_count"] if rows else None
        return float(v) if v is not None else 0.0

    def _null_rates(
        self,
        columns: list[str],
        date_where: str,
        dimension_col: str | None = None,
        where_extra: str | None = None,
    ) -> list[dict]:
        """date_where は date_col に対する WHERE 条件（例: "= DATE('2026-05-09')" や "IN (...)"）。"""
        null_exprs = ", ".join(f"COUNTIF(`{c}` IS NULL) AS nc{i}" for i, c in enumerate(columns))
        select_dim = f"`{dimension_col}` AS dimension," if dimension_col else "CAST(NULL AS STRING) AS dimension,"
        group_by = f"GROUP BY `{dimension_col}`" if dimension_col else ""
        extra = f"AND ({where_extra})" if where_extra else ""

        query = f"""
        SELECT {select_dim} COUNT(*) AS row_count, {null_exprs}
        FROM `{self._project_id}.{self._dataset}.{self._table}`
        WHERE `{self._date_col}` {date_where}
        {extra}
        {group_by}
        """
        results = []
        for row in self._client.query(query).result():
            rc = row["row_count"]
            dim = row["dimension"]
            for i, col in enumerate(columns):
                nc = row[f"nc{i}"]
                results.append({
                    "column_name": col,
                    "null_rate": nc / rc if rc > 0 else None,
                    "dimension": dim,
                })
        return results

    def _null_rates_for_period(
        self,
        columns: list[str],
        lookback_days: int,
        dimension_col: str | None = None,
        where_extra: str | None = None,
    ) -> list[dict]:
        """直近 lookback_days 日の combined NULL 率を返す。"""
        date_where = f"""IN (
          SELECT DISTINCT `{self._date_col}`
          FROM `{self._project_id}.{self._dataset}.{self._table}`
          WHERE `{self._date_col}` < DATE('{self._check_date}')
          ORDER BY `{self._date_col}` DESC
          LIMIT {lookback_days}
        )"""
        return self._null_rates(columns, date_where, dimension_col=dimension_col, where_extra=where_extra)

    def _warn(self, message: str) -> None:
        """検知した品質問題を蓄積する。Discord 通知は末尾の notify_if_failed() で集約送信する。
        個別の詳細は WARNING ログに残し、Cloud Logging から事後調査できるようにする。
        """
        self._failures.append(message)
        logging.warning("[%s] quality warning: %s", self._pipeline_name, message)
