from libs.config import jquants_lake_bucket, jquants_staging_dataset
from libs.utils import get_env_name, get_logical_date, get_project_id
from libs.utils_bigquery import load_gcs_parquet_to_bq

# ---- 新しいパイプライン作成時にここだけ変更する -------------------------
PIPELINE_NAME = "jquants_fins_summary"
GCS_OBJECT    = "fins_summary/dt={date}/fins_summary.parquet"
BUCKET_FN     = jquants_lake_bucket      # (project_id, env_name) -> str
DATASET_FN    = jquants_staging_dataset  # (env_name) -> str
BQ_TABLE      = "fins_summary"

# API は数値フィールドを文字列で返すため、BQ ロード時に型キャストする。
# API の型を信用せず SAFE_CAST / SAFE.PARSE_DATE を使用する。
# DiscDate は整数形式（20260309）と文字列形式（"2026-03-09"）の両方を考慮する。
TRANSFORM_SQL = """
SELECT
  COALESCE(
    SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(DiscDate AS STRING)),
    SAFE.PARSE_DATE('%Y%m%d',   SAFE_CAST(DiscDate AS STRING))
  )                                                        AS DiscDate,
  SAFE_CAST(DiscTime   AS STRING)                          AS DiscTime,
  SAFE_CAST(Code       AS STRING)                          AS Code,
  SAFE_CAST(DiscNo     AS STRING)                          AS DiscNo,
  SAFE_CAST(DocType    AS STRING)                          AS DocType,
  SAFE_CAST(CurPerType AS STRING)                          AS CurPerType,
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(CurPerSt AS STRING)) AS CurPerSt,
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(CurPerEn AS STRING)) AS CurPerEn,
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(CurFYSt  AS STRING)) AS CurFYSt,
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(CurFYEn  AS STRING)) AS CurFYEn,
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(NxtFYSt  AS STRING)) AS NxtFYSt,
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(NxtFYEn  AS STRING)) AS NxtFYEn,
  SAFE_CAST(Sales  AS FLOAT64) AS Sales,
  SAFE_CAST(OP     AS FLOAT64) AS OP,
  SAFE_CAST(OdP    AS FLOAT64) AS OdP,
  SAFE_CAST(NP     AS FLOAT64) AS NP,
  SAFE_CAST(EPS    AS FLOAT64) AS EPS,
  SAFE_CAST(DEPS   AS FLOAT64) AS DEPS,
  SAFE_CAST(TA     AS FLOAT64) AS TA,
  SAFE_CAST(Eq     AS FLOAT64) AS Eq,
  SAFE_CAST(EqAR   AS FLOAT64) AS EqAR,
  SAFE_CAST(BPS    AS FLOAT64) AS BPS,
  SAFE_CAST(CFO    AS FLOAT64) AS CFO,
  SAFE_CAST(CFI    AS FLOAT64) AS CFI,
  SAFE_CAST(CFF    AS FLOAT64) AS CFF,
  SAFE_CAST(CashEq AS FLOAT64) AS CashEq,
  SAFE_CAST(Div1Q          AS FLOAT64) AS Div1Q,
  SAFE_CAST(Div2Q          AS FLOAT64) AS Div2Q,
  SAFE_CAST(Div3Q          AS FLOAT64) AS Div3Q,
  SAFE_CAST(DivFY          AS FLOAT64) AS DivFY,
  SAFE_CAST(DivAnn         AS FLOAT64) AS DivAnn,
  SAFE_CAST(DivUnit        AS FLOAT64) AS DivUnit,
  SAFE_CAST(DivTotalAnn    AS FLOAT64) AS DivTotalAnn,
  SAFE_CAST(PayoutRatioAnn AS FLOAT64) AS PayoutRatioAnn,
  SAFE_CAST(FDiv1Q          AS FLOAT64) AS FDiv1Q,
  SAFE_CAST(FDiv2Q          AS FLOAT64) AS FDiv2Q,
  SAFE_CAST(FDiv3Q          AS FLOAT64) AS FDiv3Q,
  SAFE_CAST(FDivFY          AS FLOAT64) AS FDivFY,
  SAFE_CAST(FDivAnn         AS FLOAT64) AS FDivAnn,
  SAFE_CAST(FDivUnit        AS FLOAT64) AS FDivUnit,
  SAFE_CAST(FDivTotalAnn    AS FLOAT64) AS FDivTotalAnn,
  SAFE_CAST(FPayoutRatioAnn AS FLOAT64) AS FPayoutRatioAnn,
  SAFE_CAST(NxFDiv1Q          AS FLOAT64) AS NxFDiv1Q,
  SAFE_CAST(NxFDiv2Q          AS FLOAT64) AS NxFDiv2Q,
  SAFE_CAST(NxFDiv3Q          AS FLOAT64) AS NxFDiv3Q,
  SAFE_CAST(NxFDivFY          AS FLOAT64) AS NxFDivFY,
  SAFE_CAST(NxFDivAnn         AS FLOAT64) AS NxFDivAnn,
  SAFE_CAST(NxFDivUnit        AS FLOAT64) AS NxFDivUnit,
  SAFE_CAST(NxFPayoutRatioAnn AS FLOAT64) AS NxFPayoutRatioAnn,
  SAFE_CAST(FSales2Q AS FLOAT64) AS FSales2Q,
  SAFE_CAST(FOP2Q    AS FLOAT64) AS FOP2Q,
  SAFE_CAST(FOdP2Q   AS FLOAT64) AS FOdP2Q,
  SAFE_CAST(FNP2Q    AS FLOAT64) AS FNP2Q,
  SAFE_CAST(FEPS2Q   AS FLOAT64) AS FEPS2Q,
  SAFE_CAST(NxFSales2Q AS FLOAT64) AS NxFSales2Q,
  SAFE_CAST(NxFOP2Q    AS FLOAT64) AS NxFOP2Q,
  SAFE_CAST(NxFOdP2Q   AS FLOAT64) AS NxFOdP2Q,
  SAFE_CAST(NxFNp2Q    AS FLOAT64) AS NxFNp2Q,
  SAFE_CAST(NxFEPS2Q   AS FLOAT64) AS NxFEPS2Q,
  SAFE_CAST(FSales AS FLOAT64) AS FSales,
  SAFE_CAST(FOP    AS FLOAT64) AS FOP,
  SAFE_CAST(FOdP   AS FLOAT64) AS FOdP,
  SAFE_CAST(FNP    AS FLOAT64) AS FNP,
  SAFE_CAST(FEPS   AS FLOAT64) AS FEPS,
  SAFE_CAST(NxFSales AS FLOAT64) AS NxFSales,
  SAFE_CAST(NxFOP    AS FLOAT64) AS NxFOP,
  SAFE_CAST(NxFOdP   AS FLOAT64) AS NxFOdP,
  SAFE_CAST(NxFNp    AS FLOAT64) AS NxFNp,
  SAFE_CAST(NxFEPS   AS FLOAT64) AS NxFEPS,
  SAFE_CAST(MatChgSub  AS STRING) AS MatChgSub,
  SAFE_CAST(SigChgInC  AS STRING) AS SigChgInC,
  SAFE_CAST(ChgByASRev AS STRING) AS ChgByASRev,
  SAFE_CAST(ChgNoASRev AS STRING) AS ChgNoASRev,
  SAFE_CAST(ChgAcEst   AS STRING) AS ChgAcEst,
  SAFE_CAST(RetroRst   AS STRING) AS RetroRst,
  SAFE_CAST(ShOutFY AS FLOAT64) AS ShOutFY,
  SAFE_CAST(TrShFY  AS FLOAT64) AS TrShFY,
  SAFE_CAST(AvgSh   AS FLOAT64) AS AvgSh,
  SAFE_CAST(NCSales  AS FLOAT64) AS NCSales,
  SAFE_CAST(NCOP     AS FLOAT64) AS NCOP,
  SAFE_CAST(NCOdP    AS FLOAT64) AS NCOdP,
  SAFE_CAST(NCNP     AS FLOAT64) AS NCNP,
  SAFE_CAST(NCEPS    AS FLOAT64) AS NCEPS,
  SAFE_CAST(NCTA     AS FLOAT64) AS NCTA,
  SAFE_CAST(NCEq     AS FLOAT64) AS NCEq,
  SAFE_CAST(NCEqAR   AS FLOAT64) AS NCEqAR,
  SAFE_CAST(NCBPS    AS FLOAT64) AS NCBPS,
  SAFE_CAST(FNCSales2Q AS FLOAT64) AS FNCSales2Q,
  SAFE_CAST(FNCOP2Q    AS FLOAT64) AS FNCOP2Q,
  SAFE_CAST(FNCOdP2Q   AS FLOAT64) AS FNCOdP2Q,
  SAFE_CAST(FNCNP2Q    AS FLOAT64) AS FNCNP2Q,
  SAFE_CAST(FNCEPS2Q   AS FLOAT64) AS FNCEPS2Q,
  SAFE_CAST(NxFNCSales2Q AS FLOAT64) AS NxFNCSales2Q,
  SAFE_CAST(NxFNCOP2Q    AS FLOAT64) AS NxFNCOP2Q,
  SAFE_CAST(NxFNCOdP2Q   AS FLOAT64) AS NxFNCOdP2Q,
  SAFE_CAST(NxFNCNP2Q    AS FLOAT64) AS NxFNCNP2Q,
  SAFE_CAST(NxFNCEPS2Q   AS FLOAT64) AS NxFNCEPS2Q,
  SAFE_CAST(FNCSales AS FLOAT64) AS FNCSales,
  SAFE_CAST(FNCOP    AS FLOAT64) AS FNCOP,
  SAFE_CAST(FNCOdP   AS FLOAT64) AS FNCOdP,
  SAFE_CAST(FNCNP    AS FLOAT64) AS FNCNP,
  SAFE_CAST(FNCEPS   AS FLOAT64) AS FNCEPS,
  SAFE_CAST(NxFNCSales AS FLOAT64) AS NxFNCSales,
  SAFE_CAST(NxFNCOP    AS FLOAT64) AS NxFNCOP,
  SAFE_CAST(NxFNCOdP   AS FLOAT64) AS NxFNCOdP,
  SAFE_CAST(NxFNCNP    AS FLOAT64) AS NxFNCNP,
  SAFE_CAST(NxFNCEPS   AS FLOAT64) AS NxFNCEPS
FROM {source}
"""
# -------------------------------------------------------------------------


def main() -> None:
    env_name = get_env_name()
    project_id = get_project_id()
    logical_date, _ = get_logical_date()

    gcs_uri  = f"gs://{BUCKET_FN(project_id, env_name)}/{GCS_OBJECT.format(date=logical_date.isoformat())}"
    bq_dest  = f"{project_id}.{DATASET_FN(env_name)}.{BQ_TABLE}${logical_date.strftime('%Y%m%d')}"

    load_gcs_parquet_to_bq(project_id, env_name, gcs_uri, bq_dest, PIPELINE_NAME, logical_date, TRANSFORM_SQL, partition_field="DiscDate")


if __name__ == "__main__":
    main()
