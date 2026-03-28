import logging

import pandas as pd

from libs.config import jquants_lake_bucket
from libs.utils import access_secret, dataframe_to_parquet_bytes, get_env_name, get_logical_date, get_project_id, upload_bytes_to_gcs
from libs.utils_jquants import fetch_jquants_fins_summary

# ---- 新しいパイプライン作成時にここだけ変更する -------------------------
GCS_OBJECT = "fins_summary/dt={date}/fins_summary.parquet"
BUCKET_FN  = jquants_lake_bucket  # (project_id, env_name) -> str

def fetch_records(project_id: str, date_str: str) -> list[dict]:
    api_key = access_secret("jquants-api-key", project_id)
    return list(fetch_jquants_fins_summary(date_str, api_key))
# -------------------------------------------------------------------------


# 財務情報は取引日でも開示ゼロの日があるため、0件は正常ケース。
# エラーにせず空の Parquet をアップロードする。
_EXPECTED_COLUMNS = [
    "DiscDate", "DiscTime", "Code", "DiscNo", "DocType",
    "CurPerType", "CurPerSt", "CurPerEn", "CurFYSt", "CurFYEn",
    "NxtFYSt", "NxtFYEn", "Sales", "OP", "OdP", "NP", "EPS", "DEPS",
    "TA", "Eq", "EqAR", "BPS", "CFO", "CFI", "CFF", "CashEq",
    "Div1Q", "Div2Q", "Div3Q", "DivFY", "DivAnn", "DivUnit", "DivTotalAnn", "PayoutRatioAnn",
    "FDiv1Q", "FDiv2Q", "FDiv3Q", "FDivFY", "FDivAnn", "FDivUnit", "FDivTotalAnn", "FPayoutRatioAnn",
    "NxFDiv1Q", "NxFDiv2Q", "NxFDiv3Q", "NxFDivFY", "NxFDivAnn", "NxFDivUnit", "NxFPayoutRatioAnn",
    "FSales2Q", "FOP2Q", "FOdP2Q", "FNP2Q", "FEPS2Q",
    "NxFSales2Q", "NxFOP2Q", "NxFOdP2Q", "NxFNp2Q", "NxFEPS2Q",
    "FSales", "FOP", "FOdP", "FNP", "FEPS",
    "NxFSales", "NxFOP", "NxFOdP", "NxFNp", "NxFEPS",
    "MatChgSub", "SigChgInC", "ChgByASRev", "ChgNoASRev", "ChgAcEst", "RetroRst",
    "ShOutFY", "TrShFY", "AvgSh",
    "NCSales", "NCOP", "NCOdP", "NCNP", "NCEPS", "NCTA", "NCEq", "NCEqAR", "NCBPS",
    "FNCSales2Q", "FNCOP2Q", "FNCOdP2Q", "FNCNP2Q", "FNCEPS2Q",
    "NxFNCSales2Q", "NxFNCOP2Q", "NxFNCOdP2Q", "NxFNCNP2Q", "NxFNCEPS2Q",
    "FNCSales", "FNCOP", "FNCOdP", "FNCNP", "FNCEPS",
    "NxFNCSales", "NxFNCOP", "NxFNCOdP", "NxFNCNP", "NxFNCEPS",
]

def main() -> None:
    logging.basicConfig(level=logging.INFO)
    env_name = get_env_name()
    project_id = get_project_id()
    logical_date, logical_date_str = get_logical_date()
    logging.info("Job executed as one at %s", logical_date_str)

    records = fetch_records(project_id, logical_date_str)
    df = pd.DataFrame(records) if records else pd.DataFrame(columns=_EXPECTED_COLUMNS)

    bucket = BUCKET_FN(project_id, env_name)
    object_path = GCS_OBJECT.format(date=logical_date.isoformat())
    upload_bytes_to_gcs(bucket, object_path, dataframe_to_parquet_bytes(df))

    logging.info("Fetched fins summary %s", {
        "logical_date": logical_date.isoformat(),
        "records": len(records),
        "bucket": bucket,
        "object_path": object_path,
    })


if __name__ == "__main__":
    main()
