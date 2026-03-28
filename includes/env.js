// 既存の命名規則（libs/config.py）に合わせたデータセット名を提供する
// staging_jquants_{env} → mart_{env} の変換をここで一元管理する
const env          = dataform.projectConfig.vars.env;
const logical_date = dataform.projectConfig.vars.logical_date;

const sourceDataset = "staging_jquants_prod"; // transform のソースは常に prod データを参照
const martDataset   = "mart_" + env;

module.exports = { env, sourceDataset, martDataset, logical_date };
