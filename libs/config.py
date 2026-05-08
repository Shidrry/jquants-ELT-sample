def jquants_lake_bucket(project_id: str, env_name: str) -> str:
    return f"lake-jquants-{project_id}-{env_name}"

def jquants_staging_dataset(env_name: str) -> str:
    return f"staging_jquants_{env_name}"

def mart_dataset(env_name: str) -> str:
    return f"mart_{env_name}"

def pipeline_metadata_dataset(env_name: str) -> str:
    return f"pipeline_metadata_{env_name}"

DATA_LOADS_TABLE = "data_loads"

JQUANTS_API_BASE = "https://api.jquants.com/v2"