import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Set

import requests
from fastapi import FastAPI, Request, HTTPException
from google.auth import default
from google.auth.transport.requests import Request as GoogleAuthRequest

app = FastAPI(title="wf-dispatcher")

JST = timezone(timedelta(hours=9))

def schedule_time_to_logical_date_jst(schedule_time_rfc3339: str) -> str:
    # "2026-02-02T00:00:00Z" -> "2026-02-02"
    dt_utc = datetime.fromisoformat(schedule_time_rfc3339.replace("Z", "+00:00"))
    return dt_utc.astimezone(JST).date().isoformat()

def get_access_token() -> str:
    creds, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(GoogleAuthRequest())
    return creds.token

def start_workflow_execution(
    project_id: str,
    region: str,
    workflow_name: str,
    argument_obj: dict,
    access_token: str,
) -> dict:
    url = (
        "https://workflowexecutions.googleapis.com/v1/"
        f"projects/{project_id}/locations/{region}/workflows/{workflow_name}/executions"
    )

    body = {"argument": json.dumps(argument_obj, ensure_ascii=False)}
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=30,
    )
    if resp.status_code >= 300:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start workflow: {resp.status_code} {resp.text}",
        )
    return resp.json()

def parse_allowlist_env() -> Optional[Set[str]]:
    """
    WF_ALLOWLIST が空/未設定なら制限なし（=全部許可）。
    例: WF_ALLOWLIST="pipeline_jquants_ohlcv,pipeline_jquants_financials"
    """
    raw = os.getenv("WF_ALLOWLIST", "").strip()
    if not raw:
        return None
    return {x.strip() for x in raw.split(",") if x.strip()}

def assert_workflow_allowed(workflow_name: str):
    allowlist = parse_allowlist_env()
    if allowlist is None:
        return  # 制限なし

    if workflow_name not in allowlist:
        raise HTTPException(status_code=403, detail=f"Workflow not allowed: {workflow_name}")

def iter_dates(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/dates")
def dates(start: str, end: str, max_days: int = 366):
    try:
        start_d = datetime.strptime(start, "%Y-%m-%d").date()
        end_d = datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")
    if end_d < start_d:
        raise HTTPException(status_code=400, detail="end must be >= start")
    days = (end_d - start_d).days + 1
    if days > max_days:
        raise HTTPException(status_code=400, detail=f"too many days: {days} > {max_days}")
    return {"dates": [d.isoformat() for d in iter_dates(start_d, end_d)]}

@app.get("/trigger/{workflow_name}")
async def trigger(workflow_name: str, request: Request):
    # Cloud Scheduler が付与するヘッダ
    sched_time = request.headers.get("X-CloudScheduler-ScheduleTime")
    if not sched_time:
        raise HTTPException(
            status_code=400,
            detail="Missing X-CloudScheduler-ScheduleTime header (expected Cloud Scheduler call).",
        )

    assert_workflow_allowed(workflow_name)

    logical_date = schedule_time_to_logical_date_jst(sched_time)

    project_id = os.environ["PROJECT_ID"]
    region = os.environ["REGION"]

    access_token = get_access_token()
    execution = start_workflow_execution(
        project_id=project_id,
        region=region,
        workflow_name=workflow_name,
        argument_obj={"logical_date": logical_date},
        access_token=access_token,
    )

    return {
        "workflow": workflow_name,
        "schedule_time": sched_time,
        "logical_date": logical_date,
        "execution": execution,
    }
