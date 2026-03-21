"""LangSmith diagnostics – pulls recent traces, errors, and feedback via REST API.

Works without the langsmith SDK so it can run in any environment.
Requires LANGCHAIN_API_KEY (or LANGSMITH_API_KEY) in env.
"""

import os
import json
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Optional

BASE_URL = "https://api.smith.langchain.com/api/v1"
DEFAULT_PROJECT = os.environ.get("LANGCHAIN_PROJECT", "CaelynAI")


def _api_key() -> str:
    key = os.environ.get("LANGCHAIN_API_KEY") or os.environ.get("LANGSMITH_API_KEY")
    if not key:
        raise RuntimeError("No LANGCHAIN_API_KEY or LANGSMITH_API_KEY set")
    return key


def _get(path: str, params: Optional[dict] = None, timeout: int = 15) -> dict:
    url = f"{BASE_URL}{path}"
    if params:
        qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items() if v is not None)
        url = f"{url}?{qs}"
    req = urllib.request.Request(url, headers={"x-api-key": _api_key()})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _post(path: str, body: dict, timeout: int = 30) -> dict:
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={
        "x-api-key": _api_key(),
        "Content-Type": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _get_session_id(project_name: str = DEFAULT_PROJECT) -> Optional[str]:
    """Look up project/session ID by name."""
    try:
        sessions = _get("/sessions", {"name": project_name})
        if isinstance(sessions, list) and sessions:
            return sessions[0].get("id")
    except Exception:
        pass
    return None


def get_recent_runs(
    project_name: str = DEFAULT_PROJECT,
    hours: int = 24,
    error_only: bool = False,
    limit: int = 20,
) -> list:
    """Fetch recent runs."""
    session_id = _get_session_id(project_name)
    if not session_id:
        return [{"_fetch_error": f"Project '{project_name}' not found"}]

    start_time = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    body = {
        "session": [session_id],
        "is_root": True,
        "limit": limit,
    }
    if error_only:
        body["error"] = True
    body["start_time"] = start_time

    try:
        result = _post("/runs/query", body)
        return result.get("runs", [])
    except Exception as e:
        return [{"_fetch_error": str(e)}]


def get_run_detail(run_id: str) -> dict:
    """Get full detail for a single run including I/O."""
    return _get(f"/runs/{run_id}")


def get_feedback(run_ids: list) -> list:
    """Get feedback for a list of run IDs."""
    if not run_ids:
        return []
    try:
        params = {"run_ids": ",".join(run_ids)}
        return _get("/feedback", params)
    except Exception:
        return []


def diagnose(hours: int = 24, limit: int = 20, project_name: str = DEFAULT_PROJECT) -> dict:
    """
    Full diagnostic report: recent runs, errors, and summary stats.
    This is the main entry point for the debug endpoint.
    """
    report = {
        "project": project_name,
        "window_hours": hours,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "errors": [],
        "recent_runs": [],
        "summary": {},
    }

    try:
        # Fetch errors
        error_runs = get_recent_runs(project_name, hours=hours, error_only=True, limit=limit)
        report["errors"] = [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "error": r.get("error"),
                "start_time": r.get("start_time"),
                "tokens": r.get("total_tokens"),
                "tags": r.get("tags"),
            }
            for r in error_runs
            if not r.get("_fetch_error")
        ]
        if error_runs and error_runs[0].get("_fetch_error"):
            report["_error_fetch_error"] = error_runs[0]["_fetch_error"]

        # Fetch all recent runs
        all_runs = get_recent_runs(project_name, hours=hours, error_only=False, limit=limit)
        report["recent_runs"] = [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "run_type": r.get("run_type"),
                "status": r.get("status"),
                "error": r.get("error"),
                "start_time": r.get("start_time"),
                "end_time": r.get("end_time"),
                "total_tokens": r.get("total_tokens"),
                "feedback_stats": r.get("feedback_stats"),
                "latency_s": r.get("latency") or _calc_latency(r),
                "tags": r.get("tags"),
            }
            for r in all_runs
            if not r.get("_fetch_error")
        ]

        # Summary
        total = len(report["recent_runs"])
        errors = sum(1 for r in report["recent_runs"] if r.get("error"))
        tokens = sum(r.get("total_tokens") or 0 for r in report["recent_runs"])
        latencies = [r["latency_s"] for r in report["recent_runs"] if r.get("latency_s")]

        report["summary"] = {
            "total_runs": total,
            "error_count": errors,
            "error_rate": f"{errors/total*100:.1f}%" if total else "N/A",
            "total_tokens": tokens,
            "avg_latency_s": round(sum(latencies) / len(latencies), 2) if latencies else None,
            "max_latency_s": round(max(latencies), 2) if latencies else None,
        }

    except Exception as e:
        report["_diagnostic_error"] = str(e)

    return report


def _calc_latency(run: dict) -> Optional[float]:
    try:
        start = datetime.fromisoformat(run["start_time"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(run["end_time"].replace("Z", "+00:00"))
        return round((end - start).total_seconds(), 2)
    except Exception:
        return None
