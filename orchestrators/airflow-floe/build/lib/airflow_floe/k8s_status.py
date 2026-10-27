from __future__ import annotations

from typing import Any

STATUS_SUBMITTED = "submitted"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_TIMEOUT = "timeout"
STATUS_CANCELED = "canceled"


def map_k8s_job_status(job: Any) -> str:
    if job is None:
        return STATUS_CANCELED

    status = job.get("status") if isinstance(job, dict) else getattr(job, "status", None)
    if status is None:
        return STATUS_SUBMITTED

    active = _read(status, "active", 0)
    succeeded = _read(status, "succeeded", 0)
    failed = _read(status, "failed", 0)
    conditions = _read(status, "conditions", []) or []

    for condition in conditions:
        cond_type = _read(condition, "type", "")
        cond_status = _read(condition, "status", "")
        reason = _read(condition, "reason", "")

        if cond_type == "Complete" and cond_status == "True":
            return STATUS_SUCCEEDED
        if cond_type == "Failed" and cond_status == "True":
            if reason == "DeadlineExceeded":
                return STATUS_TIMEOUT
            return STATUS_FAILED

    if succeeded > 0:
        return STATUS_SUCCEEDED
    if failed > 0:
        return STATUS_FAILED
    if active > 0:
        return STATUS_RUNNING
    return STATUS_SUBMITTED


def extract_k8s_job_failure_reason(job: Any) -> str | None:
    if job is None:
        return "JobNotFound"

    status = job.get("status") if isinstance(job, dict) else getattr(job, "status", None)
    if status is None:
        return None

    conditions = _read(status, "conditions", []) or []
    for condition in conditions:
        cond_type = _read(condition, "type", "")
        cond_status = _read(condition, "status", "")
        if cond_type != "Failed" or cond_status != "True":
            continue
        reason = _read(condition, "reason", "")
        message = _read(condition, "message", "")
        if reason and message:
            return f"{reason}: {message}"
        if reason:
            return reason
        if message:
            return message
    return None


def _read(obj: Any, key: str, default: Any = None) -> Any:
    if hasattr(obj, key):
        value = getattr(obj, key)
        return default if value is None else value
    if isinstance(obj, dict):
        value = obj.get(key, default)
        return default if value is None else value
    return default
