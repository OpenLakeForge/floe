from __future__ import annotations

import logging
import time
import uuid
from dataclasses import replace
from typing import Any

from .k8s_status import (
    STATUS_CANCELED,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SUBMITTED,
    STATUS_SUCCEEDED,
    STATUS_TIMEOUT,
    extract_k8s_job_failure_reason,
    map_k8s_job_status,
)
from .manifest import ManifestRunnerDefinition
from .runner import RunResult

_log = logging.getLogger(__name__)

_DEFAULT_POLL_INTERVAL_S = 10
_DEFAULT_TIMEOUT_S = 3600


def run_kubernetes_job(
    cmd_args: list[str],
    *,
    entity: str,
    runner: ManifestRunnerDefinition,
    jobs_api: Any = None,
    core_api: Any = None,
) -> RunResult:
    namespace = runner.namespace
    if not namespace:
        raise ValueError("kubernetes_job runner requires a non-empty 'namespace' field")

    job_name = _make_job_name(entity)
    job_spec = build_k8s_job_spec(job_name, cmd_args, runner)

    poll_interval = runner.poll_interval_seconds or _DEFAULT_POLL_INTERVAL_S
    timeout = runner.timeout_seconds or _DEFAULT_TIMEOUT_S

    if jobs_api is None:
        jobs_api = _make_batch_client()
    if core_api is None:
        core_api = _make_core_client(jobs_api)

    jobs_api.create_namespaced_job(namespace=namespace, body=job_spec)

    deadline = time.monotonic() + timeout
    status = STATUS_SUBMITTED
    last_job: Any = None
    reached_final_state = False

    try:
        while True:
            last_job = jobs_api.read_namespaced_job(name=job_name, namespace=namespace)
            status = map_k8s_job_status(last_job)
            if status in (STATUS_SUCCEEDED, STATUS_FAILED, STATUS_TIMEOUT, STATUS_CANCELED):
                reached_final_state = True
                break
            if time.monotonic() >= deadline:
                status = STATUS_TIMEOUT
                break
            time.sleep(poll_interval)
    finally:
        # an interrupt, an error or the poll timeout leaves a Job nobody watches: it would keep writing
        if not reached_final_state:
            _delete_job(jobs_api, job_name, namespace)

    stdout = _collect_job_logs(core_api, job_name, namespace)
    pod_ctx = _collect_pod_context(core_api, job_name, namespace)

    failure_reason = _resolve_failure_reason(status=status, job=last_job, pod_ctx=pod_ctx)
    if status == STATUS_SUCCEEDED:
        exit_code = 0
    else:
        exit_code = int(pod_ctx.get("container_exit_code") or 1)

    backend_metadata: dict[str, str | int | None] = {
        "backend_type": "kubernetes",
        "backend_run_id": job_name,
        "namespace": namespace,
        "job_status": status,
        "job_failure_reason": extract_k8s_job_failure_reason(last_job),
        "pod_name": pod_ctx.get("pod_name"),
        "container_state": pod_ctx.get("container_state"),
        "container_reason": pod_ctx.get("container_reason"),
        "container_exit_code": pod_ctx.get("container_exit_code"),
    }

    return RunResult(
        stdout=stdout,
        stderr="",
        exit_code=exit_code,
        status=status,
        failure_reason=failure_reason,
        backend_metadata=backend_metadata,
    )


def _delete_job(jobs_api: Any, job_name: str, namespace: str) -> None:
    """Best effort, with its pods: a failure is logged and must not mask the error being raised."""
    try:
        jobs_api.delete_namespaced_job(job_name, namespace, propagation_policy="Background")
    except Exception:
        _log.warning("could not delete Kubernetes Job %s/%s", namespace, job_name, exc_info=True)


def build_k8s_job_spec(job_name: str, cmd_args: list[str], runner: ManifestRunnerDefinition) -> dict[str, Any]:
    if not runner.image:
        raise ValueError("kubernetes_job runner requires a non-empty 'image' field")
    if not runner.namespace:
        raise ValueError("kubernetes_job runner requires a non-empty 'namespace' field")

    container_cmd: list[str] = [cmd_args[0]] if cmd_args else []
    container_args: list[str] = list(cmd_args[1:]) if len(cmd_args) > 1 else []
    if runner.command is not None:
        container_cmd = runner.command
    if runner.args is not None:  # a prefix: replacing the list would drop the entity selection
        container_args = runner.args + container_args

    env_vars: list[dict[str, Any]] = []
    if runner.env:
        for k, v in runner.env.items():
            env_vars.append({"name": k, "value": v})
    if runner.secrets:
        for secret_ref in runner.secrets:
            env_vars.append(
                {
                    "name": secret_ref["name"],
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": secret_ref["secret_name"],
                            "key": secret_ref["key"],
                        }
                    },
                }
            )

    container: dict[str, Any] = {"name": "floe-runner", "image": runner.image}
    if container_cmd:
        container["command"] = container_cmd
    if container_args:
        container["args"] = container_args
    if env_vars:
        container["env"] = env_vars
    if runner.image_pull_policy:
        container["imagePullPolicy"] = runner.image_pull_policy
    if runner.security_context:
        container["securityContext"] = runner.security_context
    if runner.env_from:
        container["envFrom"] = runner.env_from
    requests, limits = _resource_requests(runner.resources), _resource_limits(runner.resources)
    if requests or limits:
        container["resources"] = {
            **({"requests": requests} if requests else {}),
            **({"limits": limits} if limits else {}),
        }

    pod_spec: dict[str, Any] = {"containers": [container], "restartPolicy": "Never"}
    if runner.service_account:
        pod_spec["serviceAccountName"] = runner.service_account
    if runner.node_selector:
        pod_spec["nodeSelector"] = runner.node_selector
    if runner.tolerations:
        pod_spec["tolerations"] = runner.tolerations

    # the defaults win: `floe-job` is how the runner finds the pod again
    pod_metadata: dict[str, Any] = {
        "labels": {**(runner.labels or {}), "app": "floe-runner", "floe-job": job_name}
    }
    if runner.annotations:
        pod_metadata["annotations"] = runner.annotations

    job_spec: dict[str, Any] = {
        "template": {
            "metadata": pod_metadata,
            "spec": pod_spec,
        },
        "backoffLimit": runner.backoff_limit or 0,
    }
    if runner.ttl_seconds_after_finished is not None:
        job_spec["ttlSecondsAfterFinished"] = runner.ttl_seconds_after_finished
    if runner.timeout_seconds is not None:
        job_spec["activeDeadlineSeconds"] = runner.timeout_seconds

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name, "namespace": runner.namespace, "labels": {"app": "floe-runner"}},
        "spec": job_spec,
    }


def _resource_requests(resources: dict[str, Any] | None) -> dict[str, str]:
    requests: dict[str, str] = {}
    if resources:
        if resources.get("cpu"):
            requests["cpu"] = str(resources["cpu"])
        if resources.get("memory_mb") is not None:
            requests["memory"] = f"{resources['memory_mb']}Mi"
    return requests


def _resource_limits(resources: dict[str, Any] | None) -> dict[str, str]:
    limits: dict[str, str] = {}
    if resources:
        if resources.get("cpu_limit"):
            limits["cpu"] = str(resources["cpu_limit"])
        if resources.get("memory_limit_mb") is not None:
            limits["memory"] = f"{resources['memory_limit_mb']}Mi"
    return limits


def sized_for_domain(runner: ManifestRunnerDefinition, domain: str | None) -> ManifestRunnerDefinition:
    """`domain_resources[domain]` replaces `resources` wholesale; a domain not listed keeps them."""
    sized = (runner.domain_resources or {}).get(domain or "")
    return replace(runner, resources=sized) if sized is not None else runner


def _make_job_name(entity: str) -> str:
    return f"floe-{entity.replace('_', '-').lower()[:20]}-{uuid.uuid4().hex[:8]}"


def _make_batch_client() -> Any:
    try:
        from kubernetes import client as k8s_client  # type: ignore[import]
        from kubernetes import config as k8s_config  # type: ignore[import]
    except ImportError as exc:
        raise ImportError("kubernetes Python package is required for kubernetes_job runner") from exc
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()
    return k8s_client.BatchV1Api()


def _make_core_client(batch_client: Any) -> Any:
    from kubernetes import client as k8s_client  # type: ignore[import]

    api_client = getattr(batch_client, "api_client", None)
    if api_client is not None:
        return k8s_client.CoreV1Api(api_client)
    return k8s_client.CoreV1Api()


def _outcome_pod(core_api: Any, job_name: str, namespace: str) -> Any:
    """With `backoff_limit` > 0 a Job has several pods: the one that succeeded, else the latest attempt."""
    pods = core_api.list_namespaced_pod(namespace=namespace, label_selector=f"floe-job={job_name}")
    items = getattr(pods, "items", None) or pods.get("items", [])
    if not items:
        return None

    def attempt(pod: Any) -> tuple[bool, str]:
        phase = _read(_read(pod, "status", {}), "phase", "")
        created = _read(_read(pod, "metadata", {}), "creation_timestamp", "")
        return phase == "Succeeded", str(created)

    return max(items, key=attempt)


def _collect_job_logs(core_api: Any, job_name: str, namespace: str) -> str:
    try:
        pod = _outcome_pod(core_api, job_name, namespace)
        if pod is None:
            return ""
        pod_name = pod.metadata.name if hasattr(pod, "metadata") else pod.get("metadata", {}).get("name", "")
        if not pod_name:
            return ""
        return core_api.read_namespaced_pod_log(name=pod_name, namespace=namespace) or ""
    except Exception:
        return ""


def _collect_pod_context(core_api: Any, job_name: str, namespace: str) -> dict[str, Any]:
    try:
        pod = _outcome_pod(core_api, job_name, namespace)
        if pod is None:
            return {}
        pod_name = _read(_read(pod, "metadata", {}), "name", None)
        statuses = _read(_read(pod, "status", {}), "container_statuses", []) or []
        if not statuses:
            return {"pod_name": pod_name}

        state = _read(statuses[0], "state", {})
        terminated = _read(state, "terminated", None)
        waiting = _read(state, "waiting", None)

        if terminated:
            return {
                "pod_name": pod_name,
                "container_state": "terminated",
                "container_reason": _read(terminated, "reason", None),
                "container_exit_code": _read(terminated, "exit_code", None),
            }
        if waiting:
            return {
                "pod_name": pod_name,
                "container_state": "waiting",
                "container_reason": _read(waiting, "reason", None),
                "container_exit_code": None,
            }
        return {
            "pod_name": pod_name,
            "container_state": "running",
            "container_reason": None,
            "container_exit_code": None,
        }
    except Exception:
        return {}


def _resolve_failure_reason(*, status: str, job: Any, pod_ctx: dict[str, Any]) -> str | None:
    if status == STATUS_SUCCEEDED:
        return None

    job_reason = extract_k8s_job_failure_reason(job)
    if status == STATUS_TIMEOUT and job_reason:
        return job_reason

    pod_reason = pod_ctx.get("container_reason")
    if isinstance(pod_reason, str) and pod_reason:
        if pod_reason in {"ImagePullBackOff", "ErrImagePull"}:
            return pod_reason
        exit_code = pod_ctx.get("container_exit_code")
        if exit_code is not None:
            return f"{pod_reason} (exit_code={exit_code})"
        return pod_reason

    if job_reason:
        return job_reason

    if status == STATUS_TIMEOUT:
        return "DeadlineExceeded"
    if status == STATUS_CANCELED:
        return "Canceled"
    return "KubernetesJobFailed"


def _read(obj: Any, key: str, default: Any = None) -> Any:
    if hasattr(obj, key):
        value = getattr(obj, key)
        return default if value is None else value
    if isinstance(obj, dict):
        value = obj.get(key, default)
        return default if value is None else value
    return default
