"""Kubernetes Job runner for airflow-floe connector.

Supports runner_type=kubernetes_job from manifest runner definitions.
Builds a Kubernetes Job spec from manifest config, submits it, polls to
terminal state, and returns a normalized run payload with
backend_type=kubernetes.
"""

from __future__ import annotations

import time
import uuid
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
from .manifest import ManifestExecution, ManifestRunnerDefinition

_DEFAULT_POLL_INTERVAL_S = 10
_DEFAULT_TIMEOUT_S = 3600


def build_k8s_job_spec(
    job_name: str,
    cmd_args: list[str],
    runner: ManifestRunnerDefinition,
) -> dict[str, Any]:
    """Build a Kubernetes Job manifest dict from manifest runner config.

    Args:
        job_name: Unique job name (used as metadata.name and pod label).
        cmd_args: Full command list built from execution contract rendering.
            Split as: cmd_args[0] = binary (container command),
            cmd_args[1:] = arguments (container args).
        runner: Runner definition supplying image, namespace, and optional
            service_account, resources, env, secrets, command/args overrides,
            timeout_seconds, and ttl_seconds_after_finished.

    Returns:
        A dict representing the Kubernetes Job resource (batch/v1).

    Raises:
        ValueError: If required fields (image, namespace) are missing.
    """
    if not runner.image:
        raise ValueError("kubernetes_job runner requires a non-empty 'image' field")
    if not runner.namespace:
        raise ValueError("kubernetes_job runner requires a non-empty 'namespace' field")

    # Derive container command / args from execution contract, allowing
    # per-runner overrides from the manifest definition.
    container_cmd: list[str] = [cmd_args[0]] if cmd_args else []
    container_args: list[str] = list(cmd_args[1:]) if len(cmd_args) > 1 else []

    if runner.command is not None:
        container_cmd = runner.command
    if runner.args is not None:
        container_args = runner.args

    # Env vars: regular key/value first, then secret refs.
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

    container: dict[str, Any] = {
        "name": "floe-runner",
        "image": runner.image,
    }
    if container_cmd:
        container["command"] = container_cmd
    if container_args:
        container["args"] = container_args
    if env_vars:
        container["env"] = env_vars

    # Resource requests/limits derived from manifest resources block.
    if runner.resources is not None:
        resource_reqs: dict[str, Any] = {}
        if runner.resources.cpu is not None:
            resource_reqs.setdefault("requests", {})["cpu"] = runner.resources.cpu
            resource_reqs.setdefault("limits", {})["cpu"] = runner.resources.cpu
        if runner.resources.memory_mb is not None:
            mem_str = f"{runner.resources.memory_mb}Mi"
            resource_reqs.setdefault("requests", {})["memory"] = mem_str
            resource_reqs.setdefault("limits", {})["memory"] = mem_str
        if resource_reqs:
            container["resources"] = resource_reqs

    pod_spec: dict[str, Any] = {
        "containers": [container],
        "restartPolicy": "Never",
    }
    if runner.service_account:
        pod_spec["serviceAccountName"] = runner.service_account

    job_spec: dict[str, Any] = {
        "template": {
            "metadata": {"labels": {"app": "floe-runner", "floe-job": job_name}},
            "spec": pod_spec,
        },
        "backoffLimit": 0,
    }
    if runner.ttl_seconds_after_finished is not None:
        job_spec["ttlSecondsAfterFinished"] = runner.ttl_seconds_after_finished
    if runner.timeout_seconds is not None:
        job_spec["activeDeadlineSeconds"] = runner.timeout_seconds

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": runner.namespace,
            "labels": {"app": "floe-runner"},
        },
        "spec": job_spec,
    }


def run_kubernetes_job(
    cmd_args: list[str],
    config_path: str,
    entities: list[str] | None,
    *,
    runner: ManifestRunnerDefinition,
    jobs_api: Any = None,
    core_api: Any = None,
) -> dict[str, Any]:
    """Submit a Kubernetes Job, poll until terminal, return normalized payload.

    Args:
        cmd_args: Pre-built command list from execution contract rendering
            (via FloeRunHook.build_args).
        config_path: Path to Floe config file (recorded in payload).
        entities: Entity names being run (recorded in payload when singular).
        runner: Kubernetes runner definition from manifest.
        jobs_api: Injectable BatchV1Api client for tests; auto-created if None.
        core_api: Injectable CoreV1Api client for log collection; derived from
            jobs_api if None.

    Returns:
        Normalized run payload dict with schema=floe.airflow.run.v1 and
        backend_type=kubernetes.
    """
    namespace = runner.namespace
    if not namespace:
        raise ValueError("kubernetes_job runner requires a non-empty 'namespace' field")

    job_name = _make_job_name(entities)
    job_spec = build_k8s_job_spec(job_name, cmd_args, runner)

    poll_interval = runner.poll_interval_seconds or _DEFAULT_POLL_INTERVAL_S
    timeout = runner.timeout_seconds or _DEFAULT_TIMEOUT_S

    if jobs_api is None:
        jobs_api = _make_batch_client()
    if core_api is None:
        core_api = _make_core_client(jobs_api)

    jobs_api.create_namespaced_job(namespace=namespace, body=job_spec)

    deadline = time.monotonic() + timeout
    current_status = STATUS_SUBMITTED
    last_job: Any = None

    while True:
        last_job = jobs_api.read_namespaced_job(name=job_name, namespace=namespace)
        current_status = map_k8s_job_status(last_job)

        if current_status in (STATUS_SUCCEEDED, STATUS_FAILED, STATUS_TIMEOUT, STATUS_CANCELED):
            break

        if time.monotonic() >= deadline:
            current_status = STATUS_TIMEOUT
            break

        time.sleep(poll_interval)

    logs = _collect_job_logs(core_api, job_name, namespace)
    pod_ctx = _collect_pod_context(core_api, job_name, namespace)

    # Parse Floe run_finished event from container stdout logs.
    # For failure/timeout cases the event may be absent — fall back gracefully.
    run_finished: dict[str, Any] = {}
    if logs:
        try:
            from .runtime import parse_run_finished

            run_finished = parse_run_finished(logs)
        except (ValueError, Exception):
            pass

    run_id = run_finished.get("run_id") or job_name
    floe_status = run_finished.get("status") or current_status

    payload: dict[str, Any] = {
        "schema": "floe.airflow.run.v1",
        "run_id": run_id,
        "status": floe_status,
        "exit_code": run_finished.get("exit_code"),
        "files": run_finished.get("files", 0),
        "rows": run_finished.get("rows", 0),
        "accepted": run_finished.get("accepted", 0),
        "rejected": run_finished.get("rejected", 0),
        "warnings": run_finished.get("warnings", 0),
        "errors": run_finished.get("errors", 0),
        "summary_uri": run_finished.get("summary_uri"),
        "config_uri": config_path,
        "floe_log_schema": "floe.log.v1",
        "finished_at_ts_ms": run_finished.get("ts_ms"),
        "backend_type": "kubernetes",
        "backend_run_id": job_name,
        "backend_status": current_status,
        "backend_metadata": {
            "namespace": namespace,
            "job_failure_reason": extract_k8s_job_failure_reason(last_job),
            "pod_name": pod_ctx.get("pod_name"),
            "container_state": pod_ctx.get("container_state"),
            "container_reason": pod_ctx.get("container_reason"),
            "container_exit_code": pod_ctx.get("container_exit_code"),
        },
    }

    payload["failure_reason"] = _resolve_failure_reason(
        status=current_status,
        job=last_job,
        pod_ctx=pod_ctx,
    )
    if "report_base" in run_finished:
        payload["report_base"] = run_finished["report_base"]
    if "entity_report_uris" in run_finished:
        payload["entity_report_uris"] = run_finished["entity_report_uris"]
    if entities and len(entities) == 1:
        payload["entity"] = entities[0]
    return payload


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_job_name(entities: list[str] | None) -> str:
    suffix = uuid.uuid4().hex[:8]
    if entities and len(entities) == 1:
        safe = entities[0].replace("_", "-").lower()[:20]
        return f"floe-{safe}-{suffix}"
    return f"floe-run-{suffix}"


def _make_batch_client() -> Any:
    try:
        from kubernetes import client as k8s_client  # type: ignore[import]
        from kubernetes import config as k8s_config  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "kubernetes Python package is required for kubernetes_job runner: "
            "pip install kubernetes"
        ) from exc
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()
    return k8s_client.BatchV1Api()


def _make_core_client(batch_client: Any) -> Any:
    try:
        from kubernetes import client as k8s_client  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "kubernetes Python package is required for kubernetes_job runner: "
            "pip install kubernetes"
        ) from exc
    api_client = getattr(batch_client, "api_client", None)
    if api_client is not None:
        return k8s_client.CoreV1Api(api_client)
    return k8s_client.CoreV1Api()


def _collect_job_logs(core_api: Any, job_name: str, namespace: str) -> str:
    """Collect stdout logs from the first pod of the completed job."""
    try:
        pods = core_api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"floe-job={job_name}",
        )
        items = getattr(pods, "items", None) or pods.get("items", [])
        if not items:
            return ""
        pod = items[0]
        pod_name = (
            pod.metadata.name
            if hasattr(pod, "metadata")
            else pod.get("metadata", {}).get("name", "")
        )
        if not pod_name:
            return ""
        logs = core_api.read_namespaced_pod_log(name=pod_name, namespace=namespace)
        return logs or ""
    except Exception:
        return ""


def _collect_pod_context(core_api: Any, job_name: str, namespace: str) -> dict[str, Any]:
    try:
        pods = core_api.list_namespaced_pod(namespace=namespace, label_selector=f"floe-job={job_name}")
        items = getattr(pods, "items", None) or pods.get("items", [])
        if not items:
            return {}
        pod = items[0]
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
