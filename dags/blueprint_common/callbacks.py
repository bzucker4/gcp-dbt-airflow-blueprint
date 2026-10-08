"""Alerting and artifact callbacks shared by blueprint DAGs.

> Public preview. Full implementation: https://payhip.com/b/YIq4E

Design rules the full implementation follows:
  * A callback must NEVER raise. A broken Slack webhook or a GCS hiccup must
    not mask the original task failure or fail a healthy task.
  * Only current Airflow context keys are used (`ti`, `run_id`, `logical_date`,
    `data_interval_end`); deprecated keys such as `execution_date` are avoided.
  * Provider imports happen lazily so DAG parsing stays fast.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

FULL_VERSION = "https://payhip.com/b/YIq4E"
Context = dict[str, Any]


def build_slack_failure_callback(*, severity: str = "high", mention: str | None = None) -> Callable[[Context], None]:
    """Return an `on_failure_callback` posting a Slack Block Kit alert (DAG, task, run ID,
    attempt, logical date, truncated error, button to the task log). Fires only after
    retries are exhausted, so every alert is a real terminal failure."""
    raise NotImplementedError(f"Included in the full blueprint: {FULL_VERSION}")


def build_slack_test_warning_callback() -> Callable[[Context], None]:
    """Return a Cosmos `on_warning_callback` that digests dbt tests finishing with severity=warn."""
    raise NotImplementedError(f"Included in the full blueprint: {FULL_VERSION}")


def upload_dbt_artifacts(project_dir: str, *, bucket_name: str, gcp_conn_id: str = "google_cloud_default", **_: Any) -> None:
    """Cosmos `callback`: persist run_results.json from the task's temporary project dir to
    gs://<bucket>/dbt-artifacts/<dag_id>/<ds>/<run_id>/<task_id>/try_<n>/ (best effort)."""
    raise NotImplementedError(f"Included in the full blueprint: {FULL_VERSION}")
