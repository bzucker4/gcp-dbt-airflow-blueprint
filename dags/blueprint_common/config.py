"""Runtime configuration for blueprint DAGs.

Every value comes from Composer environment variables provisioned by
infrastructure/terraform_setup.tf, so the DAG code itself never contains
project IDs, bucket names or service account emails.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

DBT_PROJECT_NAME = "gcp_dbt_blueprint"
DBT_PROFILE_NAME = "gcp_dbt_blueprint"
# Fallback only; Terraform sets DBT_PY_REQUIREMENTS (';'-separated) on Composer.
DEFAULT_DBT_PY_REQUIREMENTS = "dbt-core>=1.11.0,<1.12.0;dbt-bigquery>=1.11.0,<1.12.0"
COMPOSER_DATA_ROOT = Path(os.environ.get("COMPOSER_DATA_ROOT", "/home/airflow/gcs/data"))


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


@dataclass(frozen=True)
class BlueprintSettings:
    environment: str
    gcp_project: str
    bq_location: str
    dbt_service_account: str
    artifacts_bucket: str
    dbt_project_dir: Path
    dbt_profiles_dir: Path
    dbt_target: str
    dbt_threads: int
    dbt_schedule: str
    slack_conn_id: str
    gcp_conn_id: str
    dbt_venv_dir: Path
    dbt_py_requirements: tuple[str, ...] = field(default_factory=tuple)
    bq_datasets: tuple[str, ...] = field(default_factory=tuple)

    @property
    def manifest_path(self) -> Path:
        return self.dbt_project_dir / "target" / "manifest.json"

    @property
    def profiles_path(self) -> Path:
        return self.dbt_profiles_dir / "profiles.yml"

    @property
    def impersonation_chain(self) -> str | None:
        return self.dbt_service_account or None

    def missing_required(self) -> list[str]:
        """Return the names of required environment variables that are unset."""
        missing: list[str] = []
        if not self.gcp_project:
            missing.append("DBT_GCP_PROJECT")
        if self.dbt_target == "prod" and not self.dbt_service_account:
            missing.append("DBT_IMPERSONATE_SA")
        if not self.dbt_py_requirements:
            missing.append("DBT_PY_REQUIREMENTS")
        return missing


def load_settings() -> BlueprintSettings:
    project_dir = Path(_env("DBT_PROJECT_DIR", str(COMPOSER_DATA_ROOT / "dbt_project")))
    requirements = tuple(r.strip() for r in _env("DBT_PY_REQUIREMENTS", DEFAULT_DBT_PY_REQUIREMENTS).split(";") if r.strip())
    datasets = tuple(d.strip() for d in _env("BQ_DATASETS", "raw,staging,warehouse").split(",") if d.strip())
    return BlueprintSettings(
        environment=_env("BLUEPRINT_ENVIRONMENT", "prod"),
        gcp_project=_env("DBT_GCP_PROJECT"),
        bq_location=_env("DBT_BQ_LOCATION", "US"),
        dbt_service_account=_env("DBT_IMPERSONATE_SA"),
        artifacts_bucket=_env("DBT_ARTIFACTS_BUCKET"),
        dbt_project_dir=project_dir,
        dbt_profiles_dir=Path(_env("DBT_PROFILES_DIR", str(project_dir))),
        dbt_target=_env("DBT_TARGET", "prod"),
        dbt_threads=max(1, _env_int("DBT_THREADS", 8)),
        dbt_schedule=_env("DBT_SCHEDULE", "0 6 * * *"),
        slack_conn_id=_env("SLACK_CONN_ID", "slack_webhook"),
        gcp_conn_id=_env("GCP_CONN_ID", "google_cloud_default"),
        dbt_venv_dir=Path(_env("DBT_VENV_DIR", "/home/airflow/cosmos_dbt_venv")),
        dbt_py_requirements=requirements,
        bq_datasets=datasets,
    )
