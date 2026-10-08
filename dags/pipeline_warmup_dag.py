"""
### bigquery_pipeline_warmup

> **Public preview.** Structure and contracts only. The full implementation
> ships with the complete blueprint: https://payhip.com/b/YIq4E

Hourly, near-zero-cost health probe for the dbt + BigQuery runtime. It catches
IAM, impersonation and deployment breakage before the nightly dbt run does.

| Task | What it proves |
|------|----------------|
| `validate_runtime_config` | Composer env vars set, Cosmos imports, manifest deployed and fresh |
| `bigquery_auth_probe` | Airflow SA can impersonate dbt-runner, and dbt-runner can run jobs (`SELECT 1`, 0 bytes) |
| `public_dataset_read_probe` | dbt-runner can read `bigquery-public-data.thelook_ecommerce.products` (metadata-only count, 10 MiB hard cap) |
| `verify_dataset_<name>` | dbt-runner can see `raw`, `staging` and `warehouse` |
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from blueprint_common.config import load_settings

FULL_VERSION = "https://payhip.com/b/YIq4E"

DAG_ID = "bigquery_pipeline_warmup"
MANIFEST_WARN_AGE = timedelta(days=7)
PUBLIC_PROBE_TABLE = "bigquery-public-data.thelook_ecommerce.products"
SETTINGS = load_settings()


def _validate_runtime(**_: Any) -> None:
    """Collect every configuration problem in one pass and fail once with the full list."""
    raise NotImplementedError(f"Included in the full blueprint: {FULL_VERSION}")


# In the full version, a DAG wires:
#   validate_runtime_config >> bigquery_auth_probe >> public_dataset_read_probe >> warmup_complete
#   bigquery_auth_probe >> verify_dataset_{raw,staging,warehouse} >> warmup_complete
# using BigQueryInsertJobOperator / BigQueryGetDatasetOperator with
# impersonation_chain = the dbt-runner service account.
