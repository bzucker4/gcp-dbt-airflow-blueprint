> **Public preview repository.** This document is complete. Core code is stubbed here; the full deployable blueprint is available at [https://payhip.com/b/YIq4E](https://payhip.com/b/YIq4E).

# AGENTS.md

Operating rules for AI coding assistants (Cursor, Claude, Windsurf, Copilot, Codex) working in this repository. Read this file in full before proposing or making any change. When a user instruction conflicts with a rule marked **MUST**, stop and ask the user before proceeding.

---

## 1. What this repository is

A production stack that runs **dbt Core** on **BigQuery**, orchestrated by **Cloud Composer 2 (Airflow 2.x)** through **Astronomer Cosmos**, deployed by **GitHub Actions** with keyless OIDC, and provisioned by **Terraform**.

```
gcp-dbt-airflow-blueprint/
├── dags/
│   ├── .airflowignore               # Keeps blueprint_common/ out of DAG parsing
│   ├── dbt_composer_dag.py          # Main dbt DAG (Cosmos DbtTaskGroup from manifest.json)
│   ├── pipeline_warmup_dag.py       # Hourly BigQuery auth / IAM / deployment probe
│   └── blueprint_common/
│       ├── config.py                # ALL runtime settings, read from Composer env vars
│       └── callbacks.py             # Slack alerts + dbt artifact upload (never raise)
├── dbt_project/
│   ├── dbt_project.yml              # Layer configs; `name` is load-bearing (see 4.3)
│   ├── profiles.yml                 # dev (OAuth), prod (SA impersonation), ci (oauth-secrets)
│   ├── macros/generate_schema_name.sql
│   └── models/
│       ├── staging/<source>/        # stg_<source>__<entity>.sql + _<source>__sources.yml + _<source>__models.yml
│       └── marts/<domain>/          # fct_ / dim_ models + _<domain>__models.yml
├── infrastructure/
│   ├── terraform_setup.tf           # Composer, GCS, BigQuery datasets, IAM, WIF, Secret Manager
│   └── terraform.tfvars.example
├── github_workflows/deploy_to_gcp.yml   # Copy to .github/workflows/ to activate
├── AGENTS.md                        # This file
└── SKILL.md                         # Human operations runbook
```

### Data flow

```
raw (ingestion-owned, read-only for dbt)
  -> staging   (views, 1:1 with source tables, typed + renamed)
  -> warehouse (tables / incremental, business-ready facts and dimensions)
```

### Identity model (do not weaken)

| Identity | Used by | Can do | Cannot do |
|---|---|---|---|
| Airflow runtime SA (`<prefix>-airflow`) | Composer scheduler/workers | Run Airflow, read `airflow-*` secrets, impersonate dbt-runner, create objects in artifact bucket | Read or write any BigQuery data |
| dbt-runner SA (`<prefix>-dbt-runner`) | dbt (via impersonation) | Create BQ jobs; read `raw`; write `staging`, `warehouse` | Anything outside those datasets; has no keys |
| CI deployer SA | GitHub Actions (OIDC) | Write to Composer bucket and artifact bucket | BigQuery, IAM, Composer config |
| Developer | Local dbt (`dev` target) | Write to personal `dbt_<user>_scratch` dataset | Should not write to `staging` / `warehouse` |

---

## 2. Golden rules (MUST)

1. **MUST NOT** create, request, download or commit service account JSON keys. All auth is ADC, impersonation or OIDC.
2. **MUST NOT** hardcode project IDs, bucket names, dataset locations or service account emails in DAGs or dbt models. Use `blueprint_common/config.py` (Airflow) or `env_var()` / `target` (dbt).
3. **MUST NOT** grant project-level `roles/bigquery.dataEditor`, `roles/bigquery.admin`, `roles/editor` or `roles/owner` to any service account. Data access is granted per dataset.
4. **MUST NOT** call `dbt ls`, `dbt parse`, open network connections, or query BigQuery at DAG top level. Parsing happens every ~30s on the scheduler.
5. **MUST** keep `LoadMode.DBT_MANIFEST` in the Cosmos `RenderConfig`. The manifest is produced by CI.
6. **MUST** add a YAML entry with a description and primary key tests (`unique`, `not_null`) for every new model.
7. **MUST** run the validation commands in section 7 before declaring a task complete.
8. **MUST** keep callbacks non-raising (wrap in `try/except Exception` and log).
9. **MUST NOT** edit `dbt_project/target/`, `dbt_project/dbt_packages/` or `dbt_project/logs/`; they are build outputs.
10. **MUST NOT** use `select *` in the final CTE of a mart without an explicit column list upstream (the `final` CTE must enumerate columns).

---

## 3. Naming conventions

| Object | Pattern | Example |
|---|---|---|
| Source YAML | `models/staging/<source>/_<source>__sources.yml` | `_stripe__sources.yml` |
| Staging model | `stg_<source>__<entity_plural>` | `stg_stripe__charges` |
| Staging YAML | `models/staging/<source>/_<source>__models.yml` | `_stripe__models.yml` |
| Intermediate model | `int_<entity>__<verb>` in `models/intermediate/<domain>/` | `int_payments__pivoted_to_orders` |
| Fact | `fct_<business_process>` | `fct_orders` |
| Dimension | `dim_<entity>` | `dim_customers` |
| Mart YAML | `models/marts/<domain>/_<domain>__models.yml` | `_finance__models.yml` |
| Primary key column | `<entity>_id` | `order_id` |
| Timestamps | `<event>_at` (TIMESTAMP, UTC) | `ordered_at` |
| Dates | `<event>_date` (DATE) | `order_date` |
| Booleans | `is_<state>` / `has_<thing>` | `is_returned` |
| Airflow DAG file | `<purpose>_dag.py` | `finance_exports_dag.py` |
| Airflow DAG id | `snake_case`, stable forever | `dbt_bigquery_transform` |

Note the **double underscore** between source and entity in staging names. Cosmos and dbt docs both rely on these names being unique across the project.

---

## 4. Writing dbt models

### 4.1 Staging models (one per source table)

- Live in `models/staging/<source>/`. Materialized as views (inherited).
- Select only from `{{ source() }}`. Never `ref()` another model.
- Do only: rename, cast, lowercase/trim, basic unit conversion. No joins, no aggregations.
- Drop PII unless a downstream model explicitly needs it.
- Real sources live in the `raw` dataset; declare them with `database: "{{ env_var('DBT_GCP_PROJECT') }}"` and `schema: raw`.

```sql
with

source as (

    select * from {{ source('stripe', 'charges') }}

),

renamed as (

    select
        -- ids
        id as charge_id,
        customer as customer_id,

        -- amounts (cents -> currency units)
        cast(amount as numeric) / 100 as amount,
        lower(currency) as currency_code,

        -- status
        lower(status) as charge_status,

        -- timestamps
        timestamp_seconds(created) as created_at

    from source

)

select * from renamed
```

### 4.2 Marts (facts and dimensions)

- Live in `models/marts/<domain>/`. Reference only `ref()`, never `source()`.
- Structure: one import CTE per upstream model at the top, then logic CTEs, then a `final` CTE with an explicit column list, then `select * from final`.
- Large facts (more than a few GB or growing daily) **must** be incremental:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
        cluster_by=['<high-cardinality filter column>'],
        on_schema_change='append_new_columns'
    )
}}
```

- Incremental filters use `var('incremental_lookback_days')`, never a hardcoded number.
- Use BigQuery SQL dialect: `date_sub`, `timestamp_diff`, `safe_cast`, `qualify`. Prefer `safe_divide` over `/`.

### 4.3 Load-bearing names

- `dbt_project.yml` `name: gcp_dbt_blueprint` must equal `DBT_PROJECT_NAME` in `dags/blueprint_common/config.py`.
- `profiles.yml` top-level key must equal `profile:` in `dbt_project.yml` and `DBT_PROFILE_NAME` in `config.py`.
- If a user asks to rename the project, change all three in the same change set.

### 4.4 Tests and documentation

- Every model: `description`, primary key with `unique` + `not_null`.
- Foreign keys: `relationships` test.
- Enumerations: `accepted_values` with `config: {severity: warn}` unless the business requires hard failure.
- Use the `data_tests:` key (dbt 1.8+), not `tests:`.

### 4.5 Excluding a model from Airflow

Add `tags: ['exclude_from_airflow']` in the model config. The Cosmos `RenderConfig` excludes this tag.

---

## 5. Modifying the Airflow DAGs without breaking Cosmos

### Do

- Change schedule, threads or targets through Composer environment variables in Terraform (`DBT_SCHEDULE`, `DBT_THREADS`), not in Python.
- Add new settings as fields on `BlueprintSettings` in `config.py` with a safe default.
- Add upstream/downstream tasks around the task group: `preflight >> dbt_transform >> your_task`.
- To run a subset of models in a new DAG, create a **new DAG file** with its own `DbtTaskGroup` and a `RenderConfig(select=[...])`, reusing the same `ProjectConfig`, `ProfileConfig` and `ExecutionConfig` pattern.
- Keep heavy imports (`cosmos`, provider hooks) inside functions when they are only needed at run time.
- Pass dbt flags through `operator_args` (for example `{"vars": {...}, "full_refresh": False}`).

### Do not

- Do not rename `DBT_TASK_GROUP_ID` or `DAG_ID`: Airflow treats renamed tasks as new tasks and loses history.
- Do not switch `LoadMode` to `DBT_LS` or `AUTOMATIC` on Composer: it executes dbt on every scheduler parse.
- Do not remove `emit_datasets=False` from `RenderConfig` unless another DAG must schedule on dbt outputs; per-model Airflow Datasets add scheduler and metadata DB load.
- Do not use deprecated context keys (`execution_date`, `next_execution_date`, `prev_execution_date`, `tomorrow_ds`, `yesterday_ds`) in callbacks or templates. Use `logical_date`, `data_interval_start`, `data_interval_end`, `run_id`, `ti`.
- Do not install dbt with `pip install --user` or point `DBT_VENV_DIR` at `/home/airflow/.local`: user site-packages leak into Airflow's interpreter.
- Do not set `ProjectConfig(install_dbt_deps=True)`: packages are vendored by CI and runtime must not depend on the internet.
- Do not remove the `_parse_time_problem()` guard, the `try/except` around `_build_dbt_task_group()`, or `auto_register=False` in `_new_dag()`. Together they guarantee that a missing or corrupt manifest renders exactly one diagnostic task (`dbt_transform_unavailable`) instead of a broken DAG. Without `auto_register=False`, Airflow would also register the half-built DAG discarded after a render failure.
- Do not add `on_failure_callback` at both DAG and task level (double alerts). Task-level via `default_args` is the standard here.
- Do not switch away from `ExecutionMode.VIRTUALENV`, and never add `dbt-core` / `dbt-bigquery` to `composer_pypi_packages`: dbt 1.10+ needs protobuf 6, which conflicts with Airflow 2.x Composer images. Change dbt versions only in `dbt_py_requirements` (Terraform) and the workflow `DBT_*_SPEC` values.
- Do not use `ExecutionMode.KUBERNETES`, `DOCKER` or `GCP_CLOUD_RUN_JOB` without adding the matching infrastructure in Terraform first.
- Do not write to the Composer bucket from a DAG except under `data/` scratch paths.
- Do not add top-level `Variable.get()` calls (each is a Secret Manager API call on every parse).

### Retry and timeout contract

All tasks inherit: `retries=2`, `retry_delay=3m`, `retry_exponential_backoff=True`, `max_retry_delay=30m`, `execution_timeout=2h`. Configuration errors raise `AirflowFailException` to skip retries. Preserve this contract in new DAGs.

---

## 6. Terraform rules

- One file, `infrastructure/terraform_setup.tf`. New resources go in the matching section.
- Every new resource gets `labels = local.common_labels` where supported.
- New IAM grants use `_iam_member` (additive), never `_iam_policy` or `_iam_binding` (authoritative, can wipe other grants).
- Grant at the narrowest scope: dataset > bucket > project.
- A new BigQuery dataset is added to `local.datasets` with an explicit `dbt_role`; the IAM and `BQ_DATASETS` env var follow automatically.
- Never put secret values in Terraform. Create the `google_secret_manager_secret` container only.
- Always run `terraform fmt` and `terraform validate`.

---

## 7. Validation before you finish (MUST)

```bash
# dbt (from dbt_project/, with `gcloud auth application-default login` done)
export DBT_GCP_PROJECT=<project> DBT_TARGET=dev
dbt parse
dbt build --select <changed_models>+ --target dev

# DAGs
python -m compileall -q dags

# Terraform
cd infrastructure && terraform fmt -check && terraform validate
```

If you cannot run a command (no credentials), say so explicitly in your summary and list what the user must run.

---

## 8. Copy-paste prompts for users

Paste these into your AI assistant. Fill in the angle brackets.

**Add a new source and staging layer**

```
Following AGENTS.md, add a new dbt source named <source> that reads tables <table_a>, <table_b>
from the `raw` dataset. Create _<source>__sources.yml, one stg_<source>__<entity> model per table,
and _<source>__models.yml with descriptions and primary key tests. Drop PII columns:
<columns>. Then run dbt parse and show me the dbt build command for the new models.
```

**Build a new mart**

```
Following AGENTS.md section 4.2, create fct_<process> in models/marts/<domain>/ at the grain of
one row per <grain>. Join <stg models>. It is large and append-heavy, so make it incremental with
insert_overwrite partitioned on <date column> and clustered by <columns>. Add YAML docs and tests
(unique/not_null on the key, relationships to <dim>). Do not change any DAG.
```

**Add a separate DAG for a subset of models**

```
Following AGENTS.md section 5, create dags/<domain>_hourly_dag.py that runs only models tagged
'<tag>' every hour using a Cosmos DbtTaskGroup with LoadMode.DBT_MANIFEST and the same
profile/execution config as dbt_composer_dag.py. Reuse blueprint_common for settings and Slack
callbacks. Keep the _parse_time_problem guard pattern. Tag the models in dbt_project.yml.
```

**Grant dbt access to a new dataset**

```
Following AGENTS.md section 6, add a BigQuery dataset named <name> to local.datasets in
infrastructure/terraform_setup.tf with dbt_role <roles/bigquery.dataViewer or dataEditor>.
Do not add any project-level BigQuery roles. Show the terraform plan command I should run.
```

**Debug a failed production run**

```
The dbt_bigquery_transform DAG failed on task <task_id> with this error: <paste>. Using the
Troubleshooting Matrix in SKILL.md and the identity model in AGENTS.md, identify the root cause,
tell me which identity lacks which permission (if IAM related), and propose the minimal fix.
Do not suggest broad roles.
```

**Code review a change**

```
Review my staged changes against every MUST rule in AGENTS.md. List violations with file and line,
then list naming convention issues from section 3. Do not rewrite files; report only.
```

---

## 9. Definition of done

- [ ] Naming follows section 3
- [ ] New models have YAML docs and key tests
- [ ] No hardcoded project, bucket, dataset location or SA email
- [ ] No new keys, no broad roles
- [ ] `dbt parse` succeeds; `dbt build` on changed models succeeds in `dev` (or the user was told to run it)
- [ ] `python -m compileall -q dags` passes
- [ ] `terraform fmt -check && terraform validate` pass (if infrastructure changed)
- [ ] SKILL.md updated if operational behavior changed
