# gcp-dbt-airflow-blueprint

**Self-host dbt Core on Google Cloud Composer + BigQuery. Drop the dbt Cloud license.**

A production blueprint that runs every dbt model as an observable, retryable Airflow task through Astronomer Cosmos, deployed from GitHub with zero stored credentials.

> ### 🔓 This is the public preview
> You can browse the full repository layout, the operations runbook ([`SKILL.md`](SKILL.md)) and the AI-assistant rules ([`AGENTS.md`](AGENTS.md)). Core logic (DAGs, Terraform, CI/CD, callbacks) is stubbed with its real signatures and contracts.
>
> **[Get the complete, deployable blueprint →](https://payhip.com/b/YIq4E)**

---

## What's inside

```
gcp-dbt-airflow-blueprint/
├── dags/
│   ├── dbt_composer_dag.py          # Cosmos 1.15+ DAG with single-task manifest fallback   [stub]
│   ├── pipeline_warmup_dag.py       # Hourly IAM / impersonation / dataset health probe      [stub]
│   ├── .airflowignore
│   └── blueprint_common/
│       ├── config.py                # Env-var driven settings, no hardcoded IDs              [full]
│       └── callbacks.py             # Slack alerts + dbt artifact upload, never raise        [stub]
├── dbt_project/
│   ├── dbt_project.yml              # Layered dataset routing                                [full]
│   ├── profiles.yml                 # dev (OAuth scratch) / prod (impersonation) / ci (OIDC) [full]
│   ├── macros/generate_schema_name.sql                                                      [stub]
│   └── models/
│       ├── staging/thelook/         # Demo staging layer on a free public dataset            [full]
│       └── marts/core/fct_orders.sql   # Partitioned, clustered incremental fact            [stub]
├── infrastructure/
│   ├── terraform_setup.tf           # Composer 2, BigQuery, dual-SA IAM, WIF, Secret Mgr     [stub]
│   └── terraform.tfvars.example                                                             [full]
├── github_workflows/
│   └── deploy_to_gcp.yml            # terraform validate gate + dbt parse + GCS rsync        [stub]
├── AGENTS.md                        # Rules for Cursor / Claude / Windsurf                   [full]
└── SKILL.md                         # Setup guide, ops tables, troubleshooting matrix        [full]
```

## Why this blueprint

| Problem teams hit | How the blueprint solves it |
|---|---|
| dbt 1.10+ needs protobuf 6; Airflow 2.x Composer images pin protobuf 4. Installing dbt into Composer breaks Google operators. | dbt runs in an isolated, per-worker cached virtualenv via Cosmos `ExecutionMode.VIRTUALENV`. dbt is never in Composer's package list. |
| Service account JSON keys leak. | No keys anywhere: OIDC + Workload Identity Federation for CI, ADC for developers, impersonation in Composer. |
| The Airflow identity can read all your data. | Dual-account pattern: the Airflow SA has **zero** BigQuery access and impersonates a restricted `dbt-runner` SA (read `raw`, write `staging` + `warehouse` only). |
| A missing or half-synced `manifest.json` breaks the scheduler. | The DAG always imports. In 10 tested failure scenarios it renders exactly **one** diagnostic task explaining the fix. |
| Developers write into prod datasets. | Dev targets route every model to a personal `dbt_<user>_scratch` dataset. |
| Runaway queries. | `maximum_bytes_billed` guards per target; incremental `insert_overwrite` marts. |

## How it was verified

- **Airflow:** both DAGs import cleanly in Airflow 2.9.3 with Cosmos 1.15.1, with no deprecation warnings from blueprint code.
- **Failure scenarios:** missing, empty, corrupt, truncated and wrongly-shaped manifests, missing project files and missing Cosmos all degrade to a single diagnostic task with zero import errors.
- **dbt:** `dbt parse` on dbt 1.11 runs with no warnings, and dataset routing was checked for both dev and prod.
- **Terraform:** 152 arguments and 23 nested blocks were validated against the `hashicorp/google` 6.50 provider schema with 0 errors. The file is `terraform fmt` canonical.
- **CI:** the shipped workflow gates every deploy on real `terraform validate`.

## Documentation (full, free to read)

- **[SKILL.md](SKILL.md)** covers the step-by-step setup, local vs Airflow command tables, the security model, cost levers and a 20+ row troubleshooting matrix.
- **[AGENTS.md](AGENTS.md)** holds the rules and copy-paste prompts that let AI coding assistants extend the project without breaking the Airflow runtime.

## Get the full blueprint

Everything stubbed here is complete and deployable in the paid version: the full Terraform, both DAGs, Slack and artifact callbacks, the CI/CD workflow, the dataset routing macro and the incremental mart.

**[Get gcp-dbt-airflow-blueprint →](https://payhip.com/b/YIq4E)**

---

<sub>The documentation in this repository is provided for evaluation. The full source code is licensed separately to purchasers. See [LICENSE.md](LICENSE.md).</sub>
