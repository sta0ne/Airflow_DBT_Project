# Pharma Data Pipeline

A dbt + Airflow pipeline that models pharmaceutical operations data — manufacturers,
doctors, patients, pharmacies, drugs, prescriptions, sales, and adverse events —
through a medallion architecture (raw → silver_technical → silver_business → gold),
with full change-history tracking via dbt snapshots.

---

## Architecture

```

Databricks (raw source: pharma_databricks)
│
▼
┌───────────────────┐
│  silver_technical  │   1:1 mirror of raw tables, incremental load, processed_at audit col
│  (*_t models)       │
└─────────┬──────────┘
│
├──────────────┐
▼              ▼
┌───────────────────┐   ┌────────────────┐
│  silver_business   │   │   snapshots    │   SCD Type 2 — full change history
│  (dim_ / fct_ /     │   │  (*_snapshot)   │   (dbt_valid_from / dbt_valid_to)
│   agg_ models)       │   └────────────────┘
└─────────┬──────────┘
│
▼
┌───────────────────┐
│       gold          │   Denormalized, business-question-shaped tables
│  (gold_* models)    │   for dashboards / BI tools
└─────────────────────┘

```

All layers are orchestrated by **Apache Airflow**, which runs `dbt run` / `dbt test` /
`dbt snapshot` in dependency order on a schedule.

---

## Source tables

| Table | Grain | Type |
|---|---|---|
| `manufacturers` | one row per company | Dimension |
| `doctors` | one row per doctor | Dimension |
| `patients` | one row per patient | Dimension |
| `pharmacies` | one row per pharmacy | Dimension |
| `drugs` | one row per drug | Dimension |
| `prescriptions` | one row per prescription written | Fact |
| `sales` | one row per sale transaction | Fact |
| `adverse_events` | one row per reported adverse event | Fact |

**Dimension vs. fact rule of thumb:** if a row describes something that *is*
(a person, place, product), it's a dimension. If a row describes something that
*happened* at a point in time (usually with a number attached), it's a fact.

Every table carries `created_timestamp`, `updated_timestamp`, and either `is_active`
(master/dimension tables) or a domain-specific `status` column (transactional/fact
tables — e.g. `prescriptions.status`, `sales.status`, `adverse_events.status`).
`status` was chosen over a generic `is_active` flag for fact tables because
"active/inactive" doesn't map cleanly onto a one-time event — "Filled / Cancelled /
Expired" (prescriptions) or "Completed / Refunded / Voided" (sales) is more accurate.

---

## Layer-by-layer logic

### 1. `silver_technical` — technical landing zone

- One model per source table (`drugs_t`, `sales_t`, etc.)
- `select *` plus a `processed_at` audit column
- Incremental materialization, merged on each table's natural key
  (`unique_key = 'drug_id'`, etc.)
- Incremental filter:
  ```sql
  {% if is_incremental() %}
      where updated_timestamp > (select coalesce(max(updated_timestamp), '1900-01-01') from {{ this }})
  {% endif %}
  ```
- **No business logic, no joins** — this layer only answers "did the data arrive
  correctly from Databricks?"
- Overwrites on change (Type 1) — does **not** track history. Snapshots (below)
  are what track history.

### 2. `silver_business` — business-ready, clean, joinable

- Built from `silver_technical` via `ref()`, never touches `source()` directly
- **Dimension models** (`dim_drugs`, `dim_doctors`, `dim_patients`, `dim_pharmacies`,
  `dim_manufacturers`) — explicit column selection, small many-to-one joins flattened
  in (e.g. `dim_drugs` joins in `manufacturer_name` from `manufacturers_t`)
- **Fact models** (`fct_prescriptions`, `fct_sales`, `fct_adverse_events`) — kept
  thin: event columns + foreign keys only, no descriptive text joined in, to avoid
  duplicating names across thousands of rows
- **Aggregate models** (`agg_drug_performance`) — the safe pattern for combining
  multiple fact tables: each fact is aggregated down to a shared grain (e.g. one row
  per `drug_id`) in its own CTE *before* joining, which avoids the row-multiplication
  that happens if two fact tables are joined to each other directly
- Materialized as plain `table` (not incremental) — since `silver_technical` already
  dedupes/incrementally loads, re-filtering here would be redundant for
  currently-sized tables. Revisit if `_t` tables grow very large.

### 3. `gold` — denormalized, dashboard-ready

- One model per business question, not per source table
  (e.g. `gold_monthly_drug_revenue`, `gold_drug_safety_scorecard`,
  `gold_prescriber_activity`, `gold_pharmacy_performance`)
- Fully denormalized and pre-aggregated to the grain a dashboard actually needs
- Built only from `silver_business`, never from raw or `silver_technical`
- Still never joins two fact tables directly — aggregates each to a shared grain
  first, same rule as `silver_business`
- The test for "does this belong in gold": *if a business user opened this table in
  a BI tool right now with zero further joins, would it answer their question?*

### 4. Snapshots — Slowly Changing Dimensions (SCD Type 2)

- One snapshot per source table (`drugs_snapshot`, `sales_snapshot`, etc.)
- `strategy: timestamp` (compares `updated_timestamp`) for master tables;
  `strategy: check` (compares specific columns like `status`) for tables where only
  one or two fields are expected to change post-creation
- On each `dbt snapshot` run: unchanged rows are left alone; changed rows get the
  old version closed (`dbt_valid_to = now`) and a new version opened
  (`dbt_valid_from = now`, `dbt_valid_to = NULL`)
- This is the **only** layer in the project that preserves history — every other
  layer is Type 1 (overwrite in place, current state only)
- `dbt_valid_to IS NULL` = the current version of a row
- Used for point-in-time analysis — e.g. `gold_sales_with_price_at_time_of_sale`
  joins `fct_sales` to `drugs_snapshot` on a date-range condition so each sale shows
  the drug's price **as it was on the sale date**, not today's price

---

## Orchestration (Apache Airflow)

Two DAGs:

**`pharma_silver_pipeline`** (frequent — e.g. every 6 hours)
```

run_silver_technical → test_silver_technical → run_snapshots → run_silver_business → test_silver_business

```

**`pharma_gold_pipeline`** (daily, triggered after silver succeeds)
```

wait_for_silver_business → run_gold → test_gold

```

Key design choices:
- A `dbt test` task sits between every layer, not just at the end — if
  `silver_technical` loads bad data, the pipeline halts before `silver_business` or
  `gold` ever builds on top of it.
- `dbt snapshot` runs right after `silver_technical` lands, before `silver_business`
  rebuilds current-state dimensions — so history capture isn't dependent on anything
  downstream succeeding first.
- Retries (`retries=2`) and failure alerts (Slack/email) are configured on every task.

---

## Testing

- `unique` / `not_null` on every primary key
- `accepted_values` on every `status` / `is_active` column
- `relationships` tests enforcing foreign-key integrity between facts and dimensions
  (dbt's substitute for DB-level FK constraints, since Databricks doesn't enforce
  them natively)
- A singular test validating snapshot tables have no overlapping
  `dbt_valid_from` / `dbt_valid_to` ranges for the same entity

Run all tests:
```bash
dbt test
```

---

## Project structure

```

models/
├── silver_technical/
│   ├── drugs_t.sql
│   ├── manufacturers_t.sql
│   ├── doctors_t.sql
│   ├── patients_t.sql
│   ├── pharmacies_t.sql
│   ├── prescriptions_t.sql
│   ├── sales_t.sql
│   ├── adverse_events_t.sql
│   └── properties.yml
├── silver_business/
│   ├── dim_drugs.sql
│   ├── dim_manufacturers.sql
│   ├── dim_doctors.sql
│   ├── dim_patients.sql
│   ├── dim_pharmacies.sql
│   ├── fct_prescriptions.sql
│   ├── fct_sales.sql
│   ├── fct_adverse_events.sql
│   ├── agg_drug_performance.sql
│   └── properties.yml
└── gold/
├── gold_monthly_drug_revenue.sql
├── gold_drug_safety_scorecard.sql
├── gold_prescriber_activity.sql
├── gold_pharmacy_performance.sql
└── properties.yml

snapshots/
├── drugs_snapshot.sql
├── manufacturers_snapshot.sql
├── doctors_snapshot.sql
├── patients_snapshot.sql
├── pharmacies_snapshot.sql
├── prescriptions_snapshot.sql
├── sales_snapshot.sql
└── adverse_events_snapshot.sql

macros/
└── get_current_version.sql

dags/
├── pharma_silver_pipeline.py
└── pharma_gold_pipeline.py

dbt_project.yml
packages.yml

```

---

## Setup & run

```bash
# install dbt packages (dbt_utils, used by relationship/expression tests)
dbt deps

# check raw data freshness before running anything
dbt source freshness

# build layer by layer
dbt run --select silver_technical
dbt snapshot
dbt run --select silver_business
dbt run --select gold

# run all tests
dbt test

# or let dbt resolve the full dependency order automatically
dbt run
dbt test
```

In Airflow, trigger `pharma_silver_pipeline` and `pharma_gold_pipeline` manually for
a first run, or wait for the scheduled interval.

---

## Known gaps / next steps

- [ ] Confirm `dbt snapshot` has been run at least once and history tables exist
      in the warehouse (`dbt_valid_to IS NULL` row count should match `silver_business`
      dimension row counts)
- [ ] Wire `run_snapshots` into the live Airflow DAG (currently only `dbt run` /
      `dbt test` tasks are deployed)
- [ ] Build out additional "as-of" gold models using snapshot history
      (pharmacy region history, manufacturer changes) beyond the drug-price example
- [ ] If `silver_technical` tables grow large, revisit making `silver_business`
      incremental instead of full-rebuild
- [ ] `snapshots` schema should carry the same access controls as `raw` if this
      pipeline is ever pointed at real patient data (PII/PHI), since snapshot
      history retains old values indefinitely and cannot be corrected by deleting
      the current row
