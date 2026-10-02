# Pharma Data Pipeline

A dbt + Airflow pipeline that models pharmaceutical operations data — manufacturers,
doctors, patients, pharmacies, drugs, prescriptions, sales, and adverse events —
through a medallion architecture (raw → silver_technical → silver_business → gold),
with full change-history tracking via dbt snapshots.

---

## Architecture

![Pharma Data Pipeline Architecture](architecture.svg)

Raw data lands in Databricks, is mirrored into `silver_technical`, then branches
two ways: into `silver_business` (current-state dimensions and facts) and into
`snapshots` (full change history). `gold` is built from both — mostly from
`silver_business`, with point-in-time joins against `snapshots` where historical
accuracy matters (e.g. the price of a drug at the time a sale happened, not its
price today). Everything is orchestrated end-to-end by Apache Airflow.

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

Every table carries `created_timestamp` and `updated_timestamp`, plus either
`is_active` (master/dimension tables) or a domain-specific `status` column
(transactional/fact tables — e.g. `prescriptions.status`, `sales.status`,
`adverse_events.status`). `status` was chosen over a generic `is_active` flag for
fact tables because "active/inactive" doesn't map cleanly onto a one-time event —
"Filled / Cancelled / Expired" (prescriptions) or "Completed / Refunded / Voided"
(sales) is more accurate to what actually happens to that kind of record.

---

## Layer-by-layer logic

### 1. silver_technical — technical landing zone

One model per source table. Each is a near-exact mirror of the raw table plus a
`processed_at` audit column, loaded incrementally (only new/changed rows pulled
in on each run, merged on the table's natural key). No business logic and no
joins happen here — this layer only answers "did the data arrive correctly from
Databricks?" It overwrites in place on change, so it does **not** track history;
history is handled separately by snapshots.

### 2. silver_business — business-ready, clean, joinable

Built entirely from `silver_technical`, never from raw sources directly.

- **Dimension models** — one row per entity, with small many-to-one joins
  flattened in (e.g. a drug's dimension includes its manufacturer's name and
  country, joined once here, so nothing downstream has to repeat that join).
- **Fact models** — kept thin: event columns plus foreign keys only, with no
  descriptive text joined in, to avoid duplicating names across thousands of
  transaction rows.
- **Aggregate models** — the safe pattern for combining multiple fact tables:
  each fact is first aggregated down to a shared grain (e.g. one row per drug)
  before anything is joined, which avoids the row-multiplication that happens
  if two fact tables are joined to each other directly.

This layer rebuilds fully each run rather than incrementally, since
`silver_technical` already handles deduplication and the underlying tables are
small enough that a full rebuild stays cheap.

### 3. gold — denormalized, dashboard-ready

One model per business question, not per source table — a monthly revenue
report, a drug safety scorecard, prescriber activity, pharmacy performance, and
so on. Each is fully denormalized and pre-aggregated to the grain a dashboard
actually needs, so a business user can open it with zero further joins and get
a direct answer. Built only from `silver_business` (and `snapshots` where
history matters) — never from raw or `silver_technical` directly. The same rule
applies here as in `silver_business`: never join two fact tables to each other
directly, always aggregate to a shared grain first.

### 4. Snapshots — Slowly Changing Dimensions (SCD Type 2)

One snapshot per source table. On each run, unchanged rows are left alone;
changed rows get their old version closed off and a new version opened, each
tagged with the time range it was valid for. This is the only layer in the
project that preserves history — every other layer reflects current state only.
Snapshots are what let a query answer "what was true on this date," such as
showing a drug's price as it was at the time of a specific sale rather than
whatever the price happens to be today.

---

## Orchestration (Apache Airflow)

Two scheduled pipelines:

- A **silver pipeline** that runs `silver_technical`, tests it, runs snapshots,
  then builds and tests `silver_business` — in that order, with the pipeline
  halting at the first failed test so bad data never reaches a later layer.
- A **gold pipeline**, triggered after the silver pipeline succeeds, that builds
  and tests the `gold` layer.

Snapshots run right after `silver_technical` lands and before `silver_business`
rebuilds its current-state dimensions, so history capture never depends on
anything downstream succeeding first. Every task has retries configured and
alerts on failure, so breakages surface automatically rather than being
discovered when a dashboard looks wrong.

---

## Testing

- Uniqueness and not-null checks on every primary key
- Accepted-value checks on every `status` / `is_active` column
- Referential-integrity checks between facts and dimensions, standing in for
  database-level foreign key constraints that Databricks doesn't enforce
  natively
- A check that snapshot tables never have overlapping valid-time ranges for the
  same entity, which would indicate a broken change-detection strategy

---

## Known gaps / next steps

- Confirm snapshots have actually been run at least once and that the resulting
  history tables exist in the warehouse, with current-row counts matching
  `silver_business` dimension counts
- Wire the snapshot step into the live Airflow schedule — at the time of writing
  this exists in the project but isn't yet part of the deployed DAG
- Build out further point-in-time gold models beyond the drug-price example
  (e.g. pharmacy region history, manufacturer changes)
- Revisit making `silver_business` incremental if the underlying tables grow
  significantly larger
- If this pipeline is ever pointed at real patient data, apply the same access
  controls to the `snapshots` schema as to `raw` — snapshot history retains old
  values indefinitely and can't be corrected by simply deleting the current row
