Absolutely, Ashok. Below are the corresponding BigQuery tables (DDL) for the end-to-end framework I shared (daily registry sync + 15-min monitoring + hourly digest + daily baseline).



Replace vz-it-pr-hukv-cdwldo-0 and ops_monitoring if your project/dataset are different.

0) Dataset (one time)
CREATE SCHEMA IF NOT EXISTS `vz-it-pr-hukv-cdwldo-0.ops_monitoring`
OPTIONS(location="US");
1) 
ops_airflow_dag_registry
  (Auto-populated daily from Airflow API)


Stores all DAGs + tags/owners/paused + routing defaults.

CREATE TABLE IF NOT EXISTS `vz-it-pr-hukv-cdwldo-0.ops_monitoring.ops_airflow_dag_registry` (
  dag_id STRING NOT NULL,

  -- captured from Airflow API
  airflow_tags ARRAY<STRING>,
  owners ARRAY<STRING>,
  is_paused BOOL,

  -- monitoring control
  enabled BOOL DEFAULT TRUE,

  -- enrichment (optional now, can be filled later)
  domain STRING,
  team_name STRING,
  owner_emails ARRAY<STRING>,
  is_critical BOOL DEFAULT FALSE,
  sla_minutes INT64,
  is_egress BOOL DEFAULT FALSE,
  hold_if_failed_dags ARRAY<STRING>,

  updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY dag_id;
2) 
ops_airflow_dependency_graph
  (Optional, for impact analysis)


If you don’t have dependency edges yet, keep table empty; monitoring still works.

CREATE TABLE IF NOT EXISTS `vz-it-pr-hukv-cdwldo-0.ops_monitoring.ops_airflow_dependency_graph` (
  upstream_dag_id STRING NOT NULL,
  downstream_dag_id STRING NOT NULL
)
CLUSTER BY upstream_dag_id, downstream_dag_id;
3) 
ops_airflow_dag_run_fact
  (Inserted every 15 minutes)


Append-only “observations” of latest runs.

CREATE TABLE IF NOT EXISTS `vz-it-pr-hukv-cdwldo-0.ops_monitoring.ops_airflow_dag_run_fact` (
  snapshot_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  dag_id STRING,
  run_id STRING,
  logical_date TIMESTAMP,
  data_interval_start TIMESTAMP,
  data_interval_end TIMESTAMP,

  state STRING,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  duration_sec INT64,
  external_trigger BOOL,

  airflow_url STRING
)
PARTITION BY DATE(snapshot_ts)
CLUSTER BY dag_id, state;
4) 
ops_airflow_task_run_fact
  (Inserted every 15 minutes)


Append-only “observations” of failing task instances (and can be expanded later).

CREATE TABLE IF NOT EXISTS `vz-it-pr-hukv-cdwldo-0.ops_monitoring.ops_airflow_task_run_fact` (
  snapshot_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  dag_id STRING,
  run_id STRING,
  task_id STRING,

  state STRING,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  duration_sec INT64,

  try_number INT64,
  operator STRING,
  log_url STRING,

  error_message STRING,
  error_signature STRING,
  root_cause_category STRING
)
PARTITION BY DATE(snapshot_ts)
CLUSTER BY dag_id, task_id, root_cause_category;
5) 
ops_airflow_incident
  (MERGE upsert every 15 minutes)


Deduped “intelligence table” that stops repeated alerts.

CREATE TABLE IF NOT EXISTS `vz-it-pr-hukv-cdwldo-0.ops_monitoring.ops_airflow_incident` (
  incident_id STRING NOT NULL,

  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  dag_id STRING,
  run_id STRING,
  logical_date TIMESTAMP,

  severity STRING,           -- P0/P1/P2/P3
  status STRING,             -- OPEN/CLOSED (you can add auto-close later)
  root_cause_category STRING,

  error_signature STRING,
  sample_error STRING,

  upstream_blockers ARRAY<STRING>,
  downstream_impacted ARRAY<STRING>,
  recommended_actions ARRAY<STRING>,

  alert_sent_ts TIMESTAMP,
  alert_count INT64 DEFAULT 0,
  routed_to ARRAY<STRING>
)
PARTITION BY DATE(created_ts)
CLUSTER BY status, severity, dag_id;
6) 
ops_airflow_baseline_metrics
  (Daily refresh)


Used for “long-running anomaly” detection.

CREATE TABLE IF NOT EXISTS `vz-it-pr-hukv-cdwldo-0.ops_monitoring.ops_airflow_baseline_metrics` (
  dag_id STRING NOT NULL,
  window_days INT64 NOT NULL,

  sample_count INT64,
  avg_duration_sec FLOAT64,
  stddev_duration_sec FLOAT64,
  p50_duration_sec FLOAT64,
  p95_duration_sec FLOAT64,

  last_computed_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY dag_id;
Optional (but very useful): a consolidated “current status” table


If you want one single table that always contains the latest status per DAG (instead of querying fact table), create this:

CREATE TABLE IF NOT EXISTS `vz-it-pr-hukv-cdwldo-0.ops_monitoring.ops_airflow_dag_current_status` (
  dag_id STRING NOT NULL,
  last_seen TIMESTAMP,
  state STRING,
  run_id STRING,
  duration_sec INT64,
  airflow_url STRING,
  updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY dag_id;
Then the 15-min monitoring DAG can MERGE into this table too (fast dashboards).

If you want, I’ll also give you:

the exact MERGE SQL to populate ops_airflow_dag_current_status from ops_airflow_dag_run_fact, and

a single “master query” that produces the same consolidated sheet you currently maintain manually (but from BQ).
