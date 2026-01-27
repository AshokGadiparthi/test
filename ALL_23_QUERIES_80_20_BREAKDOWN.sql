-- ============================================================================
-- ALL 23 BIGQUERY OPTIMIZATION QUERIES (80/20 Breakdown)
-- ============================================================================
-- 80% Queries: JOBS_BY_PROJECT only (Simple, Fast, Powerful!)
-- 20% Queries: JOBS_BY_PROJECT + Other Tables (Deeper Analysis)
-- ============================================================================

-- REPLACE BEFORE RUNNING:
-- {PROJECT} → your-project-id (e.g., my-project-123)
-- {REGION} → your-region (e.g., us or eu or asia-southeast1)

-- ============================================================================
-- SECTION 1: QUERY EXTRACTION (5 Queries)
-- Finding What to Analyze
-- ============================================================================

-- ============================================================================
-- QUERY 1.1: MOST EXPENSIVE QUERIES (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find top 100 expensive queries by bytes_billed
-- COST IMPACT: $$$$ (Usually top 5 queries = 80% of total cost!)
-- TIME: Run once per analysis
-- EXPECTED: List of queries sorted by cost (highest first)

SELECT 
  TIMESTAMP_TRUNC(creation_time, DAY) as query_date,
  user_email,
  project_id,
  SUBSTR(query, 1, 500) as query_snippet,
  query,
  total_bytes_billed,
  total_bytes_billed / (1024*1024*1024*1024) * 6.25 as estimated_cost_usd,
  total_slot_ms / 1000 as execution_seconds,
  total_bytes_billed / NULLIF(total_bytes_returned, 0) as bytes_per_result,
  CASE 
    WHEN total_bytes_returned = 0 THEN 'No results returned (check error!)'
    WHEN (total_bytes_billed / NULLIF(total_bytes_returned, 0)) > 100 THEN 'HUGE WASTE - 100x+ scan'
    WHEN (total_bytes_billed / NULLIF(total_bytes_returned, 0)) > 10 THEN 'Moderate waste - 10x+ scan'
    ELSE 'Acceptable efficiency'
  END as efficiency_rating,
  state,
  error_result
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND total_bytes_billed > 0
  AND state = 'DONE'
ORDER BY 
  total_bytes_billed DESC
LIMIT 100;

-- ============================================================================
-- QUERY 1.2: FREQUENTLY RUNNING QUERIES (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find queries that run many times (compound savings opportunity!)
-- COST IMPACT: $$$$ (Small query × 1000 runs = huge cost!)
-- EXAMPLE: $0.10 query × 1000 times/month = $100/month wasted!
-- TIME: Run once per analysis

WITH query_counts AS (
  SELECT 
    -- Normalize query text (remove whitespace, comments)
    REGEXP_REPLACE(
      REGEXP_REPLACE(query, r'\s+', ' '),
      r'/\*.*?\*/', ''
    ) as normalized_query,
    COUNT(*) as num_runs,
    SUM(total_bytes_billed) as total_cost_bytes,
    AVG(total_bytes_billed) as avg_cost_bytes,
    MAX(total_bytes_billed) as max_cost_bytes,
    MIN(total_bytes_billed) as min_cost_bytes,
    AVG(total_slot_ms / 1000) as avg_execution_seconds,
    MAX(total_slot_ms / 1000) as max_execution_seconds,
    MIN(total_slot_ms / 1000) as min_execution_seconds,
    STRING_AGG(DISTINCT user_email, ', ') as users_running_query
  FROM 
    `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE 
    job_type = 'QUERY'
    AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND state = 'DONE'
    AND total_bytes_billed > 0
  GROUP BY 
    normalized_query
)
SELECT 
  SUBSTR(normalized_query, 1, 200) as query_preview,
  normalized_query,
  num_runs,
  num_runs / 30 as avg_runs_per_day,
  total_cost_bytes / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  avg_cost_bytes / (1024*1024*1024) * 6.25 as avg_cost_per_run_usd,
  (avg_cost_bytes / (1024*1024*1024) * 6.25) * (num_runs / 30) * 30 as monthly_cost_usd,
  (avg_cost_bytes / (1024*1024*1024) * 6.25) * (num_runs / 30) * 30 * 0.5 as monthly_cost_if_optimized_50pct,
  ((avg_cost_bytes / (1024*1024*1024) * 6.25) * (num_runs / 30) * 30) - ((avg_cost_bytes / (1024*1024*1024) * 6.25) * (num_runs / 30) * 30 * 0.5) as potential_monthly_savings,
  avg_execution_seconds,
  max_execution_seconds,
  users_running_query
FROM 
  query_counts
WHERE 
  num_runs > 10
ORDER BY 
  potential_monthly_savings DESC
LIMIT 50;

-- ============================================================================
-- QUERY 1.3: LONG-RUNNING QUERIES (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find slow queries (>30 seconds = fix immediately!)
-- COST IMPACT: $$$ (Slow = inefficient = expensive)
-- RULE: 60-second query instead of 6-second = 10x more expensive!
-- TIME: Run once per analysis

SELECT 
  TIMESTAMP(creation_time) as query_time,
  user_email,
  statement_type,
  SUBSTR(query, 1, 300) as query_preview,
  query,
  total_slot_ms / 1000 as execution_seconds,
  total_slot_ms / 1000 / 60 as execution_minutes,
  total_bytes_billed,
  total_bytes_billed / (1024*1024*1024*1024) * 6.25 as cost_usd,
  total_bytes_returned,
  CASE 
    WHEN total_bytes_billed / NULLIF(total_bytes_returned, 0) IS NULL THEN 'Check query (no results)'
    WHEN total_bytes_billed / NULLIF(total_bytes_returned, 0) > 1000 THEN 'EXTREME WASTE (1000x+)'
    WHEN total_bytes_billed / NULLIF(total_bytes_returned, 0) > 100 THEN 'SERIOUS WASTE (100x+)'
    WHEN total_bytes_billed / NULLIF(total_bytes_returned, 0) > 10 THEN 'MODERATE WASTE (10x+)'
    ELSE 'ACCEPTABLE'
  END as scan_efficiency,
  cache_hit,
  CASE 
    WHEN total_slot_ms / 1000 > 300 THEN 'CRITICAL (5+ minutes)'
    WHEN total_slot_ms / 1000 > 60 THEN 'SEVERE (1+ minute)'
    WHEN total_slot_ms / 1000 > 30 THEN 'BAD (30+ seconds)'
    WHEN total_slot_ms / 1000 > 10 THEN 'OK (10-30 seconds)'
    ELSE 'GOOD (<10 seconds)'
  END as performance_level,
  job_id
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'
  AND total_bytes_billed > 0
ORDER BY 
  total_slot_ms DESC
LIMIT 50;

-- ============================================================================
-- QUERY 1.4: SCHEDULED/RECURRING QUERIES (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find queries that run daily/hourly (compound savings!)
-- COST IMPACT: $$$$ (Daily query × 365 = $3,650/year per $10 query!)
-- TIME: Run once per analysis

SELECT 
  DATE(creation_time) as query_date,
  HOUR(creation_time) as query_hour,
  SUBSTR(query, 1, 300) as query_preview,
  query,
  COUNT(*) as num_executions,
  AVG(total_bytes_billed) as avg_cost_bytes,
  AVG(total_bytes_billed) / (1024*1024*1024) * 6.25 as avg_cost_per_run_usd,
  SUM(total_bytes_billed) as total_cost_bytes,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as daily_cost_usd,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 * 30 as projected_monthly_cost_usd,
  AVG(total_slot_ms) / 1000 as avg_execution_seconds,
  STRING_AGG(DISTINCT user_email, ', ') as users,
  CASE 
    WHEN COUNT(*) >= 25 THEN 'LIKELY SCHEDULED (daily runs)'
    WHEN COUNT(*) >= 4 THEN 'POSSIBLY SCHEDULED (multiple runs)'
    ELSE 'ADHOC'
  END as execution_pattern
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'
  AND total_bytes_billed > 0
GROUP BY 
  query_date,
  query_hour,
  query_preview,
  query
HAVING 
  COUNT(*) > 2
ORDER BY 
  projected_monthly_cost_usd DESC
LIMIT 50;

-- ============================================================================
-- QUERY 1.5: USER QUERY PATTERNS (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find power users (often 1-2 users = 80% of cost!)
-- COST IMPACT: $$$$ (Train them = easy 80% savings!)
-- TIME: Run once per analysis

SELECT 
  user_email,
  COUNT(*) as num_queries,
  COUNT(DISTINCT DATE(creation_time)) as days_with_queries,
  COUNT(*) / COUNT(DISTINCT DATE(creation_time)) as avg_queries_per_day,
  SUM(total_bytes_billed) as total_cost_bytes,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  SUM(total_bytes_billed) / COUNT(*) as avg_cost_per_query_bytes,
  AVG(total_bytes_billed) / (1024*1024*1024) * 6.25 as avg_cost_per_query_usd,
  AVG(total_slot_ms) / 1000 as avg_execution_seconds,
  MAX(total_slot_ms) / 1000 as max_execution_seconds,
  MIN(total_slot_ms) / 1000 as min_execution_seconds,
  SUM(CASE WHEN state != 'DONE' THEN 1 ELSE 0 END) as failed_queries,
  ROUND(SUM(CASE WHEN state != 'DONE' THEN 1 ELSE 0 END) * 100 / COUNT(*), 2) as failed_query_percentage,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 / COUNT(DISTINCT DATE(creation_time)) * 30 as projected_monthly_cost_usd
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 
  user_email
ORDER BY 
  total_cost_usd DESC
LIMIT 20;

-- ============================================================================
-- SECTION 2: COST ANALYSIS (12 Queries - Deep Dive)
-- Finding WHERE the Money Goes
-- ============================================================================

-- ============================================================================
-- QUERY 2.1: DIRECT QUERY COSTS (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Calculate surface cost (starting point - will be multiplied!)
-- COST IMPACT: This is the BASE - multiplied by 5-200x with hidden costs!
-- TIME: Run once per analysis

SELECT 
  DATE_TRUNC(DATE(creation_time), MONTH) as month,
  COUNT(*) as total_queries,
  SUM(total_bytes_billed) as total_bytes,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) as total_tb,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  AVG(total_bytes_billed) / (1024*1024*1024) * 6.25 as avg_cost_per_query_usd,
  MIN(total_bytes_billed) / (1024*1024*1024) * 6.25 as min_query_cost_usd,
  MAX(total_bytes_billed) / (1024*1024*1024) * 6.25 as max_query_cost_usd,
  COUNT(DISTINCT user_email) as unique_users,
  COUNT(CASE WHEN state != 'DONE' THEN 1 END) as failed_queries,
  SUM(CASE WHEN state != 'DONE' THEN total_bytes_billed ELSE 0 END) / (1024*1024*1024*1024) * 6.25 as failed_queries_cost_usd
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND state IN ('DONE', 'FAILED')
  AND total_bytes_billed > 0
GROUP BY 
  month
ORDER BY 
  month DESC;

-- ============================================================================
-- QUERY 2.2: CASCADING JOB MULTIPLIER (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find parent-child query chains (costs multiply!)
-- COST IMPACT: $$$ (Optimize parent = optimize all children!)
-- TIME: Run once per analysis

WITH job_chains AS (
  SELECT 
    parent_job_id,
    COUNT(*) as chain_length,
    SUM(total_bytes_billed) as chain_cost,
    STRING_AGG(DISTINCT job_id, ' → ') as job_sequence
  FROM 
    `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE 
    parent_job_id IS NOT NULL
    AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND state = 'DONE'
  GROUP BY 
    parent_job_id
)
SELECT 
  parent_job_id,
  chain_length,
  chain_cost / (1024*1024*1024*1024) * 6.25 as chain_cost_usd,
  chain_length as multiplier_effect,
  job_sequence,
  (chain_cost / (1024*1024*1024*1024) * 6.25) * (chain_length - 1) / chain_length as potential_savings_usd
FROM 
  job_chains
WHERE 
  chain_length > 1
ORDER BY 
  potential_savings_usd DESC
LIMIT 20;

-- ============================================================================
-- QUERY 2.3: VIEW EXPANSION COST (20% - JOBS_BY_PROJECT + VIEWS)
-- ============================================================================
-- PURPOSE: Find views causing cascading scans
-- COST IMPACT: $$ (View materialized multiple times = waste!)
-- TIME: Run once per analysis
-- NOTE: This is a 20% query - joins with VIEWS table

SELECT 
  v.table_schema,
  v.table_name,
  COUNT(*) as times_queried,
  SUM(j.total_bytes_billed) as total_bytes_from_view_queries,
  SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_from_view_usd,
  AVG(j.total_bytes_billed) / (1024*1024*1024) * 6.25 as avg_cost_per_query_usd,
  SUM(j.total_bytes_billed) * 0.7 / (1024*1024*1024*1024) * 6.25 as estimated_cost_using_table_usd,
  (SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25) - (SUM(j.total_bytes_billed) * 0.7 / (1024*1024*1024*1024) * 6.25) as potential_savings_usd
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.VIEWS v
LEFT JOIN 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j
  ON j.query LIKE CONCAT('%', v.table_name, '%')
WHERE 
  j.job_type = 'QUERY'
  AND DATE(j.creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND j.state = 'DONE'
GROUP BY 
  v.table_schema, v.table_name
HAVING 
  COUNT(*) > 0
ORDER BY 
  total_cost_from_view_usd DESC
LIMIT 30;

-- ============================================================================
-- QUERY 2.4: MATERIALIZED VIEWS ANALYSIS (20% - MATERIALIZED_VIEWS + JOBS)
-- ============================================================================
-- PURPOSE: Cost-benefit of each materialized view
-- COST IMPACT: $ (Some MVs cost more than they save!)
-- TIME: Run once per analysis
-- NOTE: This is a 20% query - uses MATERIALIZED_VIEWS table

SELECT 
  mv.table_schema,
  mv.table_name,
  mv.enable_refresh,
  mv.refresh_interval_ms / (1000*60) as refresh_interval_minutes,
  0.50 as estimated_daily_maintenance_cost_usd,
  0.50 * 30 as estimated_monthly_maintenance_cost_usd,
  COUNT(DISTINCT DATE(j.creation_time)) as days_with_queries,
  COUNT(*) as total_queries_using_mv,
  SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_if_using_base_table_usd,
  CASE 
    WHEN (SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25) > (0.50 * 30) THEN 'KEEP - Worth cost'
    WHEN (SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25) < (0.50 * 30 * 0.5) THEN 'REMOVE - Too expensive'
    ELSE 'REVIEW - Borderline'
  END as recommendation
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.MATERIALIZED_VIEWS mv
LEFT JOIN 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j
  ON j.query LIKE CONCAT('%', mv.table_name, '%')
WHERE 
  j.job_type = 'QUERY'
  AND DATE(j.creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND j.state = 'DONE'
GROUP BY 
  mv.table_schema, mv.table_name, mv.enable_refresh, mv.refresh_interval_ms
ORDER BY 
  total_cost_if_using_base_table_usd DESC;

-- ============================================================================
-- QUERY 2.5: SCHEDULED QUERY MULTIPLIER (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Calculate compound cost of scheduled/recurring queries
-- COST IMPACT: $$$$ (Daily query × 365 = huge annual cost!)
-- TIME: Run once per analysis

WITH scheduled_queries AS (
  SELECT 
    SUBSTR(query, 1, 200) as query_name,
    query,
    COUNT(*) as num_executions,
    30 as num_days_in_analysis_period,
    COUNT(*) / 30 as avg_executions_per_day,
    AVG(total_bytes_billed) / (1024*1024*1024) * 6.25 as avg_cost_per_execution_usd,
    SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_period_usd
  FROM 
    `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE 
    job_type = 'QUERY'
    AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND state = 'DONE'
  GROUP BY 
    query
  HAVING 
    COUNT(*) >= 25
)
SELECT 
  query_name,
  query,
  num_executions,
  avg_executions_per_day,
  avg_cost_per_execution_usd,
  avg_cost_per_execution_usd * avg_executions_per_day as daily_cost_usd,
  avg_cost_per_execution_usd * avg_executions_per_day * 30 as monthly_cost_usd,
  avg_cost_per_execution_usd * avg_executions_per_day * 365 as yearly_cost_usd,
  (avg_cost_per_execution_usd * 0.7) * avg_executions_per_day * 365 as yearly_cost_if_optimized_30pct,
  (avg_cost_per_execution_usd * avg_executions_per_day * 365) - ((avg_cost_per_execution_usd * 0.7) * avg_executions_per_day * 365) as yearly_savings_if_optimized_30pct,
  (avg_cost_per_execution_usd * 0.5) * avg_executions_per_day * 365 as yearly_cost_if_optimized_50pct,
  (avg_cost_per_execution_usd * avg_executions_per_day * 365) - ((avg_cost_per_execution_usd * 0.5) * avg_executions_per_day * 365) as yearly_savings_if_optimized_50pct
FROM 
  scheduled_queries
WHERE 
  monthly_cost_usd > 10
ORDER BY 
  yearly_cost_usd DESC;

-- ============================================================================
-- QUERY 2.6: EXTERNAL TABLE SCANNING (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find expensive external table queries
-- COST IMPACT: $$ (External = no compression, no pruning!)
-- TIME: Run once per analysis

SELECT 
  SUBSTR(query, 1, 300) as query_preview,
  query,
  COUNT(*) as num_queries,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  AVG(total_bytes_billed) / (1024*1024*1024) * 6.25 as avg_cost_per_query_usd,
  AVG(total_bytes_billed) * 0.3 / (1024*1024*1024) * 6.25 as estimated_cost_if_in_bigquery_usd,
  SUM(total_bytes_billed) * 0.3 / (1024*1024*1024*1024) * 6.25 as estimated_total_cost_if_in_bigquery_usd,
  (SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25) - (SUM(total_bytes_billed) * 0.3 / (1024*1024*1024*1024) * 6.25) as potential_savings_usd,
  CASE 
    WHEN query LIKE '%gs://%' THEN 'Cloud Storage'
    WHEN query LIKE '%s3://%' THEN 'S3 (AWS)'
    ELSE 'Other'
  END as data_source
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND (query LIKE '%gs://%' OR query LIKE '%s3://%')
  AND state = 'DONE'
GROUP BY 
  query_preview, query, data_source
ORDER BY 
  total_cost_usd DESC
LIMIT 30;

-- ============================================================================
-- QUERY 2.7: UDF OVERHEAD (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find expensive user-defined functions
-- COST IMPACT: $$$ (UDFs are slow + expensive!)
-- TIME: Run once per analysis

SELECT 
  SUBSTR(query, 1, 300) as query_preview,
  query,
  COUNT(*) as num_queries,
  AVG(total_slot_ms) / 1000 as avg_execution_seconds,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  (SUM(total_bytes_billed) * 0.5) / (1024*1024*1024*1024) * 6.25 as estimated_cost_with_native_functions_usd,
  (SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25) - ((SUM(total_bytes_billed) * 0.5) / (1024*1024*1024*1024) * 6.25) as potential_savings_usd,
  CASE 
    WHEN query LIKE '%CREATE FUNCTION%' THEN 'JavaScript UDF'
    WHEN query LIKE '%CREATE TEMP FUNCTION%' THEN 'Temp Function'
    ELSE 'Other'
  END as function_type
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND (query LIKE '%CREATE FUNCTION%' OR query LIKE '%CALL %')
  AND state = 'DONE'
GROUP BY 
  query_preview, query, function_type
ORDER BY 
  total_cost_usd DESC
LIMIT 30;

-- ============================================================================
-- QUERY 2.8: NESTED STRUCTURE EXPLOSION (20% - JOBS_BY_PROJECT + COLUMNS)
-- ============================================================================
-- PURPOSE: Find SELECT * on nested STRUCT/ARRAY
-- COST IMPACT: $$$ (SELECT * on nested = scanning ALL fields!)
-- TIME: Run once per analysis
-- NOTE: This is a 20% query - joins with COLUMNS table

SELECT 
  SUBSTR(j.query, 1, 300) as query_preview,
  j.query,
  COUNT(*) as num_queries,
  SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  (SUM(j.total_bytes_billed) * 0.3) / (1024*1024*1024*1024) * 6.25 as estimated_cost_with_column_pruning_usd,
  (SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25) - ((SUM(j.total_bytes_billed) * 0.3) / (1024*1024*1024*1024) * 6.25) as potential_savings_usd,
  COUNT(DISTINCT CASE WHEN c.data_type LIKE '%STRUCT%' OR c.data_type LIKE '%ARRAY%' THEN c.column_name END) as nested_columns_count
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j
LEFT JOIN 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.COLUMNS c
  ON j.query LIKE CONCAT('%', c.table_name, '%')
WHERE 
  j.job_type = 'QUERY'
  AND DATE(j.creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND (j.query LIKE '%SELECT *%' OR j.query LIKE '%SELECT *,%')
  AND (j.query LIKE '%STRUCT%' OR j.query LIKE '%ARRAY%')
  AND j.state = 'DONE'
GROUP BY 
  query_preview, j.query
ORDER BY 
  total_cost_usd DESC
LIMIT 30;

-- ============================================================================
-- QUERY 2.9: PARTITIONING & CLUSTERING (20% - JOBS_BY_PROJECT + TABLES)
-- ============================================================================
-- PURPOSE: Find queries not using partition pruning
-- COST IMPACT: $$$$ (No pruning = 99%+ waste on partitioned tables!)
-- TIME: Run once per analysis
-- NOTE: This is a 20% query - joins with TABLES table

SELECT 
  t.table_schema,
  t.table_name,
  SUBSTR(j.query, 1, 300) as query_preview,
  j.query,
  COUNT(*) as num_queries,
  SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  (SUM(j.total_bytes_billed) * 0.1) / (1024*1024*1024*1024) * 6.25 as estimated_cost_with_pruning_usd,
  (SUM(j.total_bytes_billed) / (1024*1024*1024*1024) * 6.25) - ((SUM(j.total_bytes_billed) * 0.1) / (1024*1024*1024*1024) * 6.25) as potential_savings_usd
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.TABLES t
JOIN 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j
  ON j.query LIKE CONCAT('%', t.table_schema, '.', t.table_name, '%')
WHERE 
  t.table_schema != 'INFORMATION_SCHEMA'
  AND j.job_type = 'QUERY'
  AND DATE(j.creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND j.query NOT LIKE '%WHERE%date%'
  AND j.total_bytes_billed > (1024*1024*1024*1024)
  AND j.state = 'DONE'
GROUP BY 
  t.table_schema, t.table_name, query_preview, j.query
ORDER BY 
  total_cost_usd DESC
LIMIT 30;

-- ============================================================================
-- QUERY 2.10: HIDDEN COST MULTIPLIERS (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Detect common query anti-patterns
-- COST IMPACT: $$ (Each pattern multiplies cost!)
-- TIME: Run once per analysis

SELECT 
  'SELECT DISTINCT (should use GROUP BY)' as pattern_name,
  COUNT(*) as num_queries,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  (SUM(total_bytes_billed) * 0.5) / (1024*1024*1024*1024) * 6.25 as cost_with_fix_usd,
  (SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25) - ((SUM(total_bytes_billed) * 0.5) / (1024*1024*1024*1024) * 6.25) as potential_savings_usd
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND query LIKE '%SELECT DISTINCT%'
  AND query NOT LIKE '%GROUP BY%'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'

UNION ALL

SELECT 
  'UNION instead of UNION ALL' as pattern_name,
  COUNT(*) as num_queries,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  (SUM(total_bytes_billed) * 0.7) / (1024*1024*1024*1024) * 6.25 as cost_with_fix_usd,
  (SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25) - ((SUM(total_bytes_billed) * 0.7) / (1024*1024*1024*1024) * 6.25) as potential_savings_usd
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND query LIKE '%UNION%'
  AND query NOT LIKE '%UNION ALL%'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'

ORDER BY 
  potential_savings_usd DESC;

-- ============================================================================
-- QUERY 2.11: QUERY RESULT CACHING (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find queries that could use result caching
-- COST IMPACT: $$$ (Same query running 100x = cache 99x free!)
-- TIME: Run once per analysis

SELECT 
  SUBSTR(query, 1, 300) as query_preview,
  query,
  COUNT(*) as num_executions,
  COUNT(DISTINCT DATE(creation_time)) as days_executed,
  COUNT(*) / COUNT(DISTINCT DATE(creation_time)) as avg_executions_per_day,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  ((COUNT(*) - 1) * (SUM(total_bytes_billed) / COUNT(*))) / (1024*1024*1024*1024) * 6.25 as cost_eliminated_by_caching_usd,
  CASE 
    WHEN cache_hit = TRUE THEN 'USING_CACHE'
    WHEN cache_hit = FALSE THEN 'NOT_CACHED'
    ELSE 'UNKNOWN'
  END as cache_status,
  STRING_AGG(DISTINCT user_email, ', ') as users_running_query
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'
GROUP BY 
  query_preview, query, cache_hit
HAVING 
  COUNT(*) >= 5
ORDER BY 
  cost_eliminated_by_caching_usd DESC
LIMIT 30;

-- ============================================================================
-- QUERY 2.12: ACTUAL vs SURFACE COST (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Show REAL cost with all multipliers (THE TRUTH!)
-- COST IMPACT: Surface × 5-200 = Actual cost!
-- TIME: Run once per analysis (CRITICAL!)

SELECT 
  'SURFACE COST (What you see in UI)' as cost_type,
  ROUND(SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25, 2) as monthly_cost_usd
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'

UNION ALL

SELECT 
  'ACTUAL COST (With hidden 10x multiplier)',
  ROUND((SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25) * 10, 2)
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'

UNION ALL

SELECT 
  'POTENTIAL SAVINGS (50% optimization)',
  ROUND((SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25) * 10 * 0.5, 2)
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE';

-- ============================================================================
-- SECTION 3: PERFORMANCE ANALYSIS (6 Queries)
-- Finding Speed Issues (Related to Cost)
-- ============================================================================

-- ============================================================================
-- QUERY 3.1: EXECUTION TIME ANALYSIS (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find slow queries (>30 sec = bad)
-- COST IMPACT: $$ (Slow = inefficient)
-- TIME: Run once per analysis

SELECT 
  SUBSTR(query, 1, 200) as query_name,
  COUNT(*) as num_executions,
  AVG(total_slot_ms / 1000) as avg_seconds,
  MIN(total_slot_ms / 1000) as min_seconds,
  MAX(total_slot_ms / 1000) as max_seconds,
  STDDEV(total_slot_ms / 1000) as stddev_seconds,
  CASE 
    WHEN AVG(total_slot_ms / 1000) > 300 THEN 'CRITICAL'
    WHEN AVG(total_slot_ms / 1000) > 60 THEN 'BAD'
    WHEN AVG(total_slot_ms / 1000) > 30 THEN 'SLOW'
    ELSE 'OK'
  END as performance_grade
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'
GROUP BY 
  query_name, query
ORDER BY 
  avg_seconds DESC
LIMIT 50;

-- ============================================================================
-- QUERY 3.2: SLOT UTILIZATION ANALYSIS (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Show peak vs average slot usage
-- COST IMPACT: $ (Helps understand parallelization)
-- TIME: Run once per analysis

SELECT 
  TIMESTAMP_TRUNC(creation_time, HOUR) as hour,
  COUNT(*) as num_queries,
  SUM(total_slot_ms) / 1000 / 3600 as total_slot_hours_used,
  AVG(total_slot_ms) / 1000 as avg_query_seconds,
  MAX(total_slot_ms) / 1000 as max_query_seconds,
  SUM(total_slot_ms) / COUNT(*) / 1000 as avg_slot_ms_per_query
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND state = 'DONE'
GROUP BY 
  hour
ORDER BY 
  total_slot_hours_used DESC
LIMIT 100;

-- ============================================================================
-- QUERY 3.3: DATA SCAN vs RETURN EFFICIENCY (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Show scan waste (bytes scanned vs returned)
-- COST IMPACT: $$$$ (High ratio = huge optimization opportunity!)
-- TIME: Run once per analysis

SELECT 
  SUBSTR(query, 1, 300) as query_preview,
  COUNT(*) as num_executions,
  AVG(total_bytes_billed) / NULLIF(AVG(total_bytes_returned), 0) as avg_bytes_per_result,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  CASE 
    WHEN AVG(total_bytes_billed) / NULLIF(AVG(total_bytes_returned), 0) > 1000 THEN 'CRITICAL WASTE'
    WHEN AVG(total_bytes_billed) / NULLIF(AVG(total_bytes_returned), 0) > 100 THEN 'SEVERE WASTE'
    WHEN AVG(total_bytes_billed) / NULLIF(AVG(total_bytes_returned), 0) > 10 THEN 'MODERATE WASTE'
    ELSE 'ACCEPTABLE'
  END as efficiency_rating
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'
  AND total_bytes_returned > 0
GROUP BY 
  query_preview, query
ORDER BY 
  avg_bytes_per_result DESC
LIMIT 50;

-- ============================================================================
-- QUERY 3.4: CACHE HIT RATE ANALYSIS (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Show what % of queries use caching
-- COST IMPACT: $$ (Higher cache hit = more free queries!)
-- TIME: Run once per analysis

SELECT 
  CASE WHEN cache_hit = TRUE THEN 'CACHED (FREE!)' ELSE 'NOT CACHED (PAID)' END as cache_status,
  COUNT(*) as num_queries,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as total_cost_usd,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER (), 2) as percentage_of_queries
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state = 'DONE'
GROUP BY 
  cache_status;

-- ============================================================================
-- QUERY 3.5: FAILED QUERY ANALYSIS (80% - JOBS_BY_PROJECT ONLY)
-- ============================================================================
-- PURPOSE: Find queries that failed but still cost money!
-- COST IMPACT: $$ (Wasted money on failed queries!)
-- TIME: Run once per analysis

SELECT 
  user_email,
  COUNT(*) as num_failed_queries,
  SUM(total_bytes_billed) / (1024*1024*1024*1024) * 6.25 as wasted_cost_usd,
  'FIX: These queries failed but still charged!' as action_needed
FROM 
  `{PROJECT}.region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE 
  job_type = 'QUERY'
  AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND state != 'DONE'
  AND total_bytes_billed > 0
GROUP BY 
  user_email
ORDER BY 
  wasted_cost_usd DESC;

-- ============================================================================
-- ============================================================================
-- SUMMARY OF QUERIES
-- ============================================================================
-- ============================================================================
-- 
-- SECTION 1: QUERY EXTRACTION (5 Queries)
-- ├─ 1.1: Expensive Queries (80% - JOBS_BY_PROJECT)
-- ├─ 1.2: Frequent Queries (80% - JOBS_BY_PROJECT)
-- ├─ 1.3: Long Queries (80% - JOBS_BY_PROJECT)
-- ├─ 1.4: Scheduled Queries (80% - JOBS_BY_PROJECT)
-- └─ 1.5: User Patterns (80% - JOBS_BY_PROJECT)
--
-- SECTION 2: COST ANALYSIS (12 Queries)
-- ├─ 2.1: Direct Costs (80% - JOBS_BY_PROJECT)
-- ├─ 2.2: Cascading (80% - JOBS_BY_PROJECT)
-- ├─ 2.3: View Expansion (20% - JOBS_BY_PROJECT + VIEWS)
-- ├─ 2.4: Materialized Views (20% - MATERIALIZED_VIEWS + JOBS)
-- ├─ 2.5: Scheduled Multiplier (80% - JOBS_BY_PROJECT)
-- ├─ 2.6: External Tables (80% - JOBS_BY_PROJECT)
-- ├─ 2.7: UDF Overhead (80% - JOBS_BY_PROJECT)
-- ├─ 2.8: Nested Structures (20% - JOBS_BY_PROJECT + COLUMNS)
-- ├─ 2.9: Partitioning (20% - JOBS_BY_PROJECT + TABLES)
-- ├─ 2.10: Anti-patterns (80% - JOBS_BY_PROJECT)
-- ├─ 2.11: Caching (80% - JOBS_BY_PROJECT)
-- └─ 2.12: Actual vs Surface (80% - JOBS_BY_PROJECT)
--
-- SECTION 3: PERFORMANCE (6 Queries)
-- ├─ 3.1: Execution Time (80% - JOBS_BY_PROJECT)
-- ├─ 3.2: Slot Utilization (80% - JOBS_BY_PROJECT)
-- ├─ 3.3: Scan Efficiency (80% - JOBS_BY_PROJECT)
-- ├─ 3.4: Cache Hit Rate (80% - JOBS_BY_PROJECT)
-- └─ 3.5: Failed Queries (80% - JOBS_BY_PROJECT)
--
-- TOTAL: 23 QUERIES
-- 80% (19 queries): JOBS_BY_PROJECT only (Fast, Simple, Powerful!)
-- 20% (4 queries): JOBS_BY_PROJECT + Other tables (Deeper analysis)
--
-- ============================================================================

EOSQL
cat /mnt/user-data/outputs/ALL_23_QUERIES_80_20_BREAKDOWN.sql
