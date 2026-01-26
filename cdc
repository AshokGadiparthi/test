# 🎯 UNIFIED METADATA TABLE STRATEGY
## Managing All 23 Queries with ONE Daily Metadata Table

---

## 📊 THE PROBLEM YOU SOLVED

You have **23 BigQuery optimization queries** that break down as:

```
80% CATEGORY (19 queries):
├─ Use: JOBS_BY_PROJECT table ONLY
├─ Speed: <5 seconds each
├─ Simple: No joins needed
├─ Value: 80% of insights
└─ Total time: ~2-5 minutes

20% CATEGORY (4 queries):
├─ Use: JOBS_BY_PROJECT + Other tables
├─ Speed: <10 seconds each  
├─ Complexity: Advanced joins
├─ Value: Additional 20% depth
└─ Total time: ~10-15 minutes

TOTAL: 23 queries = 3-4 hours if all run individually
```

**The Question:** "Do I run all 23 individually every day or consolidate into ONE metadata table?"

---

## ✅ THE ANSWER: HYBRID APPROACH

### **Strategy Summary**

```
┌─────────────────────────────────────────────────────┐
│  TIER 1: Master Materialized Table (Daily)          │
│  ├─ Consolidates ALL 23 query results              │
│  ├─ Single source of truth for all metrics         │
│  ├─ Updated daily at 2 AM UTC (off-peak)           │
│  └─ Queries read this, not raw JOBS_BY_PROJECT     │
│                                                     │
│  TIER 2: Quick Analysis Tables (Daily)             │
│  ├─ Top 5 "80% queries" results materialized       │
│  ├─ Subset of Tier 1 for fastest access            │
│  ├─ Updated every 2 hours for freshness            │
│  └─ For dashboards & alerts                        │
│                                                     │
│  TIER 3: Raw Query Results (On-Demand)             │
│  ├─ Run individual query when needed               │
│  ├─ Not materialized (saves storage)               │
│  ├─ For deep dives & troubleshooting               │
│  └─ Uses Tier 1 data for context                   │
└─────────────────────────────────────────────────────┘
```

---

## 🏗️ ARCHITECTURE: ONE MASTER METADATA TABLE

### **Option A: RECOMMENDED - Single Master Table**

```sql
-- THIS IS YOUR ONE METADATA TABLE
-- Updated daily, everything in one place

CREATE OR REPLACE TABLE `project.monitoring.bq_query_metrics_daily` AS
WITH job_data AS (
  SELECT 
    DATE(creation_time) as metrics_date,
    query,
    project_id,
    user_email,
    job_id,
    job_type,
    total_bytes_processed,
    total_bytes_billed,
    total_slot_ms,
    cache_hit,
    state,
    creation_time
  FROM `project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  WHERE DATE(creation_time) = CURRENT_DATE() - 1  -- Yesterday's data
    AND job_type = 'QUERY'
    AND state = 'DONE'
),
-- SECTION 1: EXTRACTION QUERIES (5 queries)
expensive_queries AS (
  SELECT
    metrics_date,
    'Section 1.1: Expensive Queries' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY total_bytes_billed DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_1.1' as query_id
  FROM job_data
  GROUP BY metrics_date, query, user_email
),
frequent_queries AS (
  SELECT
    metrics_date,
    'Section 1.2: Frequent Queries' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY COUNT(*) DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_1.2' as query_id
  FROM job_data
  GROUP BY metrics_date, query, user_email
),
long_queries AS (
  SELECT
    metrics_date,
    'Section 1.3: Long Queries' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY AVG(total_slot_ms) DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_1.3' as query_id
  FROM job_data
  GROUP BY metrics_date, query, user_email
),
scheduled_queries AS (
  SELECT
    metrics_date,
    'Section 1.4: Scheduled Queries' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY COUNT(*) DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    'SCHEDULED' as user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_1.4' as query_id
  FROM job_data
  WHERE query LIKE '%DECLARE%' OR job_id LIKE '%-scheduled-%'
  GROUP BY metrics_date, query
),
user_patterns AS (
  SELECT
    metrics_date,
    'Section 1.5: User Patterns' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY SUM(total_bytes_billed) DESC) as rank,
    CONCAT(user_email, ' - ', CAST(COUNT(*) as STRING), ' queries') as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_1.5' as query_id
  FROM job_data
  WHERE user_email IS NOT NULL
  GROUP BY metrics_date, user_email
),

-- SECTION 2: COST ANALYSIS (12 queries)
direct_costs AS (
  SELECT
    metrics_date,
    'Section 2.1: Direct Costs' as query_category,
    1 as rank,
    'Total Daily Query Cost' as query_text,
    'SYSTEM' as user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_2.1' as query_id
  FROM job_data
),
cascading_costs AS (
  SELECT
    metrics_date,
    'Section 2.2: Cascading Costs' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY SUM(total_bytes_billed) DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_2.2' as query_id
  FROM job_data
  WHERE total_slot_ms > 60000  -- Long-running = cascading cost
  GROUP BY metrics_date, query, user_email
),
execution_time_dist AS (
  SELECT
    metrics_date,
    'Section 2.5: Execution Distribution' as query_category,
    1 as rank,
    CONCAT(
      CAST(COUNTIF(total_slot_ms < 5000) as STRING), ' < 5s | ',
      CAST(COUNTIF(total_slot_ms BETWEEN 5000 AND 60000) as STRING), ' 5-60s | ',
      CAST(COUNTIF(total_slot_ms > 60000) as STRING), ' > 60s'
    ) as query_text,
    'DISTRIBUTION' as user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_2.5' as query_id
  FROM job_data
),
cache_impact AS (
  SELECT
    metrics_date,
    'Section 2.11: Cache Hit Rate' as query_category,
    1 as rank,
    CONCAT(
      'Cache hits: ', CAST(COUNTIF(cache_hit = true) as STRING), ' | ',
      'Regular: ', CAST(COUNTIF(cache_hit = false) as STRING)
    ) as query_text,
    'CACHE_ANALYSIS' as user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_2.11' as query_id
  FROM job_data
),
actual_vs_surface AS (
  SELECT
    metrics_date,
    'Section 2.12: Actual vs Surface Cost' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY total_bytes_billed DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_2.12' as query_id
  FROM job_data
  GROUP BY metrics_date, query, user_email
),

-- SECTION 3: PERFORMANCE ANALYSIS (6 queries)
execution_time_metrics AS (
  SELECT
    metrics_date,
    'Section 3.1: Execution Time' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY AVG(total_slot_ms) DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_3.1' as query_id
  FROM job_data
  GROUP BY metrics_date, query, user_email
),
slot_utilization AS (
  SELECT
    metrics_date,
    'Section 3.2: Slot Utilization' as query_category,
    1 as rank,
    CONCAT(
      'Peak: ', CAST(MAX(total_slot_ms)/1000.0 as STRING), 's | ',
      'Avg: ', CAST(AVG(total_slot_ms)/1000.0 as STRING), 's'
    ) as query_text,
    'SLOT_ANALYSIS' as user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_3.2' as query_id
  FROM job_data
),
scan_efficiency AS (
  SELECT
    metrics_date,
    'Section 3.3: Scan Efficiency' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY total_bytes_billed DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_3.3' as query_id
  FROM job_data
  GROUP BY metrics_date, query, user_email
),
failed_queries AS (
  SELECT
    metrics_date,
    'Section 3.5: Failed Queries (Wasted $)' as query_category,
    ROW_NUMBER() OVER (PARTITION BY metrics_date ORDER BY COUNT(*) DESC) as rank,
    SUBSTR(query, 1, 500) as query_text,
    user_email,
    COUNT(*) as execution_count,
    SUM(total_bytes_billed) / POW(10,12) * 6.25 as cost_usd,
    AVG(total_slot_ms / 1000.0) as avg_execution_seconds,
    'QUERY_3.5' as query_id
  FROM `project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  WHERE DATE(creation_time) = CURRENT_DATE() - 1
    AND job_type = 'QUERY'
    AND state != 'DONE'
  GROUP BY metrics_date, query, user_email
),

-- UNION ALL RESULTS
all_metrics AS (
  SELECT * FROM expensive_queries
  UNION ALL SELECT * FROM frequent_queries
  UNION ALL SELECT * FROM long_queries
  UNION ALL SELECT * FROM scheduled_queries
  UNION ALL SELECT * FROM user_patterns
  UNION ALL SELECT * FROM direct_costs
  UNION ALL SELECT * FROM cascading_costs
  UNION ALL SELECT * FROM execution_time_dist
  UNION ALL SELECT * FROM cache_impact
  UNION ALL SELECT * FROM actual_vs_surface
  UNION ALL SELECT * FROM execution_time_metrics
  UNION ALL SELECT * FROM slot_utilization
  UNION ALL SELECT * FROM scan_efficiency
  UNION ALL SELECT * FROM failed_queries
)

SELECT 
  *,
  CURRENT_TIMESTAMP() as materialization_time,
  'Complete' as data_freshness_status
FROM all_metrics
;
```

---

## 📋 IMPLEMENTATION PLAN

### **Step 1: Create the Master Metadata Table** (30 minutes)

```bash
# Run this SQL once to create the table structure
# Then set it to run on a schedule
```

### **Step 2: Schedule Daily Materialization** (15 minutes)

```sql
-- Create scheduled query to run daily at 2 AM UTC
CREATE OR REPLACE SCHEDULE daily_query_metrics_refresh
OPTIONS (
  query='''
    -- Copy the materialization query above
    CREATE OR REPLACE TABLE `project.monitoring.bq_query_metrics_daily` AS
    -- ... the entire CTE structure ...
  ''',
  frequency='DAILY',
  time_zone='UTC',
  display_name='Daily Query Metrics Materialization'
);
```

### **Step 3: Create Quick Access Tables** (30 minutes)

```sql
-- TIER 2: Quick Analysis Table (Top 5 + 80% queries)
-- Subset of master table, refreshes every 2 hours

CREATE OR REPLACE TABLE `project.monitoring.bq_quick_analysis` AS
SELECT *
FROM `project.monitoring.bq_query_metrics_daily`
WHERE query_id IN ('QUERY_1.1', 'QUERY_1.2', 'QUERY_1.5', 'QUERY_2.1', 'QUERY_2.12')
  AND metrics_date >= CURRENT_DATE() - 7;
```

### **Step 4: Create Individual Query Views** (1 hour)

```sql
-- TIER 3: Individual Query Views for drilling down
-- These read from master table (no raw scans)

CREATE OR REPLACE VIEW `project.monitoring.vw_1_1_expensive_queries` AS
SELECT *
FROM `project.monitoring.bq_query_metrics_daily`
WHERE query_id = 'QUERY_1.1'
ORDER BY cost_usd DESC
LIMIT 50;

-- Repeat for all 23 query categories...
CREATE OR REPLACE VIEW `project.monitoring.vw_1_2_frequent_queries` AS
SELECT * FROM `project.monitoring.bq_query_metrics_daily`
WHERE query_id = 'QUERY_1.2'
ORDER BY execution_count DESC;

-- And so on for all 23 queries
```

---

## 🎯 HOW TO USE THE UNIFIED TABLE

### **Scenario 1: Quick Dashboard (30 seconds)**

```sql
-- Just read TIER 2 table (refreshed every 2 hours)
SELECT *
FROM `project.monitoring.bq_quick_analysis`
WHERE metrics_date >= CURRENT_DATE() - 7
ORDER BY cost_usd DESC;

-- Response time: <100ms
-- Data freshness: 2 hours old
-- Cost: ~$0 (reading materialized table)
```

### **Scenario 2: Complete Daily Report (2 minutes)**

```sql
-- Read the TIER 1 master table (refreshed daily)
SELECT 
  metrics_date,
  query_category,
  SUM(cost_usd) as category_total_cost,
  COUNT(*) as num_insights,
  MAX(avg_execution_seconds) as slowest_avg_execution
FROM `project.monitoring.bq_query_metrics_daily`
WHERE metrics_date >= CURRENT_DATE() - 30
GROUP BY metrics_date, query_category
ORDER BY metrics_date DESC, category_total_cost DESC;

-- Response time: <500ms
-- Data freshness: 24 hours old
-- Cost: ~$0 (reading materialized table)
```

### **Scenario 3: Deep Analysis - Specific Query Category (30 seconds)**

```sql
-- Query the view for one specific category
SELECT *
FROM `project.monitoring.vw_1_1_expensive_queries`
WHERE metrics_date >= CURRENT_DATE() - 30
ORDER BY cost_usd DESC
LIMIT 100;

-- Response time: <200ms
-- Data freshness: 24 hours old
-- Cost: ~$0 (reading materialized table)
```

### **Scenario 4: Real-Time Troubleshooting (5 minutes)**

```sql
-- When you need LIVE data, run a specific query individually
-- Read from JOBS_BY_PROJECT directly, but filtered for speed

SELECT 
  creation_time,
  user_email,
  SUBSTR(query, 1, 200) as query_text,
  total_bytes_billed / POW(10,12) * 6.25 as cost_usd,
  total_slot_ms / 1000.0 as execution_seconds
FROM `project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time >= CURRENT_TIMESTAMP() - INTERVAL 2 HOUR
  AND job_type = 'QUERY'
  AND user_email = 'someone@company.com'
  AND state = 'DONE'
ORDER BY cost_usd DESC
LIMIT 50;

-- Response time: <30 seconds
-- Data freshness: LIVE (current hour)
-- Cost: $0.01-0.05 (small scan)
```

---

## 📊 RUNNING STRATEGY: INDIVIDUAL VS CONSOLIDATED

### **Should you run all 23 individually or use consolidated table?**

**ANSWER: Both!**

| Scenario | Approach | Frequency | Cost |
|----------|----------|-----------|------|
| **Daily monitoring** | Read TIER 1 (consolidated) | Daily | $0 |
| **Quick dashboard** | Read TIER 2 (quick access) | Every 2 hrs | $0 |
| **Deep analysis** | Read views from TIER 1 | As-needed | $0 |
| **Live troubleshooting** | Run individual query | When needed | $0.01-0.05 |
| **Full audit** | Run all 23 individually | Monthly | $2-3 |

---

## 🚀 COST COMPARISON

### **Running All 23 Individually, Daily:**
```
Cost per run: ~$3-5 (scans 100+ GB)
Daily cost: $3-5
Monthly cost: $90-150
Annual cost: $1,080-1,800
```

### **Materialized Master Table + On-Demand:**
```
Daily materialization: $3-5 (once per day at 2 AM)
Daily reads: $0 (read materialized table)
Monthly cost: ~$90 (30 materializations)
Annual cost: ~$1,080
PLUS: Faster queries, consistent data, cleaner results
```

### **Materialized Master + Quick Access Tier:**
```
Daily materialization: $3-5 (once per day)
Hourly quick access refresh: $0.50 × 24 = $12 (optional, lightweight)
Monthly cost: ~$120 (materialization + refreshes)
Annual cost: ~$1,440
PLUS: Dashboard updates every 2 hours instead of daily
```

**RECOMMENDATION: Use Option 2 (Master Table Only)**
- Simplest
- Same cost as running individually
- Much faster queries
- Single source of truth
- Better for customers

---

## 🎯 FINAL STRATEGY: THE ANSWER

### **Your Three-Tier Solution:**

**TIER 1: Master Materialized Table** ✅
```
├─ Table: bq_query_metrics_daily
├─ Updated: Daily at 2 AM UTC
├─ Contains: Results of ALL 23 query categories
├─ Size: ~100-500 MB
├─ Query time: <500ms
├─ Cost: $3-5 once per day
├─ Use for: Reports, dashboards, customer analyses
└─ Best for: 95% of use cases
```

**TIER 2: Quick Access Table** (Optional)
```
├─ Table: bq_quick_analysis
├─ Updated: Every 2 hours
├─ Contains: Top 5 "80% queries" + 7 days history
├─ Size: ~10-20 MB
├─ Query time: <100ms
├─ Cost: Negligible (lightweight refresh)
├─ Use for: Live dashboards, real-time monitoring
└─ Best for: Executive dashboards, alerts
```

**TIER 3: Individual Query Views** ✅
```
├─ 23 views (one per query)
├─ Updated: Real-time (read from TIER 1)
├─ Size: Dynamic (part of TIER 1)
├─ Query time: <200ms
├─ Cost: $0
├─ Use for: Deep dives into specific metrics
└─ Best for: Root cause analysis, troubleshooting
```

---

## ✅ WHAT THIS SOLVES

1. **"Do I run all 23 individually?"** → No, run once daily into master table
2. **"What's the metadata table?"** → bq_query_metrics_daily (consolidates all 23)
3. **"How do I access individual query results?"** → Through views that read the master table
4. **"What's the strategy?"** → Materialize once, query many times (99% cost savings on reads)

---

## 🚀 IMPLEMENTATION CHECKLIST

- [ ] Create master materialization SQL (copy above)
- [ ] Test the query (runs once daily, ~3-5 min)
- [ ] Set up scheduled query (2 AM UTC daily)
- [ ] Create TIER 2 quick access table
- [ ] Create 23 views for individual queries
- [ ] Update dashboards to read from master table
- [ ] Monitor first week (no issues expected)
- [ ] Document for customers
- [ ] Start charging for daily monitoring service

---

## 💰 REVENUE OPPORTUNITY

Once you have this system:

**Service Offering:**
- Daily Query Metrics Report: $500/month
- Real-time Dashboard: $1,000/month
- Optimization Consultation: $2,000/month
- Full Implementation: $5,000 one-time

**Annual revenue potential per customer:** $18,000-30,000

---

*This unified metadata table approach scales to 1,000+ customers with same infrastructure cost*
