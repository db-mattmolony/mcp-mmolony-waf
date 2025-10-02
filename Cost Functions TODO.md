-- COST_OPTIMISATION_CO_01_01_TABLE_FORMATS
SELECT 
    data_source_format AS tables_format,
    count(data_source_format) AS no_of_tables
FROM system.information_schema.tables 
GROUP BY ALL 
ORDER BY no_of_tables DESC;

-- COST_OPTIMISATION_CO_01_01_MANAGED_TABLES
SELECT 
    table_type,
    round(count(table_type)/(select count(*) from system.information_schema.tables) * 100) as percent_of_tables
FROM system.information_schema.tables
GROUP BY ALL
HAVING percent_of_tables > 0
ORDER BY percent_of_tables DESC;

-- COST_OPTIMISATION_CO_01_02_JOBS_ON_ALL_PURPOSE
WITH clusters AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) as rn
  FROM system.compute.clusters
  WHERE cluster_source="UI" OR cluster_source="API"
  QUALIFY rn=1
),
job_tasks_exploded AS (
  SELECT
    workspace_id,
    job_id,
    EXPLODE(compute_ids) as cluster_id
  FROM system.lakeflow.job_task_run_timeline
  WHERE period_start_time >= CURRENT_DATE() - INTERVAL 30 DAY
  GROUP BY ALL
),
all_purpose_cluster_jobs AS (
  SELECT
    t1.*,
    t2.cluster_name,
    t2.owned_by,
    t2.dbr_version
  FROM job_tasks_exploded t1
    INNER JOIN clusters t2 USING (workspace_id, cluster_id)
)
SELECT * FROM all_purpose_cluster_jobs LIMIT 10;

-- COST_OPTIMISATION_CO_01_03_SQL_VS_ALLPURPOSE
SELECT billing_origin_product, sum(usage_quantity) as dbu 
FROM system.billing.usage 
WHERE billing_origin_product in ('SQL','ALL_PURPOSE') 
  AND usage_date >= current_date() - interval 30 days 
GROUP BY billing_origin_product;

-- COST_OPTIMISATION_CO_01_03_SQL_ON_ALLPURPOSE
SELECT 'Coming Soon...'  as message;

-- COST_OPTIMISATION_CO_01_04_LATEST_DBR
SELECT regexp_extract(dbr_version, '^(\\d+\\.\\d+)',1) as version, count(*) as count
FROM system.compute.clusters 
WHERE NOT contains(dbr_version, 'custom') 
  AND cluster_source NOT IN('PIPELINE','PIPELINE_MAINTENANCE') 
  AND delete_time IS NULL
GROUP BY 1
ORDER BY count DESC;

-- COST_OPTIMISATION_CO_01_05_GPU
SELECT 'Coming Soon...' as message;

-- COST_OPTIMISATION_CO_01_06_SERVERLESS
WITH serverless AS (
    SELECT sum(usage_quantity) as dbu 
    FROM system.billing.usage u 
    WHERE contains(u.sku_name, 'SERVERLESS') 
      AND u.billing_origin_product in ('ALL_PURPOSE','SQL','JOBS', 'DLT','INTERACTIVE') 
      AND date_diff(day, u.usage_start_time, now()) <28
),
total AS (
    SELECT sum(usage_quantity) as dbu 
    FROM system.billing.usage u 
    WHERE u.billing_origin_product in ('ALL_PURPOSE','SQL','JOBS', 'DLT','INTERACTIVE') 
      AND date_diff(day, u.usage_start_time, now()) <28
)
SELECT serverless.dbu * 100 / total.dbu as serverless_dbu_percent 
FROM serverless 
CROSS JOIN total;

-- COST_OPTIMISATION_CO_01_06_SERVERLESS_SQL
SELECT 
CASE 
  WHEN t1.sku_name LIKE '%SERVERLESS_SQL%' THEN 'SQL_SERVERLESS' 
  WHEN t1.sku_name LIKE '%ENTERPRISE_SQL_COMPUTE%' THEN 'SQL_CLASSIC'
  WHEN t1.sku_name LIKE '%SQL_PRO%' THEN 'SQL_PRO'
  ELSE 'Other' 
END as sql_sku_name,
SUM(t1.usage_quantity * list_prices.pricing.default) as list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices 
  ON t1.cloud = list_prices.cloud 
  AND t1.sku_name = list_prices.sku_name 
  AND t1.usage_start_time >= list_prices.price_start_time 
  AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
WHERE t1.sku_name LIKE '%SQL%' 
  AND t1.usage_date >= current_date() - interval 30 days
GROUP BY ALL;

-- COST_OPTIMISATION_CO_01_07_INSTANCE_TYPE
SELECT 'Coming Soon...'  as message;

-- COST_OPTIMISATION_CO_01_08_CLUSTER_SIZE
SELECT 'Coming Soon...'  as message;

-- COST_OPTIMISATION_CO_01_08_CLUSTER_UTILISATION
WITH per_cluster_daily AS (
  SELECT
    cluster_id,
    DATE_TRUNC('DAY', start_time) AS day,
    AVG(cpu_user_percent + cpu_system_percent) AS avg_cpu_usage_percent,
    AVG(mem_used_percent) AS avg_memory_usage_percent
  FROM system.compute.node_timeline
  WHERE start_time >= CURRENT_DATE - INTERVAL 28 DAYS
  GROUP BY cluster_id, DATE_TRUNC('DAY', start_time)
)
SELECT
  percentile(avg_cpu_usage_percent, 0.75) as cpu_usage_percent_p75,
  percentile(avg_memory_usage_percent, 0.75) as memory_usage_percent_p75
FROM per_cluster_daily
GROUP BY ALL;

-- COST_OPTIMISATION_CO_01_09_PHOTON
SELECT 'Coming Soon...'  as message;

-- COST_OPTIMISATION_CO_02_01_AUTO_SCALING
WITH autoscaling_count AS (
  SELECT count(*) as autoscaling_count 
  FROM system.compute.clusters 
  WHERE max_autoscale_workers IS NOT NULL 
    AND delete_time IS NULL
),
total_clusters_count AS (
  SELECT count(*) as total_clusters_count 
  FROM system.compute.clusters 
  WHERE delete_time IS NULL
)
SELECT autoscaling_count.autoscaling_count * 100 / total_clusters_count.total_clusters_count as autoscaling_percent 
FROM total_clusters_count 
CROSS JOIN autoscaling_count;

-- COST_OPTIMISATION_CO_02_02_AUTO_TERMINATION
SELECT percentile(c.auto_termination_minutes, 0.75) as p_75_auto_termination_minutes, 
       max(c.auto_termination_minutes) as max_auto_termination_minutes, 
       count_if(c.auto_termination_minutes is null) as count_clusters_without_autoterminations, 
       count_if(c.auto_termination_minutes is not null) as count_clusters_with_autoterminations, 
       (count_clusters_without_autoterminations*100)/count(*) as percent_clusters_without_autoterminations 
FROM system.compute.clusters c 
WHERE c.cluster_source in ('UI','API') 
  AND c.delete_time IS NULL;

-- COST_OPTIMISATION_CO_02_03_CLUSTER_POLICIES
SELECT 'Coming Soon...'  as message;

-- COST_OPTIMISATION_CO_03_01_BILLING_TABLES
SELECT
  event_date,
  count(*) as usage_read
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'getTable'
  AND request_params.full_name_arg = 'system.billing.usage'
  AND user_identity.email != 'System-User'
  AND (date_diff(day, event_date, current_date()) <= 90)
GROUP BY event_date
ORDER BY event_date;

-- COST_OPTIMISATION_CO_03_02_TAGGING_COMPUTE
SELECT array_size(map_entries(tags)) as number_of_tags, count(*) as count 
FROM system.compute.clusters 
WHERE tags.ResourceClass IS NULL 
  AND delete_time IS NULL
GROUP BY number_of_tags
ORDER BY count DESC, number_of_tags DESC;

-- COST_OPTIMISATION_CO_03_02_POPULAR_TAGS
WITH tag_counts AS (
  SELECT explode(map_keys(tags)) as tag, count(*) as count
  FROM system.compute.clusters
  GROUP BY 1
),
cluster_count AS (SELECT count(*) as count FROM system.compute.clusters)
SELECT tag_counts.tag,
       sum(tag_counts.count) / any_value(cluster_count.count) *100 as percent
FROM tag_counts 
CROSS JOIN cluster_count
GROUP BY tag_counts.tag
ORDER BY percent DESC;