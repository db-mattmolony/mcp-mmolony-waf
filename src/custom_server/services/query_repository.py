"""
Query Repository Service for storing and retrieving SQL queries.

This service handles:
- Query storage and retrieval
- Query metadata management
- Future integration with external query repositories
"""

from typing import Dict, Optional

class QueryRepository:
    """Repository for storing and retrieving SQL queries used by cost optimization functions."""
    
    def __init__(self):
        """Initialize the query repository with predefined queries."""
        self.queries = {
            "CO-01-01-table-formats": """
                SELECT 
                    data_source_format AS tables_format,
                    count(data_source_format) AS no_of_tables
                FROM system.information_schema.tables 
                GROUP BY ALL
                ORDER BY no_of_tables desc;
            """,
            
            "CO-01-01-managed-tables": """
            SELECT 
            table_type,
            round(count(table_type)/(select count(*) from system.information_schema.tables) * 100) as percent_of_tables
            FROM system.information_schema.tables
            group by ALL
            having percent_of_tables > 0
            order by percent_of_tables desc
            """,
            
            "CO-01-02": """
                with clusters AS (
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
                        t2.owned_by
                    FROM job_tasks_exploded t1
                        INNER JOIN clusters t2 USING (workspace_id, cluster_id)
                )
                SELECT * FROM all_purpose_cluster_jobs LIMIT 10;
            """,
            
            "CO-01-03": """
                SELECT billing_origin_product, sum(usage_quantity) as dbu 
                FROM system.billing.usage 
                WHERE billing_origin_product in ('SQL','ALL_PURPOSE') 
                  AND usage_date >= current_date() - interval 30 days 
                GROUP BY billing_origin_product;
            """,
            
            "CO-01-04": """
                SELECT regexp_extract(dbr_version, '^(\\\\d+\\\\.\\\\d+)',1) as version, count(*) as count
                FROM system.compute.clusters 
                WHERE NOT contains(dbr_version, 'custom') 
                  AND cluster_source NOT IN('PIPELINE','PIPELINE_MAINTENANCE') 
                  AND delete_time IS NULL
                GROUP BY 1
                ORDER BY count DESC;
            """,
            
            "CO-01-06-serverless": """
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
            """,
            
            "CO-01-06-sql": """
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
            """,
            
            "CO-01-08": """
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
            """,
            
            "CO-02-01": """
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
            """,
            
            "CO-02-02": """
                SELECT percentile(c.auto_termination_minutes, 0.75) as p_75_auto_termination_minutes, 
                       max(c.auto_termination_minutes) as max_auto_termination_minutes, 
                       count_if(c.auto_termination_minutes is null) as count_clusters_without_autoterminations, 
                       count_if(c.auto_termination_minutes is not null) as count_clusters_with_autoterminations, 
                       (count_clusters_without_autoterminations*100)/count(*) as percent_clusters_without_autoterminations 
                FROM system.compute.clusters c 
                WHERE c.cluster_source in ('UI','API') 
                  AND c.delete_time IS NULL;
            """,
            
            "CO-03-01": """
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
            """,
            
            "CO-03-02-tagging": """
                SELECT array_size(map_entries(tags)) as number_of_tags, count(*) as count 
                FROM system.compute.clusters 
                WHERE tags.ResourceClass IS NULL 
                  AND delete_time IS NULL
                GROUP BY number_of_tags
                ORDER BY count DESC, number_of_tags DESC;
            """,
            
            "CO-03-02-popular": """
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
            """
        }
    
    def get_query(self, query_name: str) -> Optional[str]:
        """
        Retrieve a SQL query by name.
        
        Args:
            query_name: Name of the query to retrieve
            
        Returns:
            SQL query string or None if not found
        """
        return self.queries.get(query_name)
    
    def add_query(self, query_name: str, query: str) -> None:
        """
        Add a new query to the repository.
        
        Args:
            query_name: Name for the new query
            query: SQL query string
        """
        self.queries[query_name] = query
    
    def list_queries(self) -> Dict[str, str]:
        """
        Get all available queries.
        
        Returns:
            Dictionary of query names and their SQL strings
        """
        return self.queries.copy()


# Default singleton instance
_query_repository = None

def get_query_repository() -> QueryRepository:
    """Get or create the default query repository instance."""
    global _query_repository
    if _query_repository is None:
        _query_repository = QueryRepository()
    return _query_repository
