"""
SQL Service for Databricks operations.

This service handles:
- Database connection management
- Query execution 
- Result formatting for language models
- Error handling
"""

import os
from typing import List, Dict, Any, Optional
from databricks.sdk.core import Config  
from databricks import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class SQLService:
    """Service for executing SQL queries and formatting results for language models."""
    
    def __init__(self, config: Optional[Config] = None):
        """Initialize the SQL service with Databricks configuration."""
        self.config = config or Config()
        self.warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
        
        if not self.warehouse_id:
            raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable is required")
    
    def execute_query(self, query: str) -> List[tuple]:
        """
        Execute a SQL query and return raw results.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            List of tuples containing query results
            
        Raises:
            Exception: If query execution fails
        """
        try:
            with sql.connect(
                server_hostname=self.config.host,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                credentials_provider=lambda: self.config.authenticate,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error querying Databricks Warehouse: {e}")
    
    def execute_query_with_formatting(self, query: str, formatter_func) -> str:
        """
        Execute a SQL query and format results using a custom formatter function.
        
        Args:
            query: SQL query string to execute
            formatter_func: Function that takes query results and returns formatted string
            
        Returns:
            Formatted string output suitable for language models
        """
        try:
            results = self.execute_query(query)
            
            if not results:
                return "No data found for the specified query"
            
            return formatter_func(results)
        except Exception as e:
            return str(e)


class QueryFormatter:
    """Utility class for formatting SQL query results into text for language models."""
    
    @staticmethod
    def format_table_formats(results: List[tuple]) -> str:
        """Format table format query results."""
        output = "Table formats:\n"
        for row in results:
            format_name = row[0] if row[0] else "Unknown"
            table_count = row[1]
            output += f"{format_name}: {table_count} tables\n"
        return output.strip()
    
    @staticmethod
    def format_table_types_distribution(results: List[tuple]) -> str:
        """Format table types distribution query results."""
        output = "Table types distribution:\n"
        for row in results:
            table_type = row[0] if row[0] else "Unknown"
            percentage = row[1]
            output += f"{table_type}: {percentage}%\n"
        return output.strip()
    
    @staticmethod
    def format_jobs_on_all_purpose_clusters(results: List[tuple]) -> str:
        """Format jobs running on all-purpose clusters query results."""
        output = "Jobs running on all-purpose clusters (last 30 days):\n"
        for row in results:
            workspace_id = row[0] if row[0] else "Unknown"
            job_id = row[1] if row[1] else "Unknown"
            cluster_id = row[2] if row[2] else "Unknown"
            cluster_name = row[3] if row[3] else "Unknown"
            owned_by = row[4] if row[4] else "Unknown"
            output += f"Job {job_id} on cluster '{cluster_name}' (ID: {cluster_id}) - Owner: {owned_by}\n"
        return output.strip()
    
    @staticmethod
    def format_sql_vs_all_purpose(results: List[tuple]) -> str:
        """Format SQL vs All Purpose compute usage query results."""
        output = "SQL vs All Purpose compute usage (last 30 days):\n"
        for row in results:
            billing_product = row[0] if row[0] else "Unknown"
            dbu_usage = row[1] if row[1] else 0
            output += f"{billing_product}: {dbu_usage:.2f} DBU\n"
        return output.strip()
    
    @staticmethod
    def format_dbr_versions(results: List[tuple]) -> str:
        """Format Databricks Runtime versions query results."""
        output = "Databricks Runtime versions in use:\n"
        for row in results:
            version = row[0] if row[0] else "Unknown"
            count = row[1] if row[1] else 0
            output += f"DBR {version}: {count} clusters\n"
        return output.strip()
    
    @staticmethod
    def format_serverless_percentage(results: List[tuple]) -> str:
        """Format serverless compute percentage query results."""
        serverless_percent = results[0][0] if results and results[0][0] else 0
        return f"Serverless compute usage: {serverless_percent:.2f}% of total compute (last 28 days)"
    
    @staticmethod
    def format_sql_compute_costs(results: List[tuple]) -> str:
        """Format SQL compute costs by type query results."""
        output = "SQL compute costs by type (last 30 days):\n"
        for row in results:
            sql_type = row[0] if row[0] else "Unknown"
            cost = row[1] if row[1] else 0
            output += f"{sql_type}: ${cost:.2f}\n"
        return output.strip()
    
    @staticmethod
    def format_cluster_utilization(results: List[tuple]) -> str:
        """Format cluster utilization query results."""
        cpu_p75 = results[0][0] if results and results[0][0] else 0
        memory_p75 = results[0][1] if results and results[0][1] else 0
        return f"Cluster utilization (75th percentile, last 28 days):\nCPU: {cpu_p75:.2f}%\nMemory: {memory_p75:.2f}%"
    
    @staticmethod
    def format_autoscaling_percentage(results: List[tuple]) -> str:
        """Format autoscaling percentage query results."""
        autoscaling_percent = results[0][0] if results and results[0][0] else 0
        return f"Clusters with autoscaling enabled: {autoscaling_percent:.2f}%"
    
    @staticmethod
    def format_auto_termination_analysis(results: List[tuple]) -> str:
        """Format auto-termination analysis query results."""
        row = results[0]
        p75_minutes = row[0] if row[0] else "N/A"
        max_minutes = row[1] if row[1] else "N/A"
        without_auto = row[2] if row[2] else 0
        with_auto = row[3] if row[3] else 0
        percent_without = row[4] if row[4] else 0
        
        output = f"Auto-termination analysis:\n"
        output += f"75th percentile auto-termination: {p75_minutes} minutes\n"
        output += f"Max auto-termination: {max_minutes} minutes\n"
        output += f"Clusters without auto-termination: {without_auto} ({percent_without:.1f}%)\n"
        output += f"Clusters with auto-termination: {with_auto}"
        return output
    
    @staticmethod
    def format_billing_table_access(results: List[tuple]) -> str:
        """Format billing table access query results."""
        output = "Daily billing table access (last 90 days):\n"
        for row in results[:10]:  # Limit to first 10 days for readability
            event_date = row[0] if row[0] else "Unknown"
            usage_count = row[1] if row[1] else 0
            output += f"{event_date}: {usage_count} reads\n"
        
        if len(results) > 10:
            output += f"... and {len(results) - 10} more days"
        
        return output.strip()
    
    @staticmethod
    def format_cluster_tagging_distribution(results: List[tuple]) -> str:
        """Format cluster tagging distribution query results."""
        output = "Cluster tagging distribution:\n"
        for row in results:
            tag_count = row[0] if row[0] else 0
            cluster_count = row[1] if row[1] else 0
            output += f"{tag_count} tags: {cluster_count} clusters\n"
        return output.strip()
    
    @staticmethod
    def format_popular_tags(results: List[tuple]) -> str:
        """Format popular cluster tags query results."""
        output = "Most popular cluster tags:\n"
        for row in results[:10]:  # Limit to top 10 tags
            tag_name = row[0] if row[0] else "Unknown"
            percentage = row[1] if row[1] else 0
            output += f"{tag_name}: {percentage:.1f}% of clusters\n"
        return output.strip()


# Default singleton instance
_sql_service = None

def get_sql_service() -> SQLService:
    """Get or create the default SQL service instance."""
    global _sql_service
    if _sql_service is None:
        _sql_service = SQLService()
    return _sql_service
