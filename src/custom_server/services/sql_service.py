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
    
    def execute_query(self, query: str) -> tuple[List[tuple], List[str]]:
        """
        Execute a SQL query and return raw results with column names.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            Tuple of (results, column_names) where:
            - results: List of tuples containing query results
            - column_names: List of column names
            
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
                    results = cursor.fetchall()
                    # Get column names from cursor description
                    column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                    return results, column_names
        except Exception as e:
            raise Exception(f"Error querying Databricks Warehouse: {e}")
    
    def execute_query_with_formatting(self, query: str, formatter_func=None) -> str:
        """
        Execute a SQL query and format results using a custom formatter function.
        
        Args:
            query: SQL query string to execute
            formatter_func: Function that takes query results and column names, returns formatted string.
                          If None, uses the default formatter.
            
        Returns:
            Formatted string output suitable for language models
        """
        try:
            results, column_names = self.execute_query(query)
            
            if not results:
                return "No data found for the specified query"
            
            # Use default formatter if none provided
            if formatter_func is None:
                formatter_func = QueryFormatter.format_default
            
            return formatter_func(results, column_names)
        except Exception as e:
            return str(e)


class QueryFormatter:
    """Utility class for formatting SQL query results into text for language models."""
    
    @staticmethod
    def format_default(results: List[tuple], column_names: List[str] = None) -> str:
        """Default formatter for SQL query results when no custom formatter is provided."""
        if not results:
            return "No data found"
        
        # Get column count from first row
        if not results[0]:
            return "No data found"
        
        col_count = len(results[0])
        
        # Use provided column names or generate generic ones
        if not column_names or len(column_names) != col_count:
            column_names = [f"column_{i+1}" for i in range(col_count)]
        
        # Find the maximum width for each column (including headers)
        col_widths = [len(name) for name in column_names]
        
        # Calculate column widths based on data
        for row in results:
            for i, val in enumerate(row):
                val_str = str(val) if val is not None else "NULL"
                col_widths[i] = max(col_widths[i], len(val_str))
        
        # Ensure minimum width of 8 for readability
        col_widths = [max(8, width) for width in col_widths]
        
        # Create the formatted table
        output = []
        
        # Create header separator
        separator = "+" + "+".join("-" * (width + 2) for width in col_widths) + "+"
        output.append(separator)
        
        # Add column headers
        header_parts = []
        for i, name in enumerate(column_names):
            padded_name = f" {name:<{col_widths[i]}} "
            header_parts.append(padded_name)
        output.append("|" + "|".join(header_parts) + "|")
        output.append(separator)
        
        # Format each row
        for row_idx, row in enumerate(results):
            if row_idx > 100:  # Limit output to first 100 rows
                output.append(f"| ... and {len(results) - 100} more rows" + " " * (sum(col_widths) + col_count * 3 - 25) + "|")
                break
                
            row_parts = []
            for i, val in enumerate(row):
                val_str = str(val) if val is not None else "NULL"
                padded_val = f" {val_str:<{col_widths[i]}} "
                row_parts.append(padded_val)
            
            output.append("|" + "|".join(row_parts) + "|")
        
        # Add bottom separator
        output.append(separator)
        output.append(f"Total rows: {len(results)}")
        
        return "\n".join(output)


# Default singleton instance
_sql_service = None

def get_sql_service() -> SQLService:
    """Get or create the default SQL service instance."""
    global _sql_service
    if _sql_service is None:
        _sql_service = SQLService()
    return _sql_service
