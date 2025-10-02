from pathlib import Path
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
from fastapi.responses import FileResponse
from databricks.sdk.core import Config
from .prompts import load_prompts
from .services.sql_service import get_sql_service, QueryFormatter
from .services.query_repository import get_query_repository
import os
from dotenv import load_dotenv
from fastapi import Header
from typing import Optional
from pydantic import Field


# Load environment variables from .env file
load_dotenv() 

cfg = Config()

user_token = Header(None, alias="X-Forwarded-Access-Token")

# Initialize services
sql_service = get_sql_service()
query_repo = get_query_repository()

STATIC_DIR = Path(__file__).parent / "static"

# Create an MCP server
mcp = FastMCP("Custom MCP Server on Databricks Apps for creating")

# Load prompts and tools
load_prompts(mcp)

# Add an addition tool
@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b


@mcp.tool()
def COST_OPTIMISATION_C0_01_01_TABLE_TYPES() -> str:
    """
    CO-01-01 | Use Performance-Optimised Data Formats - Analyzes table formats in workspace to identify cost optimization opportunities
    """
    query = query_repo.get_query("CO-01-01-table-formats")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_table_formats)
    
    
@mcp.tool()
def COST_OPTIMISATION_C0_01_01_MANAGED_TABLES() -> str:
    """
    CO-01-01 | Use Performance-Optimised Data Formats - Analyzes table types in workspace to identify cost optimization opportunities from managed tables, showing the percentage distribution of table types across your data estate
    """
    query = query_repo.get_query("CO-01-01-managed-tables")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_table_types_distribution)   
    
@mcp.tool()
def COST_OPTIMISATION_C0_01_02_JOBS_ON_ALL_PURPOSE_CLUSTERS() -> str:
    """
    CO-01-02 | Use Job Clusters for Non-Interactive Workloads - Analyse jobs running on all purpose clusters to identify cost optimization opportunities by switching to dedicated clusters compute
    """
    query = query_repo.get_query("CO-01-02")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_jobs_on_all_purpose_clusters) 
    

# @mcp.tool()
# def COST_OPTIMISATION_CO_01_03_SQL_VS_ALLPURPOSE() -> str:
#     """
#     CO-01-03 | Use SQL Compute for SQL Workloads - Compares SQL vs All Purpose compute usage to identify cost optimization opportunities
#     """
#     query = query_repo.get_query("CO-01-03")
#     return sql_service.execute_query_with_formatting(query, QueryFormatter.format_sql_vs_all_purpose)


# @mcp.tool()
# def COST_OPTIMISATION_CO_01_03_SQL_ON_ALLPURPOSE() -> str:
#     """
#     CO-01-03 | Use SQL Compute for SQL Workloads - Shows SQL workloads running on All Purpose clusters (Coming Soon)
#     """
#     return "Coming Soon... - SQL workloads running on All Purpose clusters analysis will be available in a future update"


@mcp.tool()
def COST_OPTIMISATION_CO_01_04_LATEST_DBR() -> str:
    """
    CO-01-04 | Use Latest Databricks Runtime - Analyzes DBR versions across clusters to identify upgrade opportunities
    """
    query = query_repo.get_query("CO-01-04")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_dbr_versions)


# @mcp.tool()
# def COST_OPTIMISATION_CO_01_05_GPU() -> str:
#     """
#     CO-01-05 | Optimize GPU Usage - Analyzes GPU usage patterns (Coming Soon)
#     """
#     return "Coming Soon... - GPU usage optimization analysis will be available in a future update"


@mcp.tool()
def COST_OPTIMISATION_CO_01_06_SERVERLESS() -> str:
    """
    CO-01-06 | Use Serverless Compute - Shows percentage of serverless compute usage vs total compute
    """
    query = query_repo.get_query("CO-01-06-serverless")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_serverless_percentage)


@mcp.tool()
def COST_OPTIMISATION_CO_01_06_SERVERLESS_SQL() -> str:
    """
    CO-01-06 | Use Serverless Compute - Compares SQL Serverless vs Classic SQL compute costs
    """
    query = query_repo.get_query("CO-01-06-sql")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_sql_compute_costs)


# @mcp.tool()
# def COST_OPTIMISATION_CO_01_07_INSTANCE_TYPE() -> str:
#     """
#     CO-01-07 | Optimize Instance Types - Analyzes instance type usage patterns (Coming Soon)
#     """
#     return "Coming Soon... - Instance type optimization analysis will be available in a future update"


# @mcp.tool()
# def COST_OPTIMISATION_CO_01_08_CLUSTER_SIZE() -> str:
#     """
#     CO-01-08 | Right-size Clusters - Analyzes cluster sizing patterns (Coming Soon)
#     """
#     return "Coming Soon... - Cluster sizing analysis will be available in a future update"


@mcp.tool()
def COST_OPTIMISATION_CO_01_08_CLUSTER_UTILISATION() -> str:
    """
    CO-01-08 | Right-size Clusters - Analyzes cluster utilization patterns to identify optimization opportunities
    """
    query = query_repo.get_query("CO-01-08")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_cluster_utilization)


# @mcp.tool()
# def COST_OPTIMISATION_CO_01_09_PHOTON() -> str:
#     """
#     CO-01-09 | Use Photon for SQL Workloads - Analyzes Photon usage patterns (Coming Soon)
#     """
#     return "Coming Soon... - Photon usage analysis will be available in a future update"


@mcp.tool()
def COST_OPTIMISATION_CO_02_01_AUTO_SCALING() -> str:
    """
    CO-02-01 | Enable Autoscaling - Shows percentage of clusters with autoscaling enabled
    """
    query = query_repo.get_query("CO-02-01")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_autoscaling_percentage)


@mcp.tool()
def COST_OPTIMISATION_CO_02_02_AUTO_TERMINATION() -> str:
    """
    CO-02-02 | Configure Auto-termination - Analyzes auto-termination settings across clusters
    """
    query = query_repo.get_query("CO-02-02")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_auto_termination_analysis)


# @mcp.tool()
# def COST_OPTIMISATION_CO_02_03_CLUSTER_POLICIES() -> str:
#     """
#     CO-02-03 | Use Cluster Policies - Analyzes cluster policy usage patterns (Coming Soon)
#     """
#     return "Coming Soon... - Cluster policy usage analysis will be available in a future update"


@mcp.tool()
def COST_OPTIMISATION_CO_03_01_BILLING_TABLES() -> str:
    """
    CO-03-01 | Monitor Billing Tables Usage - Shows how frequently billing tables are accessed
    """
    query = query_repo.get_query("CO-03-01")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_billing_table_access)


@mcp.tool()
def COST_OPTIMISATION_CO_03_02_TAGGING_COMPUTE() -> str:
    """
    CO-03-02 | Use Tags for Cost Allocation - Analyzes tagging patterns on compute resources
    """
    query = query_repo.get_query("CO-03-02-tagging")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_cluster_tagging_distribution)


@mcp.tool()
def COST_OPTIMISATION_CO_03_02_POPULAR_TAGS() -> str:
    """
    CO-03-02 | Use Tags for Cost Allocation - Shows most popular tags used across clusters
    """
    query = query_repo.get_query("CO-03-02-popular")
    return sql_service.execute_query_with_formatting(query, QueryFormatter.format_popular_tags)


mcp_app = mcp.streamable_http_app()

app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/", mcp_app)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "custom_server.app:app",
        host="0.0.0.0", 
        port=8000,
        reload=True
    )
