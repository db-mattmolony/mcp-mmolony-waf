from pathlib import Path
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
from fastapi.responses import FileResponse
from databricks.sdk.core import Config
from databricks import sql
from .prompts import load_prompts
import os
from dotenv import load_dotenv
from fastapi import Header
from typing import Optional



# Load environment variables from .env file
load_dotenv() 

cfg = Config()

user_token = Header(None, alias="X-Forwarded-Access-Token")

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
def create_catalog(catalog_name: str) -> str:
    """
    Create a new catalog in Databricks using a SQL warehouse
    """
    try:
        query = f"CREATE CATALOG IF NOT EXISTS {catalog_name};"
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            # credentials_provider=lambda: cfg.authenticate,
            access_token=user_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return f"Catalog {catalog_name} created successfully"
    except Exception as e:
        return f"Error querying Databricks Warehouse: {e}"
    
@mcp.tool()
def create_schema(catalog_name: str, schema_name: str) -> str:
    """
    Create a new schema in Databricks catalog using a SQL warehouse
    """
    try:
        query = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};"
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            # credentials_provider=lambda: cfg.authenticate
            access_token=user_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return f"Schema {schema_name} created successfully"
    except Exception as e:
        return f"Error querying Databricks Warehouse: {e}"
    
    
@mcp.tool()
def create_metadata_tables(catalog_name: str, schema_name: str) -> str:
    """
    Create the three synthetic data metadata tables in the specified catalog and schema.
    Creates _synthetic_schema_metadata, _synthetic_table_metadata, and _string_categories tables.
    """
    try:
        # SQL queries to create the three metadata tables
        queries = [
            f"""
            CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}._synthetic_schema_metadata (
                table_name STRING COMMENT 'Name of the table in the schema',
                description STRING COMMENT 'Detailed description of the table purpose and contents'
            ) USING DELTA
            COMMENT 'Schema-level metadata describing tables and their purposes for synthetic data generation'
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}._synthetic_table_metadata (
                table_name STRING COMMENT 'Name of the table this metadata describes',
                column_name STRING COMMENT 'Name of the column in the table',
                data_type STRING COMMENT 'Data type of the column (string, integer, bigint, etc.)',
                description STRING COMMENT 'Detailed description of what this column contains'
            ) USING DELTA
            COMMENT 'Table and column-level metadata describing the structure and meaning of each column for synthetic data generation'
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}._string_categories (
                column_name STRING COMMENT 'Name of the string/categorical column',
                table_name STRING COMMENT 'Name of the table containing this column',
                data_type STRING COMMENT 'Data type of the column (typically string for categorical data)',
                description STRING COMMENT 'Description of what this categorical column represents',
                cardinality ARRAY<STRING> COMMENT 'Array of all possible categorical values for this column',
                no_of_levels BIGINT COMMENT 'Total number of distinct categorical values (cardinality count)'
            ) USING DELTA
            COMMENT 'Categorical data metadata storing all possible values for string columns to ensure consistent synthetic data generation'
            """
        ]
        
        results = []
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            # credentials_provider=lambda: cfg.authenticate
            access_token=user_token
        ) as connection:
            with connection.cursor() as cursor:
                for i, query in enumerate(queries):
                    cursor.execute(query)
                    table_names = ["_synthetic_schema_metadata", "_synthetic_table_metadata", "_string_categories"]
                    results.append(f"✓ Created table {catalog_name}.{schema_name}.{table_names[i]}")
                
        return f"Successfully created metadata tables in {catalog_name}.{schema_name}:\n" + "\n".join(results)
        
    except Exception as e:
        return f"Error creating metadata tables: {e}"
    


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"


mcp_app = mcp.streamable_http_app()


app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html")

# @app.get("/example")
# async def example(user_token: Optional[str] = Header(None, alias="X-Forwarded-Access-Token")):
#     return {"user_token": user_token}


app.mount("/", mcp_app)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "custom_server.app:app",
        host="0.0.0.0", 
        port=8000,
        reload=True
    )
