# Databricks MCP – Synthetic Data Metadata Setup

## Use Case
This MCP server enables an LLM to generate synthetic datasets inside Databricks in a structured and governed way.  
The flow is:
1. Create a catalog and schema to house new datasets.  
2. Populate metadata tables that describe schemas, tables, and columns.  
3. Use this metadata as the foundation for synthetic data generation.

## Metadata Tables
Three core tables are required in every schema:

1. **_schema_metadata**  
   - Stores schema-level information  
   - Columns: unique schema identifier, schema name, description  

2. **_table_metadata**  
   - Stores table-level information  
   - Columns: table identifier, schema identifier, table name, table description, column name, column type, column description  

3. **_string_categories**  
   - Stores category values for string-typed columns in `_table_metadata`  
   - Columns: category identifier, table identifier, column name, category value  

These tables are used to drive synthetic data creation, ensuring datasets are consistent, reproducible, and self-describing.