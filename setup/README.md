# WAF Data Setup

This directory contains scripts to load the Well-Architected Framework (WAF) CSV data into Unity Catalog tables.

## Overview

The setup script creates the following structure in Unity Catalog:

```
db_well_architected_framework (catalog)
└── waf_data_model (schema)
    ├── pillars
    ├── principles
    ├── measures
    └── analyses
```

## Prerequisites

1. Access to a Databricks workspace with Unity Catalog enabled
2. Permissions to create catalogs, schemas, and tables
3. CSV files uploaded to DBFS or Workspace Files

## Setup Steps

### Option 1: Using Databricks Bundles (Recommended)

If you have the Databricks CLI and bundles configured:

```bash
# Deploy the bundle (creates catalog, schema, and job)
databricks bundle deploy

# Run the job to load data
databricks bundle run load_waf_data
```

### Option 2: Manual Setup

1. **Upload CSV files to Databricks**
   
   Upload the CSV files from the `resources/` directory to a location in your Databricks workspace:
   - `/Workspace/files/resources/` (recommended)
   - Or update the `RESOURCES_PATH` in `load_waf_tables.py`

2. **Update the script path**
   
   Edit `load_waf_tables.py` and update the `RESOURCES_PATH` variable to match where you uploaded the files.

3. **Run the script**
   
   You can run this as:
   - A notebook in Databricks (copy the Python code into a notebook)
   - A Python wheel job
   - Via the Databricks CLI: `databricks workspace import load_waf_tables.py`

### Option 3: Direct SQL Setup

If you prefer SQL, you can manually create tables and load data using:

```sql
-- Create catalog and schema
CREATE CATALOG IF NOT EXISTS db_well_architected_framework;
USE CATALOG db_well_architected_framework;
CREATE SCHEMA IF NOT EXISTS waf_data_model;
USE SCHEMA waf_data_model;

-- Example: Create pillars table
CREATE TABLE IF NOT EXISTS pillars (
  pillar_id STRING NOT NULL,
  pillar_name STRING NOT NULL,
  pillar_description STRING
) USING DELTA
COMMENT 'WAF Pillars - Top level architectural categories';

-- Load data (adjust path as needed)
COPY INTO pillars
FROM '/Workspace/files/resources/wafe-life-assessments - pillars.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'multiLine' = 'true', 'escape' = '"');
```

## Tables Created

### 1. `pillars`
- **Columns**: `pillar_id`, `pillar_name`, `pillar_description`
- **Description**: The 7 main WAF pillars (Cost Optimization, Data Governance, etc.)

### 2. `principles`
- **Columns**: `principle_id`, `pillar_id`, `pillar_name`, `principle_description`
- **Description**: Architectural guidelines within each pillar (~31 principles)

### 3. `measures`
- **Columns**: `pillar_id`, `principle_id`, `measure_id`, `best_practice`, `databricks_capabilities`, `details`
- **Description**: Specific best practices and implementation guidance (~151 measures)

### 4. `analyses`
- **Columns**: `pillar_id`, `principle_id`, `measure_id`, `analysis_id`, `measure_sql_code`, `measure_sql_description`
- **Description**: SQL queries to evaluate workspace against measures

## Verification

After running the setup, verify the tables were created:

```sql
USE CATALOG db_well_architected_framework;
SHOW TABLES IN waf_data_model;

-- Check row counts
SELECT 'pillars' as table_name, COUNT(*) as row_count FROM waf_data_model.pillars
UNION ALL
SELECT 'principles', COUNT(*) FROM waf_data_model.principles
UNION ALL
SELECT 'measures', COUNT(*) FROM waf_data_model.measures
UNION ALL
SELECT 'analyses', COUNT(*) FROM waf_data_model.analyses;
```

## Updating Data

To refresh the data after updating CSV files:

1. Re-upload the updated CSV files to Databricks
2. Re-run the `load_waf_tables.py` script
3. The script uses `mode="overwrite"` so it will replace existing data

## Troubleshooting

### Error: "Path does not exist"
- Ensure CSV files are uploaded to the correct path in Databricks
- Update `RESOURCES_PATH` in the script to match your file location

### Error: "Catalog does not exist"
- Ensure Unity Catalog is enabled in your workspace
- Verify you have permissions to create catalogs

### Error: "Schema parsing error"
- Check that CSV files have proper headers
- Ensure multiline fields are properly quoted
- Verify CSV encoding is UTF-8

## Architecture

The data follows this hierarchy:
```
Pillar (7)
  └─ Principle (31)
      └─ Measure (151)
          └─ Analysis (multiple per measure)
```

This structure enables the MCP server to provide hierarchical navigation and context-aware analysis execution.

