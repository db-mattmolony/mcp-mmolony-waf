"""
Load WAF CSV files into Unity Catalog tables.

This script creates and populates tables in the db_well_architected_framework catalog
with data from the CSV files in the resources directory.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pathlib import Path

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Configuration
CATALOG = "db_well_architected_framework"
SCHEMA = "waf_data_model"
RESOURCES_PATH = "/Workspace/files/resources"  # Update this to your actual path in Workspace

# Ensure catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Using catalog: {CATALOG}, schema: {SCHEMA}")

# Define schemas for each table
pillars_schema = StructType([
    StructField("pillar_id", StringType(), False),
    StructField("pillar_name", StringType(), False),
    StructField("pillar_description", StringType(), True)
])

principles_schema = StructType([
    StructField("principle_id", StringType(), False),
    StructField("pillar_id", StringType(), False),
    StructField("pillar_name", StringType(), True),
    StructField("principle_description", StringType(), True)
])

measures_schema = StructType([
    StructField("pillar_id", StringType(), False),
    StructField("principle_id", StringType(), False),
    StructField("measure_id", StringType(), False),
    StructField("best_practice", StringType(), True),
    StructField("databricks_capabilities", StringType(), True),
    StructField("details", StringType(), True)
])

analysis_schema = StructType([
    StructField("pillar_id", StringType(), False),
    StructField("principle_id", StringType(), False),
    StructField("measure_id", StringType(), False),
    StructField("analysis_id", StringType(), False),
    StructField("measure_sql_code", StringType(), True),
    StructField("measure_sql_description", StringType(), True)
])

# Load and create tables
tables = [
    {
        "name": "pillars",
        "file": "wafe-life-assessments - pillars.csv",
        "schema": pillars_schema,
        "comment": "WAF Pillars - Top level architectural categories"
    },
    {
        "name": "principles",
        "file": "wafe-life-assessments - principles.csv",
        "schema": principles_schema,
        "comment": "WAF Principles - Architectural guidelines within pillars"
    },
    {
        "name": "measures",
        "file": "wafe-life-assessments - measures.csv",
        "schema": measures_schema,
        "comment": "WAF Measures - Specific best practices and implementation guidance"
    },
    {
        "name": "analyses",
        "file": "wafe-life-assessments - analysis.csv",
        "schema": analysis_schema,
        "comment": "WAF Analyses - SQL queries to evaluate workspace against measures"
    }
]

for table_config in tables:
    table_name = table_config["name"]
    file_name = table_config["file"]
    schema = table_config["schema"]
    comment = table_config["comment"]
    
    print(f"\nProcessing table: {table_name}")
    
    try:
        # Read CSV file
        file_path = f"{RESOURCES_PATH}/{file_name}"
        df = spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("escape", '"') \
            .schema(schema) \
            .csv(file_path)
        
        # Show sample data
        print(f"Loaded {df.count()} rows from {file_name}")
        df.show(5, truncate=False)
        
        # Write to Delta table (overwrite if exists)
        full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_name)
        
        # Add table comment
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{comment}'")
        
        print(f"✓ Successfully created table: {full_table_name}")
        
    except Exception as e:
        print(f"✗ Error creating table {table_name}: {str(e)}")
        raise

print("\n" + "="*80)
print("WAF data loading complete!")
print(f"Tables created in: {CATALOG}.{SCHEMA}")
print("="*80)

# Verify tables
print("\nVerifying tables:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show()

# Show row counts
for table_config in tables:
    table_name = table_config["name"]
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]["count"]
    print(f"  {table_name}: {count} rows")

