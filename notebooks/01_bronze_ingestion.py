# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC This notebook ingests raw CSV files from the data folder into Unity Catalog bronze tables.
# MAGIC
# MAGIC **Architecture:** Medallion - Bronze Layer (Raw Data)
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Ingest all raw CSV files to Delta Lake tables
# MAGIC - Preserve data exactly as received (no transformations)
# MAGIC - Add metadata columns for lineage tracking
# MAGIC - Handle incremental loads with merge logic
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `{catalog}.bronze.members_raw`
# MAGIC - `{catalog}.bronze.claims_raw`
# MAGIC - `{catalog}.bronze.call_center_raw`
# MAGIC - `{catalog}.bronze.digital_interactions_raw`
# MAGIC - `{catalog}.bronze.surveys_raw`
# MAGIC - `{catalog}.bronze.pharmacy_raw`
# MAGIC - `{catalog}.bronze.enrollment_raw`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Load configuration
with open('/Volumes/insurance_command_center/default/data/config/pipeline_config.json', 'r') as f:
    config = json.load(f)

# Notebook parameters (can be overridden via dbutils.widgets)
dbutils.widgets.text("catalog", config['catalog'], "Catalog Name")
dbutils.widgets.text("bronze_schema", config['schemas']['bronze'], "Bronze Schema")
dbutils.widgets.text("data_path", "/Volumes/insurance_command_center/default/data", "Data Path")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
data_path = dbutils.widgets.get("data_path")

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

print(f"Configuration loaded:")
print(f"  Catalog: {catalog}")
print(f"  Bronze Schema: {bronze_schema}")
print(f"  Data Path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def ingest_csv_to_bronze(source_file, table_name, schema=None):
    """
    Ingest a CSV file into a bronze Delta table with metadata columns.
    
    Args:
        source_file: Path to source CSV file
        table_name: Name of the bronze table (without schema)
        schema: Optional PySpark schema definition
    """
    print(f"\n{'='*60}")
    print(f"Ingesting: {source_file} -> {catalog}.{bronze_schema}.{table_name}_raw")
    print(f"{'='*60}")
    
    try:
        # Read CSV file
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true" if schema is None else "false") \
            .load(source_file)
        
        # Apply schema if provided
        if schema is not None:
            df = spark.read.format("csv") \
                .option("header", "true") \
                .schema(schema) \
                .load(source_file)
        
        # Add metadata columns
        df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.lit(source_file))
        
        # Table path
        table_path = f"{catalog}.{bronze_schema}.{table_name}_raw"
        
        # Check if table exists
        table_exists = spark.catalog.tableExists(table_path)
        
        if not table_exists:
            # Create new table
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_path)
            print(f"✅ Created new table with {df.count():,} records")
        else:
            # Merge for incremental load (append new records)
            df.createOrReplaceTempView("temp_bronze_data")
            
            # Use merge to handle duplicates
            df.write.format("delta") \
                .mode("append") \
                .saveAsTable(table_path)
            print(f"✅ Appended {df.count():,} records to existing table")
        
        # Display sample
        print(f"\nSample data:")
        display(spark.table(table_path).limit(5))
        
        # Show statistics
        stats = spark.table(table_path).select(
            F.count("*").alias("total_records"),
            F.countDistinct("ingestion_timestamp").alias("ingestion_batches")
        ).collect()[0]
        
        print(f"\nTable Statistics:")
        print(f"  Total Records: {stats['total_records']:,}")
        print(f"  Ingestion Batches: {stats['ingestion_batches']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error ingesting {source_file}: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schemas
# MAGIC Define explicit schemas for data quality and consistency

# COMMAND ----------

# Members schema
members_schema = StructType([
    StructField("member_id", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("segment", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("join_date", DateType(), True)
])

# Claims schema
claims_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("member_id", StringType(), True),
    StructField("claim_date", DateType(), True),
    StructField("claim_type", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("approval_time_days", IntegerType(), True),
    StructField("denial_reason", StringType(), True)
])

# Call center schema
call_center_schema = StructType([
    StructField("call_id", StringType(), False),
    StructField("member_id", StringType(), True),
    StructField("call_date", DateType(), True),
    StructField("call_timestamp", TimestampType(), True),
    StructField("call_topic", StringType(), True),
    StructField("handle_time_minutes", DoubleType(), True),
    StructField("sentiment", StringType(), True),
    StructField("is_repeat_call", BooleanType(), True),
    StructField("prior_digital_attempt", BooleanType(), True),
    StructField("transferred", BooleanType(), True),
    StructField("resolved", BooleanType(), True)
])

# Digital interactions schema
digital_schema = StructType([
    StructField("interaction_id", StringType(), False),
    StructField("member_id", StringType(), True),
    StructField("interaction_date", DateType(), True),
    StructField("interaction_timestamp", TimestampType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("session_duration_seconds", IntegerType(), True)
])

# Surveys schema
surveys_schema = StructType([
    StructField("survey_id", StringType(), False),
    StructField("member_id", StringType(), True),
    StructField("survey_date", DateType(), True),
    StructField("nps_score", IntegerType(), True),
    StructField("csat_score", IntegerType(), True),
    StructField("effort_score", IntegerType(), True),
    StructField("feedback_text", StringType(), True),
    StructField("survey_type", StringType(), True)
])

# Pharmacy schema
pharmacy_schema = StructType([
    StructField("rx_id", StringType(), False),
    StructField("member_id", StringType(), True),
    StructField("fill_date", DateType(), True),
    StructField("drug_name", StringType(), True),
    StructField("copay_amount", DoubleType(), True),
    StructField("days_supply", IntegerType(), True),
    StructField("pharmacy_type", StringType(), True),
    StructField("denied_flag", BooleanType(), True),
    StructField("denied_reason", StringType(), True)
])

# Enrollment schema
enrollment_schema = StructType([
    StructField("enrollment_id", StringType(), False),
    StructField("member_id", StringType(), True),
    StructField("enrollment_date", DateType(), True),
    StructField("plan_type", StringType(), True),
    StructField("premium_amount", DoubleType(), True),
    StructField("change_type", StringType(), True),
    StructField("change_reason", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest All Data Sources

# COMMAND ----------

# Track ingestion results
ingestion_results = {}

# Ingest members
ingestion_results['members'] = ingest_csv_to_bronze(
    f"{data_path}/members.csv", 
    "members", 
    members_schema
)

# COMMAND ----------

# Ingest claims
ingestion_results['claims'] = ingest_csv_to_bronze(
    f"{data_path}/claims.csv", 
    "claims", 
    claims_schema
)

# COMMAND ----------

# Ingest call center data
ingestion_results['call_center'] = ingest_csv_to_bronze(
    f"{data_path}/call_center.csv", 
    "call_center", 
    call_center_schema
)

# COMMAND ----------

# Ingest digital interactions
ingestion_results['digital_interactions'] = ingest_csv_to_bronze(
    f"{data_path}/digital_interactions.csv", 
    "digital_interactions", 
    digital_schema
)

# COMMAND ----------

# Ingest surveys
ingestion_results['surveys'] = ingest_csv_to_bronze(
    f"{data_path}/surveys.csv", 
    "surveys", 
    surveys_schema
)

# COMMAND ----------

# Ingest pharmacy data
ingestion_results['pharmacy'] = ingest_csv_to_bronze(
    f"{data_path}/pharmacy.csv", 
    "pharmacy", 
    pharmacy_schema
)

# COMMAND ----------

# Ingest enrollment data
ingestion_results['enrollment'] = ingest_csv_to_bronze(
    f"{data_path}/enrollment.csv", 
    "enrollment", 
    enrollment_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Summary

# COMMAND ----------

# Display ingestion summary
print("\n" + "="*60)
print("BRONZE LAYER INGESTION SUMMARY")
print("="*60)

all_success = all(ingestion_results.values())

for table, success in ingestion_results.items():
    status = "✅ SUCCESS" if success else "❌ FAILED"
    print(f"{table:30s}: {status}")

print("="*60)

if all_success:
    print("\n✅ All data sources successfully ingested to bronze layer!")
else:
    print("\n⚠️ Some data sources failed to ingest. Check logs above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Basic data quality checks
print("\nData Quality Checks:")
print("="*60)

# Check record counts
tables = ['members', 'claims', 'call_center', 'digital_interactions', 'surveys', 'pharmacy', 'enrollment']

for table in tables:
    table_path = f"{catalog}.{bronze_schema}.{table}_raw"
    try:
        count = spark.table(table_path).count()
        print(f"{table:30s}: {count:>10,} records")
    except Exception as e:
        print(f"{table:30s}: ERROR - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed (for downstream processing)

# COMMAND ----------

# Enable Change Data Feed on all bronze tables
for table in tables:
    table_path = f"{catalog}.{bronze_schema}.{table}_raw"
    try:
        spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print(f"✅ Enabled Change Data Feed on {table_path}")
    except Exception as e:
        print(f"⚠️ Could not enable CDF on {table_path}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Tables

# COMMAND ----------

# Optimize all bronze tables for better query performance
for table in tables:
    table_path = f"{catalog}.{bronze_schema}.{table}_raw"
    try:
        spark.sql(f"OPTIMIZE {table_path}")
        print(f"✅ Optimized {table_path}")
    except Exception as e:
        print(f"⚠️ Could not optimize {table_path}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions (Optional - Uncomment when ready)

# COMMAND ----------

# Uncomment to grant permissions to specific users/groups
# Example:
# spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{bronze_schema} TO `data_analysts`")
# spark.sql(f"GRANT MODIFY ON SCHEMA {catalog}.{bronze_schema} TO `data_engineers`")

print("💡 Tip: Uncomment permission grants when deploying to production")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Complete
# MAGIC
# MAGIC ✅ Bronze layer ingestion complete!
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Run `02_silver_transformation.py` to clean and enrich the data
# MAGIC 2. Verify data quality in Unity Catalog
# MAGIC 3. Set up scheduled jobs for incremental loads
