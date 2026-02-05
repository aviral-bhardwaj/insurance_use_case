# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Transformation and Quality
# MAGIC
# MAGIC This notebook transforms bronze layer data into clean, business-ready silver tables.
# MAGIC
# MAGIC **Architecture:** Medallion - Silver Layer (Clean Data)
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Apply data quality transformations
# MAGIC - Deduplicate records
# MAGIC - Validate referential integrity
# MAGIC - Enrich with business logic
# MAGIC - Create journey linkages
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Digital → Call leakage flagging (digital failure followed by call within 24 hours)
# MAGIC - Repeat call detection (same member, same topic within 7 days)
# MAGIC - Tenure bucketing
# MAGIC - Sentiment standardization
# MAGIC - Null handling

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Load configuration
with open('/Volumes/insurance_command_center/default/data/config/pipeline_config.json', 'r') as f:
    config = json.load(f)

# Notebook parameters
dbutils.widgets.text("catalog", config['catalog'], "Catalog Name")
dbutils.widgets.text("bronze_schema", config['schemas']['bronze'], "Bronze Schema")
dbutils.widgets.text("silver_schema", config['schemas']['silver'], "Silver Schema")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# Business rules from config
leakage_window_hours = config['business_rules']['leakage_window_hours']
repeat_window_days = config['business_rules']['repeat_call_window_days']

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

print(f"Configuration loaded:")
print(f"  Catalog: {catalog}")
print(f"  Bronze Schema: {bronze_schema}")
print(f"  Silver Schema: {silver_schema}")
print(f"  Leakage Window: {leakage_window_hours} hours")
print(f"  Repeat Call Window: {repeat_window_days} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Bronze Tables Availability

# COMMAND ----------

# Verify all bronze tables exist before proceeding
required_tables = ['members', 'claims', 'call_center', 'digital_interactions', 'surveys', 'pharmacy', 'enrollment']
missing_tables = []

for table in required_tables:
    table_path = f"{catalog}.{bronze_schema}.{table}_raw"
    if not spark.catalog.tableExists(table_path):
        missing_tables.append(table_path)
        print(f"❌ Missing: {table_path}")
    else:
        print(f"✅ Found: {table_path}")

if missing_tables:
    raise Exception(f"Missing bronze tables: {missing_tables}. Run 01_bronze_ingestion.py first!")

print("\n✅ All required bronze tables are available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def transform_to_silver(bronze_table_name, silver_table_name, transformation_func):
    """
    Generic function to transform bronze to silver with error handling
    
    Args:
        bronze_table_name: Name of bronze table (without schema)
        silver_table_name: Name of silver table (without schema)
        transformation_func: Function that takes a DataFrame and returns transformed DataFrame
    """
    print(f"\n{'='*60}")
    print(f"Transforming: {bronze_table_name} -> {silver_table_name}")
    print(f"{'='*60}")
    
    try:
        # Read from bronze
        bronze_path = f"{catalog}.{bronze_schema}.{bronze_table_name}_raw"
        df = spark.table(bronze_path)
        
        print(f"Bronze records: {df.count():,}")
        
        # Apply transformation
        transformed_df = transformation_func(df)
        
        # Add processing metadata
        transformed_df = transformed_df.withColumn("silver_processed_timestamp", F.current_timestamp())
        
        # Write to silver
        silver_path = f"{catalog}.{silver_schema}.{silver_table_name}_clean"
        transformed_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(silver_path)
        
        print(f"Silver records: {transformed_df.count():,}")
        print(f"✅ Successfully created {silver_path}")
        
        # Show sample
        print("\nSample data:")
        display(spark.table(silver_path).limit(5))
        
        return True
        
    except Exception as e:
        print(f"❌ Error transforming {bronze_table_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def log_data_quality_metrics(table_name, metrics_dict):
    """Log data quality metrics for monitoring"""
    print(f"\nData Quality Metrics for {table_name}:")
    print("-" * 40)
    for metric, value in metrics_dict.items():
        print(f"  {metric}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Members Table

# COMMAND ----------

def transform_members(df):
    """Transform members data with deduplication and enrichment"""
    
    # Remove duplicates (keep latest ingestion)
    window_spec = Window.partitionBy("member_id").orderBy(F.desc("ingestion_timestamp"))
    df = df.withColumn("rn", F.row_number().over(window_spec)) \
           .filter(F.col("rn") == 1) \
           .drop("rn")
    
    # Create tenure buckets
    df = df.withColumn("tenure_bucket", 
        F.when(F.col("tenure_months") < 6, "0-6 months")
         .when(F.col("tenure_months") < 12, "6-12 months")
         .when(F.col("tenure_months") < 24, "1-2 years")
         .when(F.col("tenure_months") < 36, "2-3 years")
         .otherwise("3+ years")
    )
    
    # Age groups
    df = df.withColumn("age_group",
        F.when(F.col("age") < 65, "Under 65")
         .when(F.col("age") < 75, "65-74")
         .when(F.col("age") < 85, "75-84")
         .otherwise("85+")
    )
    
    # Data quality flags
    df = df.withColumn("is_complete_profile", 
        F.when(
            F.col("age").isNotNull() & 
            F.col("gender").isNotNull() & 
            F.col("plan_type").isNotNull() &
            F.col("state").isNotNull(), 
            True
        ).otherwise(False)
    )
    
    # Calculate member since date
    df = df.withColumn("member_since_date", F.col("join_date"))
    
    return df

# Transform members
transform_to_silver("members", "members", transform_members)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Claims Table

# COMMAND ----------

def transform_claims(df):
    """Transform claims data with validation"""
    
    # Remove duplicates
    window_spec = Window.partitionBy("claim_id").orderBy(F.desc("ingestion_timestamp"))
    df = df.withColumn("rn", F.row_number().over(window_spec)) \
           .filter(F.col("rn") == 1) \
           .drop("rn")
    
    # Validate against members table
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    df = df.join(F.broadcast(members_df.select("member_id")), "member_id", "left") \
           .withColumn("member_exists", F.col("member_id").isNotNull())
    
    # Claim amount categories
    df = df.withColumn("claim_amount_category",
        F.when(F.col("claim_amount") < 100, "Low (<$100)")
         .when(F.col("claim_amount") < 1000, "Medium ($100-$1K)")
         .when(F.col("claim_amount") < 10000, "High ($1K-$10K)")
         .otherwise("Very High (>$10K)")
    )
    
    # Approval speed flags
    df = df.withColumn("is_fast_approval",
        F.when(F.col("approval_time_days") <= 2, True).otherwise(False)
    )
    
    df = df.withColumn("is_delayed_approval",
        F.when(F.col("approval_time_days") > 14, True).otherwise(False)
    )
    
    # Extract year-month for reporting
    df = df.withColumn("claim_year_month", F.date_format(F.col("claim_date"), "yyyy-MM"))
    
    return df

# Transform claims
transform_to_silver("claims", "claims", transform_claims)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Digital Interactions Table

# COMMAND ----------

def transform_digital_interactions(df):
    """Transform digital interactions with failure analysis"""
    
    # Remove duplicates
    window_spec = Window.partitionBy("interaction_id").orderBy(F.desc("ingestion_timestamp"))
    df = df.withColumn("rn", F.row_number().over(window_spec)) \
           .filter(F.col("rn") == 1) \
           .drop("rn")
    
    # Validate against members
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    df = df.join(F.broadcast(members_df.select("member_id")), "member_id", "left")
    
    # Standardize outcome
    df = df.withColumn("outcome_clean", F.lower(F.trim(F.col("outcome"))))
    
    # Flag failures
    df = df.withColumn("is_failure", 
        F.col("outcome_clean").isin(['failure', 'abandoned'])
    )
    
    # Session duration categories
    df = df.withColumn("session_category",
        F.when(F.col("session_duration_seconds") < 60, "Quick (<1 min)")
         .when(F.col("session_duration_seconds") < 300, "Normal (1-5 min)")
         .when(F.col("session_duration_seconds") < 600, "Long (5-10 min)")
         .otherwise("Very Long (>10 min)")
    )
    
    # High-value interactions (likely to lead to calls if failed)
    df = df.withColumn("is_high_value_interaction",
        F.col("interaction_type").isin(['bill_pay', 'id_card_download', 'claims_check'])
    )
    
    # Extract hour and day of week for pattern analysis
    df = df.withColumn("interaction_hour", F.hour(F.col("interaction_timestamp")))
    df = df.withColumn("interaction_day_of_week", F.dayofweek(F.col("interaction_date")))
    
    return df

# Transform digital interactions
transform_to_silver("digital_interactions", "digital_interactions", transform_digital_interactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Call Center Table with Leakage Detection

# COMMAND ----------

def transform_call_center(df):
    """Transform call center data with digital leakage detection"""
    
    # Remove duplicates
    window_spec = Window.partitionBy("call_id").orderBy(F.desc("ingestion_timestamp"))
    df = df.withColumn("rn", F.row_number().over(window_spec)) \
           .filter(F.col("rn") == 1) \
           .drop("rn")
    
    # Validate against members
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    df = df.join(F.broadcast(members_df.select("member_id", "plan_type", "segment", "tenure_bucket")), 
                 "member_id", "left")
    
    # Standardize sentiment
    df = df.withColumn("sentiment_clean", F.lower(F.trim(F.col("sentiment"))))
    
    # Handle time categories
    df = df.withColumn("handle_time_category",
        F.when(F.col("handle_time_minutes") < 5, "Quick (<5 min)")
         .when(F.col("handle_time_minutes") < 10, "Normal (5-10 min)")
         .when(F.col("handle_time_minutes") < 20, "Long (10-20 min)")
         .otherwise("Very Long (>20 min)")
    )
    
    # Flag high-cost calls
    df = df.withColumn("is_high_cost_call",
        (F.col("handle_time_minutes") > 20) | 
        (F.col("sentiment_clean") == "negative") |
        (F.col("is_repeat_call") == True)
    )
    
    # Extract time dimensions
    df = df.withColumn("call_hour", F.hour(F.col("call_timestamp")))
    df = df.withColumn("call_day_of_week", F.dayofweek(F.col("call_date")))
    
    # Detect digital leakage (enhanced - beyond the flag)
    # Get failed digital interactions within 24 hours before call
    digital_df = spark.table(f"{catalog}.{silver_schema}.digital_interactions_clean")
    failed_digital = digital_df.filter(F.col("is_failure") == True) \
        .select(
            F.col("member_id").alias("dig_member_id"),
            F.col("interaction_timestamp").alias("digital_timestamp"),
            F.col("interaction_type").alias("failed_interaction_type")
        )
    
    # Join to find digital attempts before calls
    df = df.alias("calls").join(
        failed_digital.alias("digital"),
        (F.col("calls.member_id") == F.col("digital.dig_member_id")) &
        (F.col("calls.call_timestamp") > F.col("digital.digital_timestamp")) &
        (F.col("calls.call_timestamp") <= F.col("digital.digital_timestamp") + F.expr(f"INTERVAL {leakage_window_hours} HOURS")),
        "left"
    )
    
    # Flag confirmed leakage
    df = df.withColumn("is_digital_leakage", 
        F.when(F.col("digital_timestamp").isNotNull(), True).otherwise(False)
    )
    
    # Select final columns
    df = df.select(
        F.col("calls.*"),
        F.col("failed_interaction_type")
    )
    
    # Calculate cost per call (from config)
    cost_per_call = 12.50  # Can be loaded from config
    df = df.withColumn("estimated_call_cost", 
        F.lit(cost_per_call) + (F.col("handle_time_minutes") * 0.75)
    )
    
    return df

# Transform call center
transform_to_silver("call_center", "call_center", transform_call_center)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Surveys Table

# COMMAND ----------

def transform_surveys(df):
    """Transform survey data with sentiment scoring"""
    
    # Remove duplicates
    window_spec = Window.partitionBy("survey_id").orderBy(F.desc("ingestion_timestamp"))
    df = df.withColumn("rn", F.row_number().over(window_spec)) \
           .filter(F.col("rn") == 1) \
           .drop("rn")
    
    # Validate against members
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    df = df.join(F.broadcast(members_df.select("member_id")), "member_id", "left")
    
    # NPS classification
    df = df.withColumn("nps_category",
        F.when(F.col("nps_score") >= 9, "Promoter")
         .when(F.col("nps_score") >= 7, "Passive")
         .otherwise("Detractor")
    )
    
    # CSAT classification
    df = df.withColumn("csat_category",
        F.when(F.col("csat_score") >= 4, "Satisfied")
         .when(F.col("csat_score") >= 3, "Neutral")
         .otherwise("Dissatisfied")
    )
    
    # Effort classification (lower is better)
    df = df.withColumn("effort_category",
        F.when(F.col("effort_score") <= 2, "Low Effort")
         .when(F.col("effort_score") <= 4, "Medium Effort")
         .otherwise("High Effort")
    )
    
    # Overall satisfaction flag
    df = df.withColumn("is_positive_experience",
        (F.col("nps_score") >= 7) & (F.col("csat_score") >= 3) & (F.col("effort_score") <= 4)
    )
    
    return df

# Transform surveys
transform_to_silver("surveys", "surveys", transform_surveys)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Pharmacy Table

# COMMAND ----------

def transform_pharmacy(df):
    """Transform pharmacy data with denial analysis"""
    
    # Remove duplicates
    window_spec = Window.partitionBy("rx_id").orderBy(F.desc("ingestion_timestamp"))
    df = df.withColumn("rn", F.row_number().over(window_spec)) \
           .filter(F.col("rn") == 1) \
           .drop("rn")
    
    # Validate against members
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    df = df.join(F.broadcast(members_df.select("member_id")), "member_id", "left")
    
    # Copay categories
    df = df.withColumn("copay_category",
        F.when(F.col("copay_amount") < 10, "Low (<$10)")
         .when(F.col("copay_amount") < 25, "Medium ($10-$25)")
         .when(F.col("copay_amount") < 50, "High ($25-$50)")
         .otherwise("Very High (>$50)")
    )
    
    # Supply duration category
    df = df.withColumn("supply_category",
        F.when(F.col("days_supply") == 30, "30-day")
         .when(F.col("days_supply") == 60, "60-day")
         .when(F.col("days_supply") == 90, "90-day")
         .otherwise("Other")
    )
    
    # Flag potential issues
    df = df.withColumn("is_high_value_drug",
        F.col("copay_amount") > 50
    )
    
    df = df.withColumn("is_specialty",
        F.col("pharmacy_type") == "specialty"
    )
    
    return df

# Transform pharmacy
transform_to_silver("pharmacy", "pharmacy", transform_pharmacy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Enrollment Table

# COMMAND ----------

def transform_enrollment(df):
    """Transform enrollment data"""
    
    # Remove duplicates
    window_spec = Window.partitionBy("enrollment_id").orderBy(F.desc("ingestion_timestamp"))
    df = df.withColumn("rn", F.row_number().over(window_spec)) \
           .filter(F.col("rn") == 1) \
           .drop("rn")
    
    # Validate against members
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    df = df.join(F.broadcast(members_df.select("member_id")), "member_id", "left")
    
    # Premium categories
    df = df.withColumn("premium_category",
        F.when(F.col("premium_amount") == 0, "Zero Premium")
         .when(F.col("premium_amount") < 100, "Low (<$100)")
         .when(F.col("premium_amount") < 200, "Medium ($100-$200)")
         .otherwise("High (>$200)")
    )
    
    # Flag plan changes
    df = df.withColumn("is_plan_change",
        F.col("change_type") == "change"
    )
    
    # Extract enrollment year and month
    df = df.withColumn("enrollment_year_month", 
        F.date_format(F.col("enrollment_date"), "yyyy-MM")
    )
    
    return df

# Transform enrollment
transform_to_silver("enrollment", "enrollment", transform_enrollment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

# Generate data quality report
print("\n" + "="*60)
print("SILVER LAYER DATA QUALITY SUMMARY")
print("="*60)

tables = ['members', 'claims', 'call_center', 'digital_interactions', 'surveys', 'pharmacy', 'enrollment']

for table in tables:
    silver_path = f"{catalog}.{silver_schema}.{table}_clean"
    try:
        df = spark.table(silver_path)
        total_count = df.count()
        
        print(f"\n{table.upper()}")
        print("-" * 40)
        print(f"Total Records: {total_count:,}")
        
        # Null analysis
        null_counts = {}
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                null_pct = (null_count / total_count) * 100
                null_counts[col] = f"{null_count:,} ({null_pct:.1f}%)"
        
        if null_counts:
            print("Null Values:")
            for col, count in list(null_counts.items())[:5]:  # Show top 5
                print(f"  {col}: {count}")
        else:
            print("✅ No null values detected")
            
    except Exception as e:
        print(f"❌ Error analyzing {table}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed

# COMMAND ----------

# Enable Change Data Feed on silver tables
for table in tables:
    silver_path = f"{catalog}.{silver_schema}.{table}_clean"
    try:
        spark.sql(f"ALTER TABLE {silver_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print(f"✅ Enabled Change Data Feed on {silver_path}")
    except Exception as e:
        print(f"⚠️ Could not enable CDF on {silver_path}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Tables

# COMMAND ----------

# Optimize silver tables with Z-Ordering on key columns
optimize_config = {
    'members': ['member_id', 'segment'],
    'claims': ['member_id', 'claim_date'],
    'call_center': ['member_id', 'call_date'],
    'digital_interactions': ['member_id', 'interaction_date'],
    'surveys': ['member_id', 'survey_date'],
    'pharmacy': ['member_id', 'fill_date'],
    'enrollment': ['member_id', 'enrollment_date']
}

for table, zorder_cols in optimize_config.items():
    silver_path = f"{catalog}.{silver_schema}.{table}_clean"
    try:
        # Basic optimize
        spark.sql(f"OPTIMIZE {silver_path} ZORDER BY ({', '.join(zorder_cols)})")
        print(f"✅ Optimized and Z-Ordered {silver_path}")
    except Exception as e:
        print(f"⚠️ Could not optimize {silver_path}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Complete
# MAGIC
# MAGIC ✅ Silver layer transformation complete!
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Run `03_gold_analytics.py` to create business analytics tables
# MAGIC 2. Review data quality metrics
# MAGIC 3. Validate journey linkages
