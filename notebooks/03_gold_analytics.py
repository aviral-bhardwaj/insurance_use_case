# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Analytics Tables
# MAGIC
# MAGIC This notebook creates business-ready analytics tables from the silver layer.
# MAGIC
# MAGIC **Architecture:** Medallion - Gold Layer (Business Analytics)
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Create aggregated business metrics
# MAGIC - Build executive KPI dashboards
# MAGIC - Enable self-service analytics
# MAGIC - Support command center queries
# MAGIC
# MAGIC **Gold Tables Created:**
# MAGIC 1. `digital_call_leakage` - Digital failures that converted to calls
# MAGIC 2. `repeat_calls_analysis` - Repeat call patterns with emotion tracking
# MAGIC 3. `top_opportunities` - Top 5 cost-saving opportunities
# MAGIC 4. `member_journey_timeline` - Cross-channel journey reconstruction
# MAGIC 5. `executive_kpis` - Daily rollup for command center dashboard

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
dbutils.widgets.text("silver_schema", config['schemas']['silver'], "Silver Schema")
dbutils.widgets.text("gold_schema", config['schemas']['gold'], "Gold Schema")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

# Cost assumptions from config
cost_per_call = config['cost_assumptions']['cost_per_call']
cost_per_minute = config['cost_assumptions']['cost_per_minute']

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

print(f"Configuration loaded:")
print(f"  Catalog: {catalog}")
print(f"  Silver Schema: {silver_schema}")
print(f"  Gold Schema: {gold_schema}")
print(f"  Cost per Call: ${cost_per_call}")
print(f"  Cost per Minute: ${cost_per_minute}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Silver Tables Availability

# COMMAND ----------

# Verify all silver tables exist
required_tables = ['members', 'claims', 'call_center', 'digital_interactions', 'surveys', 'pharmacy', 'enrollment']
missing_tables = []

for table in required_tables:
    table_path = f"{catalog}.{silver_schema}.{table}_clean"
    if not spark.catalog.tableExists(table_path):
        missing_tables.append(table_path)
        print(f"❌ Missing: {table_path}")
    else:
        print(f"✅ Found: {table_path}")

if missing_tables:
    raise Exception(f"Missing silver tables: {missing_tables}. Run 02_silver_transformation.py first!")

print("\n✅ All required silver tables are available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def create_gold_table(table_name, query_func, description=""):
    """
    Generic function to create gold tables with error handling
    
    Args:
        table_name: Name of gold table (without schema)
        query_func: Function that returns a DataFrame
        description: Description of the table
    """
    print(f"\n{'='*60}")
    print(f"Creating Gold Table: {table_name}")
    if description:
        print(f"Description: {description}")
    print(f"{'='*60}")
    
    try:
        # Execute query
        df = query_func()
        
        print(f"Records: {df.count():,}")
        
        # Write to gold
        gold_path = f"{catalog}.{gold_schema}.{table_name}"
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gold_path)
        
        print(f"✅ Successfully created {gold_path}")
        
        # Show sample
        print("\nSample data:")
        display(spark.table(gold_path).limit(10))
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating {table_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Digital Call Leakage Analysis

# COMMAND ----------

def create_digital_call_leakage():
    """
    Analyze digital failures that converted to calls
    Shows cost impact of digital channel failures
    """
    
    # Read silver tables
    calls_df = spark.table(f"{catalog}.{silver_schema}.call_center_clean")
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    
    # Filter to leakage calls
    leakage_df = calls_df.filter(F.col("is_digital_leakage") == True)
    
    # Join with member details (plan_type, segment, tenure_bucket already in call_center_clean)
    leakage_df = leakage_df.join(
        members_df.select("member_id", "age_group"),
        "member_id",
        "left"
    )
    
    # Aggregate by multiple dimensions
    agg_df = leakage_df.groupBy(
        "call_date",
        "call_topic",
        "plan_type",
        "segment",
        "tenure_bucket",
        "failed_interaction_type"
    ).agg(
        F.count("*").alias("leakage_count"),
        F.avg("handle_time_minutes").alias("avg_handle_time"),
        F.sum("estimated_call_cost").alias("total_cost"),
        F.countDistinct("member_id").alias("unique_members"),
        F.sum(F.when(F.col("sentiment_clean") == "negative", 1).otherwise(0)).alias("negative_sentiment_count"),
        F.sum(F.when(F.col("resolved") == True, 1).otherwise(0)).alias("resolved_count")
    )
    
    # Calculate metrics
    agg_df = agg_df.withColumn("avg_cost_per_call", F.col("total_cost") / F.col("leakage_count"))
    agg_df = agg_df.withColumn("negative_sentiment_pct", 
        (F.col("negative_sentiment_count") / F.col("leakage_count")) * 100
    )
    agg_df = agg_df.withColumn("resolution_rate", 
        (F.col("resolved_count") / F.col("leakage_count")) * 100
    )
    
    # Add ranking
    window_spec = Window.orderBy(F.desc("total_cost"))
    agg_df = agg_df.withColumn("cost_rank", F.row_number().over(window_spec))
    
    # Add year-month for trending
    agg_df = agg_df.withColumn("year_month", F.date_format(F.col("call_date"), "yyyy-MM"))
    
    return agg_df.orderBy(F.desc("total_cost"))

# Create digital call leakage table
create_gold_table(
    "digital_call_leakage",
    create_digital_call_leakage,
    "Digital failures that resulted in call center contacts with cost impact"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Repeat Calls Analysis

# COMMAND ----------

def create_repeat_calls_analysis():
    """
    Analyze repeat call patterns with emotion tracking
    Identify members calling multiple times for same issues
    """
    
    calls_df = spark.table(f"{catalog}.{silver_schema}.call_center_clean")
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    
    # Join with member details (plan_type, segment, tenure_bucket already in call_center_clean)
    calls_with_members = calls_df.join(
        members_df.select("member_id", "age_group"),
        "member_id",
        "left"
    )
    
    # Calculate repeat patterns by member and topic
    window_spec = Window.partitionBy("member_id", "call_topic").orderBy("call_timestamp")
    
    calls_with_sequence = calls_with_members.withColumn(
        "call_sequence", 
        F.row_number().over(window_spec)
    )
    
    # Aggregate repeat call metrics
    repeat_analysis = calls_with_sequence.groupBy(
        "call_date",
        "call_topic",
        "plan_type",
        "segment",
        "tenure_bucket"
    ).agg(
        F.count("*").alias("total_calls"),
        F.sum(F.when(F.col("is_repeat_call") == True, 1).otherwise(0)).alias("repeat_calls"),
        F.countDistinct("member_id").alias("unique_members"),
        F.sum(F.when(F.col("sentiment_clean") == "negative", 1).otherwise(0)).alias("negative_calls"),
        F.sum(F.when(F.col("sentiment_clean") == "positive", 1).otherwise(0)).alias("positive_calls"),
        F.avg("handle_time_minutes").alias("avg_handle_time"),
        F.sum("estimated_call_cost").alias("total_cost"),
        F.sum(F.when(F.col("transferred") == True, 1).otherwise(0)).alias("transferred_calls"),
        F.sum(F.when(F.col("resolved") == False, 1).otherwise(0)).alias("unresolved_calls")
    )
    
    # Calculate percentages and rates
    repeat_analysis = repeat_analysis.withColumn(
        "repeat_call_rate",
        (F.col("repeat_calls") / F.col("total_calls")) * 100
    )
    
    repeat_analysis = repeat_analysis.withColumn(
        "negative_sentiment_pct",
        (F.col("negative_calls") / F.col("total_calls")) * 100
    )
    
    repeat_analysis = repeat_analysis.withColumn(
        "avg_calls_per_member",
        F.col("total_calls") / F.col("unique_members")
    )
    
    repeat_analysis = repeat_analysis.withColumn(
        "transfer_rate",
        (F.col("transferred_calls") / F.col("total_calls")) * 100
    )
    
    repeat_analysis = repeat_analysis.withColumn(
        "resolution_rate",
        ((F.col("total_calls") - F.col("unresolved_calls")) / F.col("total_calls")) * 100
    )
    
    # Add year-month
    repeat_analysis = repeat_analysis.withColumn(
        "year_month", 
        F.date_format(F.col("call_date"), "yyyy-MM")
    )
    
    return repeat_analysis.orderBy(F.desc("repeat_call_rate"))

# Create repeat calls analysis table
create_gold_table(
    "repeat_calls_analysis",
    create_repeat_calls_analysis,
    "Repeat call patterns with emotion tracking and resolution metrics"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Top Opportunities for Cost Savings

# COMMAND ----------

def create_top_opportunities():
    """
    Identify top cost-saving opportunities
    Continuously refreshed view of high-impact fixes
    """
    
    # Read relevant tables
    calls_df = spark.table(f"{catalog}.{silver_schema}.call_center_clean")
    digital_df = spark.table(f"{catalog}.{silver_schema}.digital_interactions_clean")
    
    # Analyze call patterns by topic
    call_opportunities = calls_df.groupBy("call_topic").agg(
        F.count("*").alias("call_volume"),
        F.sum("estimated_call_cost").alias("total_cost"),
        F.sum(F.when(F.col("is_digital_leakage") == True, 1).otherwise(0)).alias("avoidable_calls"),
        F.avg("handle_time_minutes").alias("avg_handle_time"),
        F.sum(F.when(F.col("sentiment_clean") == "negative", 1).otherwise(0)).alias("negative_calls"),
        F.sum(F.when(F.col("is_repeat_call") == True, 1).otherwise(0)).alias("repeat_calls")
    )
    
    # Calculate opportunity metrics
    call_opportunities = call_opportunities.withColumn(
        "avoidable_calls_pct",
        (F.col("avoidable_calls") / F.col("call_volume")) * 100
    )
    
    call_opportunities = call_opportunities.withColumn(
        "estimated_savings",
        F.col("avoidable_calls") * cost_per_call
    )
    
    call_opportunities = call_opportunities.withColumn(
        "negative_sentiment_pct",
        (F.col("negative_calls") / F.col("call_volume")) * 100
    )
    
    # Determine primary lever for each topic
    call_opportunities = call_opportunities.withColumn(
        "primary_lever",
        F.when(F.col("avoidable_calls_pct") > 30, "Digital")
         .when(F.col("repeat_calls") > F.col("call_volume") * 0.3, "Operations")
         .otherwise("Communications")
    )
    
    # Calculate 7-day trend
    recent_date = calls_df.agg(F.max("call_date")).collect()[0][0]
    week_ago = recent_date - timedelta(days=7)
    
    recent_calls = calls_df.filter(F.col("call_date") >= week_ago).groupBy("call_topic").agg(
        F.count("*").alias("recent_volume")
    )
    
    older_calls = calls_df.filter(
        (F.col("call_date") >= week_ago - timedelta(days=7)) & 
        (F.col("call_date") < week_ago)
    ).groupBy("call_topic").agg(
        F.count("*").alias("older_volume")
    )
    
    call_opportunities = call_opportunities.join(recent_calls, "call_topic", "left")
    call_opportunities = call_opportunities.join(older_calls, "call_topic", "left")
    
    call_opportunities = call_opportunities.withColumn(
        "trend_7d",
        F.when(
            F.col("older_volume").isNotNull() & (F.col("older_volume") > 0),
            ((F.col("recent_volume") - F.col("older_volume")) / F.col("older_volume")) * 100
        ).otherwise(0)
    )
    
    # Rank opportunities by savings potential
    window_spec = Window.orderBy(F.desc("estimated_savings"))
    call_opportunities = call_opportunities.withColumn(
        "opportunity_rank",
        F.row_number().over(window_spec)
    )
    
    # Select final columns
    final_df = call_opportunities.select(
        "opportunity_rank",
        "call_topic",
        "call_volume",
        "avoidable_calls",
        "avoidable_calls_pct",
        "estimated_savings",
        "total_cost",
        "avg_handle_time",
        "negative_sentiment_pct",
        "trend_7d",
        "primary_lever"
    )
    
    # Return top opportunities
    return final_df.filter(F.col("opportunity_rank") <= 20).orderBy("opportunity_rank")

# Create top opportunities table
create_gold_table(
    "top_opportunities",
    create_top_opportunities,
    "Top 20 cost-saving opportunities with impact metrics and recommended levers"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Member Journey Timeline

# COMMAND ----------

def create_member_journey_timeline():
    """
    Cross-channel journey reconstruction
    Wide table linking all member touchpoints
    """
    
    # Read all relevant tables
    members_df = spark.table(f"{catalog}.{silver_schema}.members_clean")
    digital_df = spark.table(f"{catalog}.{silver_schema}.digital_interactions_clean")
    calls_df = spark.table(f"{catalog}.{silver_schema}.call_center_clean")
    surveys_df = spark.table(f"{catalog}.{silver_schema}.surveys_clean")
    claims_df = spark.table(f"{catalog}.{silver_schema}.claims_clean")
    pharmacy_df = spark.table(f"{catalog}.{silver_schema}.pharmacy_clean")
    
    # Create unified timeline with event type
    digital_events = digital_df.select(
        F.col("member_id"),
        F.col("interaction_timestamp").alias("event_timestamp"),
        F.lit("digital").alias("event_type"),
        F.col("interaction_type").alias("event_subtype"),
        F.col("outcome").alias("event_outcome"),
        F.col("is_failure").alias("is_negative_event"),
        F.lit(None).cast(StringType()).alias("event_details")
    )
    
    call_events = calls_df.select(
        F.col("member_id"),
        F.col("call_timestamp").alias("event_timestamp"),
        F.lit("call").alias("event_type"),
        F.col("call_topic").alias("event_subtype"),
        F.col("sentiment_clean").alias("event_outcome"),
        F.when(F.col("sentiment_clean") == "negative", True).otherwise(False).alias("is_negative_event"),
        F.concat(
            F.lit("Handle Time: "), F.col("handle_time_minutes").cast(StringType()), F.lit(" min")
        ).alias("event_details")
    )
    
    survey_events = surveys_df.select(
        F.col("member_id"),
        F.col("survey_date").cast(TimestampType()).alias("event_timestamp"),
        F.lit("survey").alias("event_type"),
        F.col("survey_type").alias("event_subtype"),
        F.col("nps_category").alias("event_outcome"),
        F.when(F.col("nps_category") == "Detractor", True).otherwise(False).alias("is_negative_event"),
        F.concat(
            F.lit("NPS: "), F.col("nps_score").cast(StringType())
        ).alias("event_details")
    )
    
    claim_events = claims_df.select(
        F.col("member_id"),
        F.col("claim_date").cast(TimestampType()).alias("event_timestamp"),
        F.lit("claim").alias("event_type"),
        F.col("claim_type").alias("event_subtype"),
        F.col("status").alias("event_outcome"),
        F.when(F.col("status") == "Denied", True).otherwise(False).alias("is_negative_event"),
        F.concat(
            F.lit("Amount: $"), F.col("claim_amount").cast(StringType())
        ).alias("event_details")
    )
    
    pharmacy_events = pharmacy_df.select(
        F.col("member_id"),
        F.col("fill_date").cast(TimestampType()).alias("event_timestamp"),
        F.lit("pharmacy").alias("event_type"),
        F.col("drug_name").alias("event_subtype"),
        F.when(F.col("denied_flag") == True, "Denied").otherwise("Filled").alias("event_outcome"),
        F.col("denied_flag").alias("is_negative_event"),
        F.concat(
            F.lit("Copay: $"), F.col("copay_amount").cast(StringType())
        ).alias("event_details")
    )
    
    # Union all events
    all_events = digital_events.union(call_events).union(survey_events).union(claim_events).union(pharmacy_events)
    
    # Join with member details
    journey_timeline = all_events.join(
        members_df.select("member_id", "plan_type", "segment", "tenure_bucket", "age_group", "state"),
        "member_id",
        "inner"
    )
    
    # Add sequence numbers per member
    window_spec = Window.partitionBy("member_id").orderBy("event_timestamp")
    journey_timeline = journey_timeline.withColumn(
        "event_sequence",
        F.row_number().over(window_spec)
    )
    
    # Add time since previous event
    journey_timeline = journey_timeline.withColumn(
        "time_since_last_event_hours",
        (F.col("event_timestamp").cast("long") - 
         F.lag("event_timestamp").over(window_spec).cast("long")) / 3600
    )
    
    # Add date for partitioning
    journey_timeline = journey_timeline.withColumn(
        "event_date",
        F.to_date(F.col("event_timestamp"))
    )
    
    return journey_timeline.orderBy("member_id", "event_sequence")

# Create member journey timeline table
create_gold_table(
    "member_journey_timeline",
    create_member_journey_timeline,
    "Cross-channel member journey timeline with all touchpoints"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Executive KPIs Dashboard

# COMMAND ----------

def create_executive_kpis():
    """
    Daily rollup of key metrics for executive command center
    High-level KPIs updated daily
    """
    
    # Read relevant tables
    calls_df = spark.table(f"{catalog}.{silver_schema}.call_center_clean")
    digital_df = spark.table(f"{catalog}.{silver_schema}.digital_interactions_clean")
    
    # Daily call metrics
    daily_call_metrics = calls_df.groupBy("call_date").agg(
        F.count("*").alias("total_calls"),
        F.sum(F.when(F.col("is_repeat_call") == True, 1).otherwise(0)).alias("repeat_calls"),
        F.sum(F.when(F.col("sentiment_clean") == "negative", 1).otherwise(0)).alias("negative_emotion_calls"),
        F.avg("handle_time_minutes").alias("avg_handle_time"),
        F.sum("estimated_call_cost").alias("estimated_daily_call_cost"),
        F.sum(F.when(F.col("is_digital_leakage") == True, 1).otherwise(0)).alias("digital_leakage_calls"),
        F.countDistinct("member_id").alias("unique_calling_members")
    )
    
    # Daily digital metrics
    daily_digital_metrics = digital_df.groupBy(
        F.col("interaction_date").alias("call_date")
    ).agg(
        F.count("*").alias("total_digital_interactions"),
        F.sum(F.when(F.col("outcome_clean") == "success", 1).otherwise(0)).alias("successful_digital"),
        F.sum(F.when(F.col("is_failure") == True, 1).otherwise(0)).alias("failed_digital")
    )
    
    # Join metrics
    executive_kpis = daily_call_metrics.join(daily_digital_metrics, "call_date", "left")
    
    # Calculate rates and percentages
    executive_kpis = executive_kpis.withColumn(
        "repeat_call_pct",
        (F.col("repeat_calls") / F.col("total_calls")) * 100
    )
    
    executive_kpis = executive_kpis.withColumn(
        "negative_emotion_pct",
        (F.col("negative_emotion_calls") / F.col("total_calls")) * 100
    )
    
    executive_kpis = executive_kpis.withColumn(
        "digital_success_rate",
        (F.col("successful_digital") / F.col("total_digital_interactions")) * 100
    )
    
    executive_kpis = executive_kpis.withColumn(
        "digital_leakage_rate",
        (F.col("digital_leakage_calls") / F.col("total_calls")) * 100
    )
    
    # Get top 3 call topics per day
    top_topics = calls_df.groupBy("call_date", "call_topic").agg(
        F.count("*").alias("topic_count")
    )
    
    window_spec = Window.partitionBy("call_date").orderBy(F.desc("topic_count"))
    top_topics_ranked = top_topics.withColumn("rank", F.row_number().over(window_spec))
    
    top_3_topics = top_topics_ranked.filter(F.col("rank") <= 3).groupBy("call_date").agg(
        F.concat_ws(", ", F.collect_list("call_topic")).alias("top_3_topics")
    )
    
    # Join top topics
    executive_kpis = executive_kpis.join(top_3_topics, "call_date", "left")
    
    # Add year-month and day of week
    executive_kpis = executive_kpis.withColumn(
        "year_month",
        F.date_format(F.col("call_date"), "yyyy-MM")
    )
    
    executive_kpis = executive_kpis.withColumn(
        "day_of_week",
        F.date_format(F.col("call_date"), "EEEE")
    )
    
    # Add 7-day moving average for trends
    window_7day = Window.orderBy("call_date").rowsBetween(-6, 0)
    
    executive_kpis = executive_kpis.withColumn(
        "avg_calls_7day",
        F.avg("total_calls").over(window_7day)
    )
    
    executive_kpis = executive_kpis.withColumn(
        "avg_cost_7day",
        F.avg("estimated_daily_call_cost").over(window_7day)
    )
    
    return executive_kpis.orderBy(F.desc("call_date"))

# Create executive KPIs table
create_gold_table(
    "executive_kpis",
    create_executive_kpis,
    "Daily executive KPIs for command center dashboard"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------

print("\n" + "="*60)
print("GOLD LAYER SUMMARY")
print("="*60)

gold_tables = [
    'digital_call_leakage',
    'repeat_calls_analysis',
    'top_opportunities',
    'member_journey_timeline',
    'executive_kpis'
]

for table in gold_tables:
    gold_path = f"{catalog}.{gold_schema}.{table}"
    try:
        count = spark.table(gold_path).count()
        print(f"{table:30s}: {count:>10,} records")
    except Exception as e:
        print(f"{table:30s}: ERROR - {str(e)}")

print("="*60)
print("\n✅ Gold layer analytics tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Materialized Views for Real-Time Queries

# COMMAND ----------

# Create views for common queries
print("\nCreating materialized views...")

# View 1: Current week leakage summary
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.current_week_leakage AS
SELECT 
    call_topic,
    plan_type,
    SUM(leakage_count) as total_leakage,
    SUM(total_cost) as total_cost,
    AVG(avg_handle_time) as avg_handle_time
FROM {catalog}.{gold_schema}.digital_call_leakage
WHERE call_date >= date_sub(current_date(), 7)
GROUP BY call_topic, plan_type
ORDER BY total_cost DESC
""")
print("✅ Created view: current_week_leakage")

# View 2: Trending opportunities
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.trending_up_opportunities AS
SELECT *
FROM {catalog}.{gold_schema}.top_opportunities
WHERE trend_7d > 10
ORDER BY trend_7d DESC, estimated_savings DESC
""")
print("✅ Created view: trending_up_opportunities")

# View 3: High-risk member segments
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.high_risk_segments AS
SELECT 
    plan_type,
    segment,
    COUNT(*) as call_count,
    AVG(repeat_call_rate) as avg_repeat_rate,
    AVG(negative_sentiment_pct) as avg_negative_pct,
    SUM(total_cost) as total_cost
FROM {catalog}.{gold_schema}.repeat_calls_analysis
GROUP BY plan_type, segment
HAVING AVG(repeat_call_rate) > 30 OR AVG(negative_sentiment_pct) > 40
ORDER BY total_cost DESC
""")
print("✅ Created view: high_risk_segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Tables

# COMMAND ----------

# Optimize and Z-Order gold tables
optimize_config = {
    'digital_call_leakage': ['call_date', 'call_topic'],
    'repeat_calls_analysis': ['call_date', 'call_topic'],
    'top_opportunities': ['opportunity_rank'],
    'member_journey_timeline': ['member_id', 'event_date'],
    'executive_kpis': ['call_date']
}

for table, zorder_cols in optimize_config.items():
    gold_path = f"{catalog}.{gold_schema}.{table}"
    try:
        spark.sql(f"OPTIMIZE {gold_path} ZORDER BY ({', '.join(zorder_cols)})")
        print(f"✅ Optimized {gold_path}")
    except Exception as e:
        print(f"⚠️ Could not optimize {gold_path}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed

# COMMAND ----------

# Enable CDF on gold tables for downstream consumption
for table in gold_tables:
    gold_path = f"{catalog}.{gold_schema}.{table}"
    try:
        spark.sql(f"ALTER TABLE {gold_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print(f"✅ Enabled Change Data Feed on {gold_path}")
    except Exception as e:
        print(f"⚠️ Could not enable CDF on {gold_path}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Complete
# MAGIC
# MAGIC ✅ Gold layer analytics complete!
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Run `04_command_center_dashboard.py` to execute executive queries
# MAGIC 2. Create visualizations in Databricks SQL
# MAGIC 3. Set up alerts and monitoring
