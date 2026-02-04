# Databricks notebook source
# MAGIC %md
# MAGIC # Command Center Dashboard - Executive Queries
# MAGIC 
# MAGIC This notebook contains SQL queries and visualizations to answer key executive questions.
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Answer the 5 critical executive questions from requirements
# MAGIC - Provide actionable insights for decision-making
# MAGIC - Enable real-time command center operations
# MAGIC 
# MAGIC **Executive Questions Addressed:**
# MAGIC 1. Which three journeys are driving the most avoidable call cost right now?
# MAGIC 2. Which member segments are experiencing the highest negative emotion?
# MAGIC 3. Which digital journeys are leaking into the call center—and at what rate?
# MAGIC 4. Where do members abandon online flows and immediately call?
# MAGIC 5. Are new members failing more than tenured members—and where?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

import json
from pyspark.sql import functions as F

# Load configuration
with open('/dbfs/FileStore/config/pipeline_config.json', 'r') as f:
    config = json.load(f)

# Notebook parameters
dbutils.widgets.text("catalog", config['catalog'], "Catalog Name")
dbutils.widgets.text("gold_schema", config['schemas']['gold'], "Gold Schema")
dbutils.widgets.text("lookback_days", "30", "Lookback Days")

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")
lookback_days = int(dbutils.widgets.get("lookback_days"))

print(f"Configuration loaded:")
print(f"  Catalog: {catalog}")
print(f"  Gold Schema: {gold_schema}")
print(f"  Lookback Days: {lookback_days}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Gold Tables

# COMMAND ----------

# Check gold tables are available
required_tables = ['digital_call_leakage', 'repeat_calls_analysis', 'top_opportunities', 
                   'member_journey_timeline', 'executive_kpis']
missing_tables = []

for table in required_tables:
    table_path = f"{catalog}.{gold_schema}.{table}"
    if not spark.catalog.tableExists(table_path):
        missing_tables.append(table_path)
        print(f"❌ Missing: {table_path}")
    else:
        print(f"✅ Found: {table_path}")

if missing_tables:
    raise Exception(f"Missing gold tables: {missing_tables}. Run 03_gold_analytics.py first!")

print("\n✅ All required gold tables are available")

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Question 1: Top 3 Journeys Driving Avoidable Call Cost
# MAGIC 
# MAGIC **Business Context:** Identify which digital-to-call journeys are costing the most money so we can prioritize fixes.
# MAGIC 
# MAGIC **Data Source:** `top_opportunities` gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 3 Journeys by Avoidable Cost
# MAGIC SELECT 
#     opportunity_rank,
#     call_topic as journey,
#     call_volume as total_calls,
#     avoidable_calls,
#     ROUND(avoidable_calls_pct, 1) as avoidable_pct,
#     CONCAT('$', FORMAT_NUMBER(estimated_savings, 0)) as estimated_savings,
#     CONCAT('$', FORMAT_NUMBER(total_cost, 0)) as total_cost,
#     ROUND(avg_handle_time, 1) as avg_handle_time_min,
#     ROUND(negative_sentiment_pct, 1) as negative_sentiment_pct,
#     ROUND(trend_7d, 1) as trend_7d_pct,
#     primary_lever as recommended_action
# FROM ${catalog}.${gold_schema}.top_opportunities
# WHERE opportunity_rank <= 3
# ORDER BY opportunity_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insight Summary
# MAGIC 
# MAGIC **Python analysis of top opportunities:**

# COMMAND ----------

# Get top 3 opportunities programmatically
top_3_df = spark.sql(f"""
    SELECT 
        call_topic,
        estimated_savings,
        avoidable_calls,
        primary_lever,
        trend_7d
    FROM {catalog}.{gold_schema}.top_opportunities
    WHERE opportunity_rank <= 3
    ORDER BY opportunity_rank
""").collect()

print("=" * 80)
print("TOP 3 AVOIDABLE CALL COST OPPORTUNITIES")
print("=" * 80)

total_savings = 0
for i, row in enumerate(top_3_df, 1):
    print(f"\n{i}. {row['call_topic']}")
    print(f"   💰 Potential Savings: ${row['estimated_savings']:,.2f}")
    print(f"   📞 Avoidable Calls: {row['avoidable_calls']:,}")
    print(f"   🎯 Primary Lever: {row['primary_lever']}")
    
    if row['trend_7d'] > 0:
        print(f"   📈 Trending UP: +{row['trend_7d']:.1f}% (URGENT)")
    elif row['trend_7d'] < 0:
        print(f"   📉 Trending down: {row['trend_7d']:.1f}%")
    else:
        print(f"   ➡️  Stable trend")
    
    total_savings += row['estimated_savings']

print(f"\n{'='*80}")
print(f"💵 TOTAL POTENTIAL SAVINGS FROM TOP 3: ${total_savings:,.2f}")
print(f"{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Question 2: Member Segments with Highest Negative Emotion
# MAGIC 
# MAGIC **Business Context:** Identify at-risk member segments to prevent churn and improve satisfaction.
# MAGIC 
# MAGIC **Data Source:** `repeat_calls_analysis` gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Segments by Negative Emotion
# MAGIC SELECT 
#     segment,
#     plan_type,
#     tenure_bucket,
#     SUM(total_calls) as total_calls,
#     SUM(negative_calls) as negative_calls,
#     ROUND(AVG(negative_sentiment_pct), 1) as avg_negative_pct,
#     ROUND(AVG(repeat_call_rate), 1) as avg_repeat_rate,
#     CONCAT('$', FORMAT_NUMBER(SUM(total_cost), 0)) as total_cost
# FROM ${catalog}.${gold_schema}.repeat_calls_analysis
# WHERE call_date >= date_sub(current_date(), ${lookback_days})
# GROUP BY segment, plan_type, tenure_bucket
# HAVING AVG(negative_sentiment_pct) > 30
# ORDER BY AVG(negative_sentiment_pct) DESC, SUM(total_calls) DESC
# LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### High-Risk Segment Analysis

# COMMAND ----------

# Detailed segment analysis
high_risk_segments = spark.sql(f"""
    SELECT 
        segment,
        plan_type,
        tenure_bucket,
        SUM(total_calls) as total_calls,
        AVG(negative_sentiment_pct) as avg_negative_pct,
        AVG(repeat_call_rate) as avg_repeat_rate,
        SUM(total_cost) as total_cost
    FROM {catalog}.{gold_schema}.repeat_calls_analysis
    WHERE call_date >= date_sub(current_date(), {lookback_days})
    GROUP BY segment, plan_type, tenure_bucket
    HAVING AVG(negative_sentiment_pct) > 30
    ORDER BY AVG(negative_sentiment_pct) DESC
    LIMIT 5
""").collect()

print("=" * 80)
print("MEMBER SEGMENTS WITH HIGHEST NEGATIVE EMOTION")
print("=" * 80)

for i, row in enumerate(high_risk_segments, 1):
    print(f"\n{i}. {row['segment']} - {row['plan_type']} ({row['tenure_bucket']})")
    print(f"   😠 Negative Sentiment: {row['avg_negative_pct']:.1f}%")
    print(f"   🔁 Repeat Call Rate: {row['avg_repeat_rate']:.1f}%")
    print(f"   📞 Total Calls: {row['total_calls']:,}")
    print(f"   💵 Total Cost: ${row['total_cost']:,.2f}")
    
    # Recommendations
    if row['avg_repeat_rate'] > 30:
        print(f"   ⚠️  ACTION: High repeat rate - investigate root cause resolution")
    if row['avg_negative_pct'] > 40:
        print(f"   ⚠️  ACTION: Critical sentiment - immediate intervention needed")

print(f"\n{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Question 3: Digital Journeys Leaking to Call Center
# MAGIC 
# MAGIC **Business Context:** Understand which digital self-service failures are causing members to call.
# MAGIC 
# MAGIC **Data Source:** `digital_call_leakage` gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Digital Leakage by Journey Type
# MAGIC SELECT 
#     failed_interaction_type as digital_journey,
#     call_topic as resulting_call_topic,
#     SUM(leakage_count) as total_leakage_calls,
#     CONCAT('$', FORMAT_NUMBER(SUM(total_cost), 0)) as total_cost,
#     ROUND(AVG(avg_handle_time), 1) as avg_handle_time_min,
#     ROUND(AVG(negative_sentiment_pct), 1) as avg_negative_pct,
#     COUNT(DISTINCT year_month) as months_active
# FROM ${catalog}.${gold_schema}.digital_call_leakage
# WHERE call_date >= date_sub(current_date(), ${lookback_days})
#     AND failed_interaction_type IS NOT NULL
# GROUP BY failed_interaction_type, call_topic
# ORDER BY SUM(leakage_count) DESC
# LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leakage Rate Analysis

# COMMAND ----------

# Calculate overall leakage metrics
leakage_summary = spark.sql(f"""
    SELECT 
        SUM(leakage_count) as total_leakage,
        SUM(total_cost) as total_cost,
        AVG(avg_handle_time) as avg_handle_time,
        COUNT(DISTINCT failed_interaction_type) as unique_failure_types,
        COUNT(DISTINCT call_topic) as unique_call_topics
    FROM {catalog}.{gold_schema}.digital_call_leakage
    WHERE call_date >= date_sub(current_date(), {lookback_days})
        AND failed_interaction_type IS NOT NULL
""").collect()[0]

# Get total calls for comparison
total_calls_summary = spark.sql(f"""
    SELECT SUM(total_calls) as total_calls
    FROM {catalog}.{gold_schema}.executive_kpis
    WHERE call_date >= date_sub(current_date(), {lookback_days})
""").collect()[0]

leakage_rate = (leakage_summary['total_leakage'] / total_calls_summary['total_calls']) * 100

print("=" * 80)
print("DIGITAL → CALL LEAKAGE SUMMARY")
print("=" * 80)
print(f"\n📊 Overall Leakage Metrics ({lookback_days} days):")
print(f"   Total Leakage Calls: {leakage_summary['total_leakage']:,}")
print(f"   Leakage Rate: {leakage_rate:.1f}% of all calls")
print(f"   Total Cost: ${leakage_summary['total_cost']:,.2f}")
print(f"   Avg Handle Time: {leakage_summary['avg_handle_time']:.1f} minutes")
print(f"   Unique Failure Types: {leakage_summary['unique_failure_types']}")

# Get top leakage journeys
top_leakage = spark.sql(f"""
    SELECT 
        failed_interaction_type,
        SUM(leakage_count) as leakage_count,
        SUM(total_cost) as total_cost
    FROM {catalog}.{gold_schema}.digital_call_leakage
    WHERE call_date >= date_sub(current_date(), {lookback_days})
        AND failed_interaction_type IS NOT NULL
    GROUP BY failed_interaction_type
    ORDER BY SUM(leakage_count) DESC
    LIMIT 5
""").collect()

print(f"\n🔥 Top Leaking Digital Journeys:")
for i, row in enumerate(top_leakage, 1):
    print(f"   {i}. {row['failed_interaction_type']}: {row['leakage_count']:,} calls (${row['total_cost']:,.2f})")

print(f"\n{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Question 4: Digital Abandonment → Immediate Call Pattern
# MAGIC 
# MAGIC **Business Context:** Find specific moments where members give up online and immediately pick up the phone.
# MAGIC 
# MAGIC **Data Source:** `member_journey_timeline` gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Abandoned Digital → Quick Call Pattern
# MAGIC WITH digital_abandonment AS (
#     SELECT 
#         member_id,
#         event_timestamp as digital_timestamp,
#         event_subtype as abandoned_journey,
#         event_details
#     FROM ${catalog}.${gold_schema}.member_journey_timeline
#     WHERE event_type = 'digital'
#         AND event_outcome = 'abandoned'
#         AND event_date >= date_sub(current_date(), ${lookback_days})
# ),
# immediate_calls AS (
#     SELECT 
#         member_id,
#         event_timestamp as call_timestamp,
#         event_subtype as call_topic,
#         event_details
#     FROM ${catalog}.${gold_schema}.member_journey_timeline
#     WHERE event_type = 'call'
#         AND event_date >= date_sub(current_date(), ${lookback_days})
# )
# SELECT 
#     d.abandoned_journey,
#     c.call_topic,
#     COUNT(*) as occurrence_count,
#     ROUND(AVG((UNIX_TIMESTAMP(c.call_timestamp) - UNIX_TIMESTAMP(d.digital_timestamp)) / 60), 1) as avg_time_to_call_minutes,
#     COUNT(DISTINCT d.member_id) as unique_members
# FROM digital_abandonment d
# JOIN immediate_calls c ON d.member_id = c.member_id
# WHERE c.call_timestamp > d.digital_timestamp
#     AND c.call_timestamp <= d.digital_timestamp + INTERVAL 2 HOURS
# GROUP BY d.abandoned_journey, c.call_topic
# HAVING COUNT(*) >= 10
# ORDER BY COUNT(*) DESC
# LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Abandonment Hotspot Analysis

# COMMAND ----------

print("=" * 80)
print("DIGITAL ABANDONMENT → IMMEDIATE CALL HOTSPOTS")
print("=" * 80)

abandonment_patterns = spark.sql(f"""
    WITH digital_abandonment AS (
        SELECT 
            member_id,
            event_timestamp as digital_timestamp,
            event_subtype as abandoned_journey
        FROM {catalog}.{gold_schema}.member_journey_timeline
        WHERE event_type = 'digital'
            AND event_outcome = 'abandoned'
            AND event_date >= date_sub(current_date(), {lookback_days})
    ),
    immediate_calls AS (
        SELECT 
            member_id,
            event_timestamp as call_timestamp,
            event_subtype as call_topic
        FROM {catalog}.{gold_schema}.member_journey_timeline
        WHERE event_type = 'call'
            AND event_date >= date_sub(current_date(), {lookback_days})
    )
    SELECT 
        d.abandoned_journey,
        c.call_topic,
        COUNT(*) as occurrence_count,
        AVG((UNIX_TIMESTAMP(c.call_timestamp) - UNIX_TIMESTAMP(d.digital_timestamp)) / 60) as avg_minutes
    FROM digital_abandonment d
    JOIN immediate_calls c ON d.member_id = c.member_id
    WHERE c.call_timestamp > d.digital_timestamp
        AND c.call_timestamp <= d.digital_timestamp + INTERVAL 2 HOURS
    GROUP BY d.abandoned_journey, c.call_topic
    HAVING COUNT(*) >= 10
    ORDER BY COUNT(*) DESC
    LIMIT 10
""").collect()

print(f"\n🚨 Top Abandonment Patterns (within 2 hours):\n")
for i, row in enumerate(abandonment_patterns, 1):
    print(f"{i}. {row['abandoned_journey']} → {row['call_topic']}")
    print(f"   Occurrences: {row['occurrence_count']:,}")
    print(f"   Avg Time to Call: {row['avg_minutes']:.1f} minutes")
    print(f"   ⚡ ACTION: Fix digital flow before member frustration leads to call\n")

print(f"{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Question 5: New vs. Tenured Member Experience
# MAGIC 
# MAGIC **Business Context:** Determine if onboarding experience needs improvement compared to tenured members.
# MAGIC 
# MAGIC **Data Source:** `repeat_calls_analysis` and `digital_call_leakage` gold tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- New vs Tenured Member Comparison
# MAGIC SELECT 
#     tenure_bucket,
#     SUM(total_calls) as total_calls,
#     ROUND(AVG(repeat_call_rate), 1) as avg_repeat_rate,
#     ROUND(AVG(negative_sentiment_pct), 1) as avg_negative_pct,
#     ROUND(AVG(resolution_rate), 1) as avg_resolution_rate,
#     CONCAT('$', FORMAT_NUMBER(SUM(total_cost), 0)) as total_cost
# FROM ${catalog}.${gold_schema}.repeat_calls_analysis
# WHERE call_date >= date_sub(current_date(), ${lookback_days})
# GROUP BY tenure_bucket
# ORDER BY 
#     CASE tenure_bucket
#         WHEN '0-6 months' THEN 1
#         WHEN '6-12 months' THEN 2
#         WHEN '1-2 years' THEN 3
#         WHEN '2-3 years' THEN 4
#         WHEN '3+ years' THEN 5
#     END

# COMMAND ----------

# MAGIC %md
# MAGIC ### New Member Failure Analysis

# COMMAND ----------

# Compare new vs tenured performance
tenure_comparison = spark.sql(f"""
    SELECT 
        tenure_bucket,
        SUM(total_calls) as total_calls,
        AVG(repeat_call_rate) as avg_repeat_rate,
        AVG(negative_sentiment_pct) as avg_negative_pct,
        SUM(total_cost) as total_cost
    FROM {catalog}.{gold_schema}.repeat_calls_analysis
    WHERE call_date >= date_sub(current_date(), {lookback_days})
    GROUP BY tenure_bucket
""").collect()

print("=" * 80)
print("NEW vs TENURED MEMBER EXPERIENCE COMPARISON")
print("=" * 80)

# Create tenure mapping
tenure_order = ['0-6 months', '6-12 months', '1-2 years', '2-3 years', '3+ years']
tenure_data = {row['tenure_bucket']: row for row in tenure_comparison}

# Get baseline (tenured members)
tenured = tenure_data.get('3+ years')
new_members = tenure_data.get('0-6 months')

if tenured and new_members:
    print(f"\n📊 Key Metrics Comparison:\n")
    print(f"{'Metric':<30} {'New (0-6m)':<15} {'Tenured (3+y)':<15} {'Difference'}")
    print(f"{'-'*75}")
    
    # Repeat rate
    repeat_diff = new_members['avg_repeat_rate'] - tenured['avg_repeat_rate']
    print(f"{'Repeat Call Rate':<30} {new_members['avg_repeat_rate']:>6.1f}% {tenured['avg_repeat_rate']:>13.1f}% {repeat_diff:>13.1f}%")
    
    # Negative sentiment
    neg_diff = new_members['avg_negative_pct'] - tenured['avg_negative_pct']
    print(f"{'Negative Sentiment':<30} {new_members['avg_negative_pct']:>6.1f}% {tenured['avg_negative_pct']:>13.1f}% {neg_diff:>13.1f}%")
    
    # Cost per call
    new_cost_per_call = new_members['total_cost'] / new_members['total_calls']
    tenured_cost_per_call = tenured['total_cost'] / tenured['total_calls']
    cost_diff = new_cost_per_call - tenured_cost_per_call
    print(f"{'Avg Cost per Call':<30} ${new_cost_per_call:>6.2f} ${tenured_cost_per_call:>14.2f} ${cost_diff:>13.2f}")
    
    print(f"\n{'='*75}")
    
    # Diagnosis
    print(f"\n🔍 DIAGNOSIS:\n")
    if repeat_diff > 5:
        print(f"   ⚠️  New members have {repeat_diff:.1f}% HIGHER repeat rate - onboarding gaps")
    if neg_diff > 5:
        print(f"   ⚠️  New members have {neg_diff:.1f}% MORE negative sentiment - frustration with complexity")
    if cost_diff > 2:
        print(f"   ⚠️  New member calls cost ${cost_diff:.2f} MORE - longer handle times")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:\n")
    if repeat_diff > 5 or neg_diff > 5:
        print(f"   1. Enhanced onboarding materials for new members")
        print(f"   2. Proactive outreach during first 6 months")
        print(f"   3. Simplified digital flows for common new member tasks")
        print(f"   4. Dedicated support line for new member questions")

print(f"\n{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Dashboard Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Last 7 Days Summary
# MAGIC SELECT 
#     call_date,
#     total_calls,
#     ROUND(repeat_call_pct, 1) as repeat_call_pct,
#     ROUND(negative_emotion_pct, 1) as negative_emotion_pct,
#     ROUND(digital_success_rate, 1) as digital_success_rate,
#     ROUND(digital_leakage_rate, 1) as digital_leakage_rate,
#     ROUND(avg_handle_time, 1) as avg_handle_time_min,
#     CONCAT('$', FORMAT_NUMBER(estimated_daily_call_cost, 0)) as daily_cost,
#     top_3_topics
# FROM ${catalog}.${gold_schema}.executive_kpis
# WHERE call_date >= date_sub(current_date(), 7)
# ORDER BY call_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete Executive Summary

# COMMAND ----------

# Generate comprehensive executive summary
print("=" * 80)
print("EXECUTIVE COMMAND CENTER - DAILY SUMMARY")
print("=" * 80)

# Get latest KPIs
latest_kpis = spark.sql(f"""
    SELECT *
    FROM {catalog}.{gold_schema}.executive_kpis
    ORDER BY call_date DESC
    LIMIT 1
""").collect()[0]

print(f"\nDate: {latest_kpis['call_date']}")
print(f"\n📊 VOLUME METRICS:")
print(f"   Total Calls: {latest_kpis['total_calls']:,}")
print(f"   Unique Members: {latest_kpis['unique_calling_members']:,}")
print(f"   Digital Interactions: {latest_kpis['total_digital_interactions']:,}")

print(f"\n💰 FINANCIAL METRICS:")
print(f"   Daily Call Cost: ${latest_kpis['estimated_daily_call_cost']:,.2f}")
print(f"   7-Day Avg Cost: ${latest_kpis['avg_cost_7day']:,.2f}")

print(f"\n📈 PERFORMANCE METRICS:")
print(f"   Repeat Call Rate: {latest_kpis['repeat_call_pct']:.1f}%")
print(f"   Negative Emotion: {latest_kpis['negative_emotion_pct']:.1f}%")
print(f"   Digital Success Rate: {latest_kpis['digital_success_rate']:.1f}%")
print(f"   Digital Leakage Rate: {latest_kpis['digital_leakage_rate']:.1f}%")
print(f"   Avg Handle Time: {latest_kpis['avg_handle_time']:.1f} minutes")

print(f"\n🔥 TOP CALL TOPICS:")
print(f"   {latest_kpis['top_3_topics']}")

# Health indicators
print(f"\n🚦 HEALTH STATUS:")
if latest_kpis['repeat_call_pct'] > 25:
    print(f"   🔴 ALERT: Repeat call rate above threshold (>25%)")
elif latest_kpis['repeat_call_pct'] > 20:
    print(f"   🟡 WATCH: Repeat call rate elevated (>20%)")
else:
    print(f"   🟢 GOOD: Repeat call rate within target")

if latest_kpis['negative_emotion_pct'] > 35:
    print(f"   🔴 ALERT: Negative sentiment high (>35%)")
elif latest_kpis['negative_emotion_pct'] > 25:
    print(f"   🟡 WATCH: Negative sentiment elevated (>25%)")
else:
    print(f"   🟢 GOOD: Negative sentiment within target")

if latest_kpis['digital_success_rate'] < 70:
    print(f"   🔴 ALERT: Digital success rate low (<70%)")
elif latest_kpis['digital_success_rate'] < 80:
    print(f"   🟡 WATCH: Digital success rate needs improvement (<80%)")
else:
    print(f"   🟢 GOOD: Digital success rate healthy")

print(f"\n{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Complete
# MAGIC 
# MAGIC ✅ Command Center Dashboard queries complete!
# MAGIC 
# MAGIC **All Executive Questions Answered:**
# MAGIC 1. ✅ Top 3 journeys driving avoidable call cost
# MAGIC 2. ✅ Member segments with highest negative emotion
# MAGIC 3. ✅ Digital journeys leaking to call center
# MAGIC 4. ✅ Digital abandonment → immediate call patterns
# MAGIC 5. ✅ New vs. tenured member experience comparison
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC - Schedule these notebooks to run daily
# MAGIC - Create Databricks SQL dashboards with visualizations
# MAGIC - Set up alerts for critical thresholds
# MAGIC - Integrate with operational systems for action
