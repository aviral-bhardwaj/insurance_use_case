# Insurance Member Experience Command Center

A complete end-to-end implementation using Databricks Medallion Architecture to transform member interactions into actionable insights.

## 🎯 Objective
Transform a one-time, retrospective analysis of 100M+ member interactions into a live, integrated nervous system that continuously monitors, explains, and improves member experience while quantifying real financial impact and driving daily action, not annual insights.

This implementation enables executives to see where experience and cost are leaking, understand why, and activate fixes across digital, call center, and operations in near real time.
## 📋 Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Repository Structure](#repository-structure)
3. [Data Dictionary](#data-dictionary)
4. [Setup Instructions](#setup-instructions)
5. [Running the Pipeline](#running-the-pipeline)
6. [Business Requirements](#business-requirements)
7. [Executive Questions](#executive-questions)
8. [Key Metrics](#key-metrics)

---

## 🏗 Architecture Overview

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG                                │
│              insurance_command_center                           │
└─────────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   ┌─────────┐       ┌──────────┐       ┌────────┐
   │ BRONZE  │──────▶│  SILVER  │──────▶│  GOLD  │
   │  (Raw)  │       │  (Clean) │       │(Business)│
   └─────────┘       └──────────┘       └────────┘
        │                  │                  │
   Raw CSV Data    Data Quality &     Business Analytics
   Ingestion       Transformation      & KPI Tables
                                             │
                                        Dashboard &
                                      Executive Queries
```

### Layer Details

#### 🥉 Bronze Layer (Raw Data)
- **Purpose:** Preserve raw data exactly as received
- **Tables:** `*_raw` (members, claims, call_center, digital_interactions, surveys, pharmacy, enrollment)
- **Format:** Delta Lake with full schema enforcement
- **Features:** Ingestion metadata, change data feed

#### 🥈 Silver Layer (Clean Data)
- **Purpose:** Cleaned, validated, and enriched business data
- **Tables:** `*_clean` 
- **Transformations:**
  - Deduplication
  - Data quality checks
  - Referential integrity validation
  - Business logic enrichment (leakage detection, repeat calls)
  - Tenure bucketing, sentiment standardization
- **Features:** SCD Type 2 for slowly changing dimensions

#### 🥇 Gold Layer (Business Analytics)
- **Purpose:** Aggregated business metrics and KPIs
- **Tables:**
  - `digital_call_leakage` - Digital failures → call conversions
  - `repeat_calls_analysis` - Repeat call patterns
  - `top_opportunities` - Cost-saving opportunities
  - `member_journey_timeline` - Cross-channel journeys
  - `executive_kpis` - Daily executive dashboard
- **Features:** Materialized views, optimized for queries

---

## 📁 Repository Structure

```
insurance_use_case/
│
├── data/                          # Synthetic insurance datasets
│   ├── members.csv               # 10K member demographics
│   ├── claims.csv                # 50K claims transactions
│   ├── call_center.csv           # 100K call interactions
│   ├── digital_interactions.csv  # 150K digital touchpoints
│   ├── surveys.csv               # 25K customer surveys
│   ├── pharmacy.csv              # 75K pharmacy transactions
│   └── enrollment.csv            # 15K enrollment records
│
├── config/
│   └── pipeline_config.json      # Pipeline configuration
│
├── notebooks/                     # Databricks notebooks
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_analytics.py
│   └── 04_command_center_dashboard.py
│
├── generate_synthetic_data.py    # Data generation script
├── README.md                      # This file
└── .gitignore
```

---

## 📊 Data Dictionary

### Members (`members.csv`)
10,000+ member demographics and attributes

| Field | Type | Description |
|-------|------|-------------|
| member_id | String | Unique member identifier (M00000001) |
| age | Integer | Member age (typically 65+) |
| gender | String | M or F |
| plan_type | String | MAPD, PPO, HMO, POS, EPO |
| tenure_months | Integer | Months as member |
| segment | String | High/Medium/Low Value, At Risk, New |
| state | String | US state code |
| zip_code | String | 5-digit ZIP code |
| join_date | Date | Enrollment start date |

### Claims (`claims.csv`)
50,000+ claims and prior authorization transactions

| Field | Type | Description |
|-------|------|-------------|
| claim_id | String | Unique claim identifier |
| member_id | String | Foreign key to members |
| claim_date | Date | Date claim submitted |
| claim_type | String | Medical, Pharmacy, Preventive, etc. |
| claim_amount | Double | Claim amount in USD |
| status | String | Approved, Denied, Pending, Under Review |
| approval_time_days | Integer | Days to approve/deny |
| denial_reason | String | Reason if denied |

### Call Center (`call_center.csv`)
100,000+ call center interactions with ACD/IVR data

| Field | Type | Description |
|-------|------|-------------|
| call_id | String | Unique call identifier |
| member_id | String | Foreign key to members |
| call_date | Date | Call date |
| call_timestamp | Timestamp | Exact call time |
| call_topic | String | ID Card, Billing, Claims, etc. |
| handle_time_minutes | Double | Call duration |
| sentiment | String | positive, neutral, negative |
| is_repeat_call | Boolean | Repeat within 7 days |
| prior_digital_attempt | Boolean | Digital failure before call |
| transferred | Boolean | Call transferred |
| resolved | Boolean | Issue resolved |

### Digital Interactions (`digital_interactions.csv`)
150,000+ website/app interactions

| Field | Type | Description |
|-------|------|-------------|
| interaction_id | String | Unique interaction identifier |
| member_id | String | Foreign key to members |
| interaction_date | Date | Interaction date |
| interaction_timestamp | Timestamp | Exact interaction time |
| interaction_type | String | login, bill_pay, claims_check, etc. |
| outcome | String | success, failure, abandoned |
| failure_reason | String | Reason if failed |
| session_duration_seconds | Integer | Session length |

### Surveys (`surveys.csv`)
25,000+ Voice of Customer surveys

| Field | Type | Description |
|-------|------|-------------|
| survey_id | String | Unique survey identifier |
| member_id | String | Foreign key to members |
| survey_date | Date | Survey completion date |
| nps_score | Integer | Net Promoter Score (0-10) |
| csat_score | Integer | Customer Satisfaction (1-5) |
| effort_score | Integer | Customer Effort Score (1-7, lower better) |
| feedback_text | String | Open text feedback |
| survey_type | String | post_call, post_digital, periodic, annual |

### Pharmacy (`pharmacy.csv`)
75,000+ pharmacy transactions

| Field | Type | Description |
|-------|------|-------------|
| rx_id | String | Unique prescription identifier |
| member_id | String | Foreign key to members |
| fill_date | Date | Prescription fill date |
| drug_name | String | Medication name |
| copay_amount | Double | Member copay in USD |
| days_supply | Integer | 30, 60, or 90 days |
| pharmacy_type | String | retail, mail, specialty |
| denied_flag | Boolean | Prescription denied |
| denied_reason | String | Reason if denied |

### Enrollment (`enrollment.csv`)
15,000+ plan enrollment and changes

| Field | Type | Description |
|-------|------|-------------|
| enrollment_id | String | Unique enrollment identifier |
| member_id | String | Foreign key to members |
| enrollment_date | Date | Enrollment/change date |
| plan_type | String | MAPD, PPO, HMO, POS, EPO |
| premium_amount | Double | Monthly premium in USD |
| change_type | String | new, renewal, change |
| change_reason | String | Reason for enrollment/change |

---

## 🚀 Setup Instructions

### Prerequisites

1. **Databricks Workspace**
   - Unity Catalog enabled
   - DBR 13.0+ (or latest LTS)
   - Python 3.9+

2. **Access Rights**
   - CREATE CATALOG permission
   - CREATE SCHEMA permission
   - CREATE TABLE permission

### Step 1: Clone Repository

```bash
git clone https://github.com/aviral-bhardwaj/insurance_use_case.git
cd insurance_use_case
```

### Step 2: Upload Data to Databricks

Upload all files from the `data/` folder to Databricks FileStore:

```bash
# Using Databricks CLI
databricks fs cp data/ dbfs:/FileStore/data/ --recursive

# Or manually via UI: Data → Add → Upload Files
```

### Step 3: Upload Configuration

```bash
# Upload config file
databricks fs cp config/pipeline_config.json dbfs:/FileStore/config/
```

### Step 4: Import Notebooks

Import all notebooks from `notebooks/` folder into your Databricks workspace:

1. Go to Workspace → Import
2. Select "File" and choose each notebook
3. Import in order: 01 → 02 → 03 → 04

### Step 5: Configure Unity Catalog

Update `config/pipeline_config.json` if needed:

```json
{
  "catalog": "insurance_command_center",  // Your catalog name
  "schemas": {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold"
  }
}
```

Create catalog manually if it doesn't exist:

```sql
CREATE CATALOG IF NOT EXISTS insurance_command_center;
```

---

## ▶️ Running the Pipeline

### Sequential Execution (First Time)

Run notebooks in order:

#### 1. Bronze Layer Ingestion
```
Run: notebooks/01_bronze_ingestion.py
Duration: ~5-10 minutes
Output: 7 raw tables in bronze schema
```

#### 2. Silver Layer Transformation
```
Run: notebooks/02_silver_transformation.py
Duration: ~10-15 minutes
Output: 7 clean tables in silver schema with enrichment
```

#### 3. Gold Layer Analytics
```
Run: notebooks/03_gold_analytics.py
Duration: ~15-20 minutes
Output: 5 business analytics tables in gold schema
```

#### 4. Command Center Dashboard
```
Run: notebooks/04_command_center_dashboard.py
Duration: ~2-3 minutes
Output: Executive insights and visualizations
```

### Notebook Parameters

All notebooks accept these parameters (via widgets):

- `catalog` - Unity Catalog name (default: insurance_command_center)
- `bronze_schema` / `silver_schema` / `gold_schema` - Schema names
- `data_path` - Path to data files (default: /dbfs/FileStore/data)
- `lookback_days` - Days to analyze (default: 30)

### Scheduling (Production)

Create Databricks Jobs to run daily:

```yaml
Job: Insurance Command Center Pipeline
Schedule: Daily at 2:00 AM
Cluster: Job Cluster (8-16 GB, DBR 13.3 LTS)
Tasks:
  1. Bronze Ingestion (01_bronze_ingestion.py)
  2. Silver Transformation (02_silver_transformation.py) - depends on #1
  3. Gold Analytics (03_gold_analytics.py) - depends on #2
  4. Dashboard Refresh (04_command_center_dashboard.py) - depends on #3
```

---

## 📋 Business Requirements

### 2. Scope & Data Foundation
The solution must integrate and correlate disparate experience, transactional, and operational data on the Databricks Lakehouse, including:
• Claims & Prior Authorization
• Call Center Data (ACD, IVR, transcripts, handle time)
• Voice of Customer (Qualtrics surveys, sentiment, effort)
• Digital Interaction Data (logins, failures, abandonment)
• Enrollment & Plan Data
• Pharmacy & Benefits Data
• Socio-demographic & Member Attributes
All data must be linkable at the member journey level, enabling cross-channel journey reconstruction and analysis.
### 3. Core Capabilities (What the System Must Do)
A. Live Journey Monitoring
• Continuously monitor end-to-end member journeys across:
• Digital self-service
• Call center interactions
• Pharmacy and benefits changes
• Detect journey breakdowns (e.g., digital → call leakage, repeat demand) in near real time.
B. Financial Quantification
• Translate experience failures into dollar impact, not just CX scores:
• Avoidable call volume
• Cost per call
• Repeat demand
• Annualized and near-term savings potential
• Allow leaders to model 5–10% reductions and see financial upside in weeks, not years.
C. Integrated Experience Command Center
Deliver a role-based, live dashboard (not static reporting) that includes:
• Digital → Call Failures
• Live count and cost of digital journeys that convert to calls
• Breakdown by topic, plan type, tenure, and segment
• Repeat Calls & Emotion
• Surging repeat demand and negative sentiment
• Correlation to communication gaps, process failures, or agent behaviors
• “Money on the Table”
• Continuously refreshed Top 5 opportunities
• Call volume, cost, trend, and primary levers (digital, comms, ops)
• Member Journey Playback
• Cross-channel timeline view of an individual journey
• Digital failures, calls, sentiment, claims/auth context
4. Agentic AI Requirements (From Insight to Action)
Insight Scout Agent
• Runs daily on the Lakehouse
• Automatically detects:
• Topic spikes
• Repeat call pockets
• Sentiment and effort drops
• Produces human-readable executive briefings, e.g.
“Digital bill-pay failures increased 14% for MAPD Segment X, driving ~$Y in incremental call cost.”
Next-Best-Action Agent
• For each high-value issue, proposes specific, executable actions, such as:
• Digital copy or flow updates
• IVR messaging changes
• FAQ or communication fixes
• Training or QA interventions
• Prioritizes actions by:
• Estimated cost savings
• CX improvement
• Speed to impact
---

## 🎯 Executive Questions

The system answers these 5 critical executive questions in real-time:

### 5. Sample Executive Questions the System Must Answer Live

#### Cost & Prioritization
• Which three journeys are driving the most avoidable call cost right now?
• If we fix one issue this quarter, which saves the most money and why?
• Where are repeat calls that should be one-and-done?
#### Experience & Risk
• Which member segments are experiencing the highest negative emotion?
• Are new members failing more than tenured members—and where?
• Which benefit or cost changes are creating confusion ahead of AEP?

#### Digital Effectiveness
• Which digital journeys are leaking into the call center—and at what rate?
• Where do members abandon online flows and immediately call?
• Which "self-service" journeys are not actually self-service?

#### Operational Alignment
• Are agent behaviors impacting sentiment and repeat calls?
• Which issues are best solved by digital fixes vs training vs communication?

**📊 Find Answers:** Run notebook `04_command_center_dashboard.py`

---

## 📈 Key Metrics

### Current Problem Areas (From Analysis)

Based on the synthetic data generation matching real-world patterns:

| Problem Area | Metric | Impact |
|--------------|--------|--------|
| **Digital Friction** | 1.2M+ calls after failed digital | High cost leakage |
| | 2.4M ID card calls | Digital self-service failing |
| | 2.2M billing calls | Payment confusion |
| **Call Center Strain** | 23.5% repeat call rate | Poor first-call resolution |
| | 32.6% negative emotion | Member dissatisfaction |
| | Pharmacy calls dominate | Complex benefit issues |
| **Communication Gaps** | Hundreds of thousands benefit calls | Confusion driving volume |
| | Long handle times | Expensive interactions |
| | High repeat demand | Not addressing root cause |

### Success Metrics

The POC is successful if it demonstrates:

✅ **Conversion:** Historical insight → Live operational system  
✅ **Financial:** 5-10% cost reduction in targeted journeys  
✅ **Integration:** One source of truth across experience, cost, operations  
✅ **Action-Oriented:** Move from knowing what's broken to fixing it weekly  

---

## 🔧 Configuration

### Cost Assumptions

Edit `config/pipeline_config.json` to adjust:

```json
{
  "cost_assumptions": {
    "cost_per_call": 12.50,           // Average cost per call
    "cost_per_minute": 0.75,          // Additional cost per minute
    "digital_cost_per_interaction": 0.25  // Digital interaction cost
  },
  "business_rules": {
    "leakage_window_hours": 24,       // Digital→Call timeframe
    "repeat_call_window_days": 7,     // Repeat call definition
    "high_handle_time_threshold_minutes": 20,  // Long call threshold
    "negative_sentiment_threshold": -0.5       // Negative sentiment cutoff
  }
}
```

---

## 🔍 Validation & Testing

### Data Quality Checks

Each notebook includes built-in data quality checks:

- **Bronze:** Record counts, schema validation
- **Silver:** Null analysis, referential integrity, deduplication
- **Gold:** Metric validation, trend analysis

### Sample Queries

Test the system with these queries:

```sql
-- Check bronze ingestion
SELECT COUNT(*) FROM insurance_command_center.bronze.members_raw;

-- Check silver transformation
SELECT tenure_bucket, COUNT(*) 
FROM insurance_command_center.silver.members_clean 
GROUP BY tenure_bucket;

-- Check gold analytics
SELECT * FROM insurance_command_center.gold.top_opportunities 
WHERE opportunity_rank <= 5;

-- Check executive KPIs
SELECT * FROM insurance_command_center.gold.executive_kpis 
ORDER BY call_date DESC LIMIT 7;
```

---

## 🎨 Visualization & Dashboards

### Databricks SQL Dashboard

Create a dashboard with these tiles:

1. **Executive KPIs** - Daily trends
2. **Top Opportunities** - Bar chart of savings
3. **Digital Leakage** - Sankey diagram
4. **Member Segments** - Heatmap of sentiment
5. **Journey Timeline** - Timeline visualization

### Sample Dashboard Query

```sql
-- Daily Trend Card
SELECT 
  call_date,
  total_calls,
  repeat_call_pct,
  negative_emotion_pct,
  estimated_daily_call_cost
FROM insurance_command_center.gold.executive_kpis
WHERE call_date >= date_sub(current_date(), 30)
ORDER BY call_date;
```

---

## 🚨 Troubleshooting

### Common Issues

**Issue:** "Table not found" error  
**Solution:** Ensure notebooks run in order (01 → 02 → 03 → 04)

**Issue:** "Permission denied" on Unity Catalog  
**Solution:** Grant proper permissions:
```sql
GRANT CREATE ON CATALOG insurance_command_center TO `your_user`;
GRANT USAGE ON CATALOG insurance_command_center TO `your_user`;
```

**Issue:** Data files not loading  
**Solution:** Verify data path in config matches upload location

**Issue:** Slow performance  
**Solution:** 
- Use larger cluster (16GB+ memory)
- Ensure OPTIMIZE has run on Delta tables
- Check partition strategy

---

## 🔄 Incremental Updates

For ongoing operations after initial load:

1. **Daily Incremental Loads**
   - Place new CSV files with datestamps
   - Bronze notebook handles append logic
   - Silver/Gold refresh automatically

2. **Merge Strategy**
   ```python
   # Bronze notebook uses this pattern
   df.write.format("delta") \
       .mode("append") \
       .saveAsTable(table_path)
   ```

3. **Change Data Feed**
   - Enabled on all tables
   - Tracks inserts, updates, deletes
   - Use for downstream consumers

---

## 📚 Additional Resources

### Documentation
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)

### Best Practices
- Always version control notebooks
- Test on development catalog first
- Monitor job execution times
- Set up alerting for failures
- Document business logic changes

---

## 👥 Contributors

- **Insurance Domain Experts** - Business requirements
- **Data Engineering Team** - Pipeline implementation
- **Analytics Team** - Metric definitions

---

## 📝 License

This project is for demonstration purposes as part of insurance use case implementation.

---

## 🎯 Next Steps

After successful deployment:

1. **Agentic AI Integration** (Phase 2)
   - Insight Scout Agent for automated anomaly detection
   - Next-Best-Action Agent for recommended fixes

2. **Real-Time Processing** (Phase 3)
   - Streaming ingestion with Databricks Structured Streaming
   - Real-time alerting for critical metrics

3. **Advanced Analytics** (Phase 4)
   - Predictive modeling for churn risk
   - Propensity scoring for intervention
   - A/B testing framework for improvements

4. **Operational Integration** (Phase 5)
   - Integrate with CRM systems
   - Automate communication triggers
   - Close the loop with action tracking

---

## 📞 Support

For questions or issues:
- Create GitHub issue
- Contact data engineering team
- Refer to Databricks documentation

---

**This is not another analytics dashboard.**  
**It is a living nervous system watching member experience, quantifying financial impact, and using data to tell teams what to fix next.**

Powered by Databricks Lakehouse Platform 🚀

---

## 📅 Version History

- **v1.0.0** (2024-02-04) - Initial implementation with complete Medallion Architecture
  - Bronze, Silver, Gold layers implemented
  - 7 data sources, 5 gold tables
  - Executive dashboard with 5 key questions
  - 425,000+ synthetic records across all tables

---

### Original Business Requirements Document

#### 1. Objective
• Which member segments are experiencing the highest negative emotion?
• Are new members failing more than tenured members—and where?
• Which benefit or cost changes are creating confusion ahead of AEP?
Digital Effectiveness
• Which digital journeys are leaking into the call center—and at what rate?
• Where do members abandon online flows and immediately call?
• Which “self-service” journeys are not actually self-service?
Operational Alignment
• Are agent behaviors impacting sentiment and repeat calls?
• Which issues are best solved by digital fixes vs training vs communication?
6. Success Criteria
The POC is successful if it:
• Converts historical insight into a live, operational system
• Demonstrates measurable cost reduction (5–10%) in targeted journeys
• Provides executives with one source of truth across experience, cost, and operations
• Moves the organization from knowing what’s broken to fixing it every week
This is not another analytics dashboard.
It is a living nervous system watching member experience, quantifying financial impact, and using agentic AI to tell teams what to fix next, powered by the Databricks platform the client already owns.
This is the full concept for the  implementation. However for presales in the meeting with client next Monday what kind of demo can we prepare with synthetic data.
Some of the current problem areas identified are:
1. Digital friction is massive: millions of failed digital‑first journeys, 1.2M+ calls after failed digital attempts, 2.4M ID card and 2.2M billing calls.
2. Call center strain is real: 23.5% repeat calls, 32.6% negative emotion calls, pharmacy calls dominating volume.
3. Communication & benefit confusion drive cost and churn: hundreds of thousands of benefit‑change and cost calls, long handle times, repeat demand.
