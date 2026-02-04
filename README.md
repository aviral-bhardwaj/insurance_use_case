# Insurance Member Experience Command Center

A complete end-to-end implementation using Databricks Medallion Architecture to transform member interactions into actionable insights.

## 🎯 Objective
Transform a one-time, retrospective analysis of 100M+ member interactions into a live, integrated nervous system that continuously monitors, explains, and improves member experience while quantifying real financial impact and driving daily action, not annual insights.

This implementation enables executives to see where experience and cost are leaking, understand why, and activate fixes across digital, call center, and operations in near real time.

---

## 🚀 Quick Reference

| 🎯 Goal | 📍 Location |
|---------|-------------|
| **Get started in 5 minutes** | [Setup Instructions](#setup-instructions) |
| **Understand the architecture** | [Architecture Overview](#architecture-overview) |
| **Answer executive questions** | [Executive Questions](#executive-questions) |
| **View key metrics** | [Key Metrics](#key-metrics) |
| **Adjust costs/rules** | [Configuration](#configuration) |
| **Fix problems** | [Troubleshooting](#troubleshooting) |

---

## 📋 Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Repository Structure](#repository-structure)
3. [Data Dictionary](#data-dictionary)
4. [Setup Instructions](#setup-instructions)
5. [Running the Pipeline](#running-the-pipeline)
6. [Business Capabilities](#business-capabilities)
7. [Executive Questions](#executive-questions)
8. [Key Metrics](#key-metrics)
9. [Advanced Topics](#advanced-topics)

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

| Layer | Purpose | Tables | Key Features |
|-------|---------|--------|--------------|
| 🥉 **Bronze** | Preserve raw data | 7 `*_raw` tables | Schema enforcement, ingestion metadata, change data feed |
| 🥈 **Silver** | Clean & enrich | 7 `*_clean` tables | Deduplication, validation, business logic (leakage, repeat calls), SCD Type 2 |
| 🥇 **Gold** | Business metrics | 5 analytics tables | Digital leakage, repeat calls, opportunities, journeys, executive KPIs |

---

## 📁 Repository Structure

```
insurance_use_case/
│
├── data/                          # Synthetic insurance datasets (425K+ records)
│   ├── members.csv               # 10K member demographics
│   ├── claims.csv                # 50K claims transactions
│   ├── call_center.csv           # 100K call interactions
│   ├── digital_interactions.csv  # 150K digital touchpoints
│   ├── surveys.csv               # 25K customer surveys
│   ├── pharmacy.csv              # 75K pharmacy transactions
│   └── enrollment.csv            # 15K enrollment records
│
├── config/
│   └── pipeline_config.json      # Pipeline configuration & costs
│
├── notebooks/                     # Databricks notebooks (run sequentially)
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_analytics.py
│   └── 04_command_center_dashboard.py
│
├── generate_synthetic_data.py    # Data generation script
├── README.md                      # This file
├── ARCHITECTURE.md                # Technical architecture details
├── QUICKSTART.md                  # Quick start guide
└── .gitignore
```

---

## 📊 Data Dictionary

### Members (10K records)
Member demographics: `member_id`, `age`, `gender`, `plan_type` (MAPD/PPO/HMO/POS/EPO), `tenure_months`, `segment` (High/Medium/Low Value, At Risk, New), `state`, `zip_code`, `join_date`

### Claims (50K records)
Claims & prior auth: `claim_id`, `member_id`, `claim_date`, `claim_type` (Medical/Pharmacy/Preventive), `claim_amount`, `status` (Approved/Denied/Pending), `approval_time_days`, `denial_reason`

### Call Center (100K records)
Call interactions: `call_id`, `member_id`, `call_date`, `call_timestamp`, `call_topic` (ID Card/Billing/Claims/etc), `handle_time_minutes`, `sentiment` (positive/neutral/negative), `is_repeat_call`, `prior_digital_attempt`, `transferred`, `resolved`

### Digital Interactions (150K records)
Website/app usage: `interaction_id`, `member_id`, `interaction_date`, `interaction_timestamp`, `interaction_type` (login/bill_pay/claims_check/etc), `outcome` (success/failure/abandoned), `failure_reason`, `session_duration_seconds`

### Surveys (25K records)
Voice of Customer: `survey_id`, `member_id`, `survey_date`, `nps_score` (0-10), `csat_score` (1-5), `effort_score` (1-7, lower=better), `feedback_text`, `survey_type` (post_call/post_digital/periodic/annual)

### Pharmacy (75K records)
Pharmacy transactions: `rx_id`, `member_id`, `fill_date`, `drug_name`, `copay_amount`, `days_supply` (30/60/90), `pharmacy_type` (retail/mail/specialty), `denied_flag`, `denied_reason`

### Enrollment (15K records)
Plan enrollment: `enrollment_id`, `member_id`, `enrollment_date`, `plan_type`, `premium_amount`, `change_type` (new/renewal/change), `change_reason`

<details>
<summary>📖 View detailed field descriptions</summary>

Full schema specifications with data types, constraints, and business rules are embedded in notebook `01_bronze_ingestion.py`. All tables use Delta Lake format with schema enforcement and change data feed enabled.

</details>

---

## 🚀 Setup Instructions

### Prerequisites

**Required:**
- Databricks Workspace with Unity Catalog enabled
- DBR 13.0+ (or latest LTS)
- Python 3.9+
- CREATE CATALOG, CREATE SCHEMA, CREATE TABLE permissions

### Quick Start (5 Minutes)

```bash
# 1. Clone repository
git clone https://github.com/aviral-bhardwaj/insurance_use_case.git
cd insurance_use_case

# 2. Upload data to Databricks FileStore
databricks fs cp data/ dbfs:/FileStore/data/ --recursive
databricks fs cp config/pipeline_config.json dbfs:/FileStore/config/

# 3. Import notebooks to workspace
# Via UI: Workspace → Import → Select notebooks/

# 4. Create Unity Catalog
```
```sql
CREATE CATALOG IF NOT EXISTS insurance_command_center;
```

**📚 Detailed setup guide:** See [QUICKSTART.md](QUICKSTART.md) | [ARCHITECTURE.md](ARCHITECTURE.md)

---

## ▶️ Running the Pipeline

### Sequential Execution (First Time)

| # | Notebook | Duration | Output |
|---|----------|----------|--------|
| 1️⃣ | `01_bronze_ingestion.py` | ~5-10 min | 7 raw tables (bronze schema) |
| 2️⃣ | `02_silver_transformation.py` | ~10-15 min | 7 clean tables with enrichment |
| 3️⃣ | `03_gold_analytics.py` | ~15-20 min | 5 business analytics tables |
| 4️⃣ | `04_command_center_dashboard.py` | ~2-3 min | Executive insights & visualizations |

**Total time:** ~30-45 minutes initial load | ~10-15 minutes incremental

### Notebook Parameters (Widgets)

All notebooks accept:
- `catalog` - Unity Catalog name (default: `insurance_command_center`)
- `bronze_schema` / `silver_schema` / `gold_schema` - Schema names
- `data_path` - Data location (default: `/dbfs/FileStore/data`)
- `lookback_days` - Analysis window (default: 30)

### Production Scheduling

```yaml
Job: Insurance Command Center Pipeline
Schedule: Daily at 2:00 AM
Cluster: Job Cluster (8-16 GB, DBR 13.3 LTS)
Tasks:
  01_bronze_ingestion → 02_silver_transformation → 03_gold_analytics → 04_dashboard_refresh
```

---

## 💼 Business Capabilities

### Core System Functions

| Capability | Description | Implementation |
|------------|-------------|----------------|
| 🔍 **Live Journey Monitoring** | Track digital→call leakage (24h window), repeat calls (7d window), cross-channel journeys | Bronze-Silver-Gold pipeline |
| 💰 **Financial Quantification** | Cost per call ($12.50 + $0.75/min), digital ($0.25), opportunity sizing, ROI modeling | `pipeline_config.json` + Gold analytics |
| 📊 **Executive Command Center** | Top 5 opportunities, digital leakage dashboard, repeat call analytics, journey playback | Gold tables + Dashboard notebook |

### Data Foundation

**7 Integrated Sources:** Claims & Prior Auth | Call Center (ACD/IVR) | Voice of Customer (surveys) | Digital Interactions | Enrollment | Pharmacy | Member Demographics

**Key Feature:** All data linkable at member-journey level for cross-channel analysis

### Agentic AI (Phase 2 - Future)

<details>
<summary><b>Insight Scout Agent</b> - Automated anomaly detection</summary>

- Runs daily, detects topic spikes, repeat call pockets, sentiment/effort drops
- Produces briefings: "Digital bill-pay failures +14% for MAPD Segment X = $Y cost"

</details>

<details>
<summary><b>Next-Best-Action Agent</b> - Recommended fixes</summary>

- Proposes actions: digital fixes, IVR changes, FAQ updates, training
- Prioritizes by: cost savings, CX improvement, speed to impact

</details>

---

## 🎯 Executive Questions

The system answers these questions in real-time:

### 💰 Cost & Prioritization

| Question | Gold Table |
|----------|------------|
| Which 3 journeys drive the most avoidable call cost? | `top_opportunities` |
| If we fix one issue this quarter, which saves the most? | `top_opportunities` |
| Where are repeat calls that should be one-and-done? | `repeat_calls_analysis` |

### 😟 Experience & Risk

| Question | Gold Table |
|----------|------------|
| Which segments have highest negative emotion? | `executive_kpis` |
| Are new members failing more than tenured—where? | `member_journey_timeline` |
| Which benefit changes create confusion before AEP? | `digital_call_leakage` |

### 💻 Digital Effectiveness

| Question | Gold Table |
|----------|------------|
| Which digital journeys leak into call center—at what rate? | `digital_call_leakage` |
| Where do members abandon online and immediately call? | `digital_call_leakage` |
| Which "self-service" journeys aren't actually self-service? | `digital_call_leakage` |

### 🔧 Operational Alignment

| Question | Gold Table |
|----------|------------|
| Are agent behaviors impacting sentiment/repeat calls? | `repeat_calls_analysis` |
| Which issues need digital fixes vs training vs comms? | `top_opportunities` |

**📊 Run queries:** Execute notebook `04_command_center_dashboard.py`

---

## 📈 Key Metrics

### Current Problem Areas

Based on synthetic data matching real-world patterns:

| Problem Area | Metric | Impact |
|--------------|--------|--------|
| 🖥️ **Digital Friction** | 1.2M+ calls after failed digital<br/>2.4M ID card calls<br/>2.2M billing calls | High cost leakage<br/>Self-service failing<br/>Payment confusion |
| ☎️ **Call Center Strain** | 23.5% repeat call rate<br/>32.6% negative emotion<br/>Pharmacy calls dominate | Poor first-call resolution<br/>Member dissatisfaction<br/>Complex benefit issues |
| 📢 **Communication Gaps** | Hundreds of thousands benefit calls<br/>Long handle times | Confusion driving volume<br/>Expensive interactions |

### Success Criteria

✅ **Conversion:** Historical insight → Live operational system  
✅ **Financial:** 5-10% cost reduction in targeted journeys  
✅ **Integration:** One source of truth (experience + cost + operations)  
✅ **Action-Oriented:** Weekly fixes, not annual insights  

---

## 🔧 Configuration

Edit `config/pipeline_config.json` to adjust costs and business rules:

```json
{
  "cost_assumptions": {
    "cost_per_call": 12.50,
    "cost_per_minute": 0.75,
    "digital_cost_per_interaction": 0.25
  },
  "business_rules": {
    "leakage_window_hours": 24,
    "repeat_call_window_days": 7,
    "high_handle_time_threshold_minutes": 20,
    "negative_sentiment_threshold": -0.5
  }
}
```

---

## 🔬 Advanced Topics

<details>
<summary><b>🧪 Validation & Testing</b></summary>

### Data Quality Checks
Each notebook includes: **Bronze** (record counts, schema validation) | **Silver** (null analysis, referential integrity, deduplication) | **Gold** (metric validation, trend analysis)

### Sample Validation Queries

```sql
-- Bronze ingestion
SELECT COUNT(*) FROM insurance_command_center.bronze.members_raw;

-- Silver transformation
SELECT tenure_bucket, COUNT(*) FROM insurance_command_center.silver.members_clean GROUP BY tenure_bucket;

-- Gold analytics
SELECT * FROM insurance_command_center.gold.top_opportunities WHERE opportunity_rank <= 5;

-- Executive KPIs
SELECT * FROM insurance_command_center.gold.executive_kpis ORDER BY call_date DESC LIMIT 7;
```

</details>

<details>
<summary><b>🚨 Troubleshooting</b></summary>

**"Table not found"** → Run notebooks in order (01→02→03→04)

**"Permission denied"** → Grant permissions:
```sql
GRANT CREATE ON CATALOG insurance_command_center TO `your_user`;
GRANT USAGE ON CATALOG insurance_command_center TO `your_user`;
```

**Data files not loading** → Verify `data_path` in config matches upload location

**Slow performance** → Use larger cluster (16GB+), run OPTIMIZE on tables, check partitioning

</details>

<details>
<summary><b>🔄 Incremental Updates</b></summary>

### Daily Operations
1. Place new CSV files with datestamps
2. Bronze notebook handles append logic
3. Silver/Gold refresh automatically

### Merge Strategy
```python
df.write.format("delta").mode("append").saveAsTable(table_path)
```

### Change Data Feed
Enabled on all tables for downstream tracking (inserts, updates, deletes)

</details>

<details>
<summary><b>📊 Dashboards & Visualization</b></summary>

### Databricks SQL Dashboard Tiles
1. **Executive KPIs** - Daily trends
2. **Top Opportunities** - Savings bar chart
3. **Digital Leakage** - Sankey diagram
4. **Member Segments** - Sentiment heatmap
5. **Journey Timeline** - Individual journey view

### Sample Query
```sql
SELECT call_date, total_calls, repeat_call_pct, negative_emotion_pct, estimated_daily_call_cost
FROM insurance_command_center.gold.executive_kpis
WHERE call_date >= date_sub(current_date(), 30)
ORDER BY call_date;
```

</details>

<details>
<summary><b>📚 Best Practices & Resources</b></summary>

### Development Guidelines
- Version control notebooks | Test on dev catalog first | Monitor job times | Set up alerting | Document changes

### Resources
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog Docs](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)

</details>

<details>
<summary><b>🛣️ Roadmap / Next Steps</b></summary>

**Phase 2:** Agentic AI (Insight Scout + Next-Best-Action)  
**Phase 3:** Real-time processing (Structured Streaming, alerting)  
**Phase 4:** Advanced analytics (churn prediction, propensity scoring, A/B testing)  
**Phase 5:** CRM integration, automated triggers, action tracking  

</details>

---

## 📞 Support

**Questions?** Create GitHub issue | Contact data engineering team | See Databricks docs

---

## 👥 Contributors

**Insurance Domain Experts** (requirements) | **Data Engineering** (pipeline) | **Analytics** (metrics)

---

## 📝 License

Demonstration project for insurance use case implementation.

---

## 📅 Version History

**v1.0.0** (2024-02-04) - Initial implementation: Bronze/Silver/Gold layers, 7 data sources, 5 gold tables, executive dashboard, 425K+ synthetic records

---

**This is not another analytics dashboard.**  
**It is a living nervous system watching member experience, quantifying financial impact, and driving teams to fix what matters next.**

Powered by Databricks Lakehouse Platform 🚀
