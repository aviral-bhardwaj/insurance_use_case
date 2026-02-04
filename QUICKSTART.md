# Quick Start Guide - Insurance Command Center

Get the Insurance Command Center up and running in 30 minutes.

## 🚀 Prerequisites Checklist

- [ ] Databricks workspace with Unity Catalog enabled
- [ ] DBR 13.0+ cluster (8-16GB recommended)
- [ ] CREATE CATALOG and CREATE SCHEMA permissions
- [ ] Databricks CLI installed (optional, for file uploads)

## 📦 Step 1: Clone & Upload (5 minutes)

### Clone the Repository

```bash
git clone https://github.com/aviral-bhardwaj/insurance_use_case.git
cd insurance_use_case
```

### Upload Data Files

**Option A: Using Databricks CLI**
```bash
# Install CLI if needed
pip install databricks-cli

# Configure authentication
databricks configure --token

# Upload data
databricks fs cp data/ dbfs:/FileStore/data/ --recursive
databricks fs cp config/pipeline_config.json dbfs:/FileStore/config/
```

**Option B: Using Databricks UI**
1. Go to Databricks Workspace → Data → Add Data
2. Click "Upload File" 
3. Upload all CSV files from `data/` folder to `/FileStore/data/`
4. Upload `config/pipeline_config.json` to `/FileStore/config/`

### Verify Uploads

```bash
databricks fs ls dbfs:/FileStore/data/
# Should see: call_center.csv, claims.csv, digital_interactions.csv, etc.
```

## 📓 Step 2: Import Notebooks (3 minutes)

### Import to Workspace

1. In Databricks, go to **Workspace** → **Users** → **[Your User]**
2. Right-click → **Import**
3. Select **File** and import each notebook:
   - `notebooks/01_bronze_ingestion.py`
   - `notebooks/02_silver_transformation.py`
   - `notebooks/03_gold_analytics.py`
   - `notebooks/04_command_center_dashboard.py`

Or use CLI:
```bash
databricks workspace import_dir notebooks/ /Users/your.email@company.com/insurance_use_case/
```

## 🎯 Step 3: Create Catalog (2 minutes)

### Option A: Via Notebook

Run this in any Databricks notebook:

```sql
CREATE CATALOG IF NOT EXISTS insurance_command_center;
USE CATALOG insurance_command_center;
SHOW SCHEMAS;
```

### Option B: Via UI

1. Go to **Data** → **Catalogs**
2. Click **Create Catalog**
3. Name: `insurance_command_center`
4. Click **Create**

## ▶️ Step 4: Run the Pipeline (30-45 minutes)

### Attach to Cluster

Before running, ensure all notebooks are attached to a cluster:
- **Cluster Type:** Single Node or Multi-Node
- **DBR Version:** 13.3 LTS or later
- **Node Type:** 8-16GB memory recommended
- **Auto-termination:** 60 minutes

### Run Sequentially

#### 1️⃣ Bronze Layer (5-10 min)

Open `01_bronze_ingestion.py` and click **Run All**

Wait for completion. You should see:
```
✅ All data sources successfully ingested to bronze layer!
```

**Verify:**
```sql
SELECT COUNT(*) FROM insurance_command_center.bronze.members_raw;
-- Should return: 10,000
```

#### 2️⃣ Silver Layer (10-15 min)

Open `02_silver_transformation.py` and click **Run All**

Wait for completion. You should see:
```
✅ Silver layer transformation complete!
```

**Verify:**
```sql
SELECT tenure_bucket, COUNT(*) 
FROM insurance_command_center.silver.members_clean 
GROUP BY tenure_bucket;
```

#### 3️⃣ Gold Layer (15-20 min)

Open `03_gold_analytics.py` and click **Run All**

Wait for completion. You should see:
```
✅ Gold layer analytics tables created successfully!
```

**Verify:**
```sql
SELECT * FROM insurance_command_center.gold.top_opportunities 
WHERE opportunity_rank <= 5;
```

#### 4️⃣ Dashboard (2-3 min)

Open `04_command_center_dashboard.py` and click **Run All**

View executive insights and answers to all 5 key questions!

## 🎨 Step 5: Create Visualization Dashboard (10 minutes)

### Create Databricks SQL Dashboard

1. Go to **SQL** → **Dashboards** → **Create Dashboard**
2. Name: "Insurance Command Center"

### Add Dashboard Tiles

#### Tile 1: Executive KPIs

```sql
SELECT 
  call_date as Date,
  total_calls as `Total Calls`,
  repeat_call_pct as `Repeat Call %`,
  negative_emotion_pct as `Negative Emotion %`,
  digital_success_rate as `Digital Success %`,
  estimated_daily_call_cost as `Daily Cost`
FROM insurance_command_center.gold.executive_kpis
WHERE call_date >= date_sub(current_date(), 30)
ORDER BY call_date DESC
```

**Visualization:** Line chart with Date on X-axis, metrics on Y-axis

#### Tile 2: Top Opportunities

```sql
SELECT 
  opportunity_rank as `Rank`,
  call_topic as `Journey`,
  call_volume as `Call Volume`,
  ROUND(avoidable_calls_pct, 1) as `Avoidable %`,
  estimated_savings as `Potential Savings`,
  primary_lever as `Action Lever`
FROM insurance_command_center.gold.top_opportunities
WHERE opportunity_rank <= 10
ORDER BY opportunity_rank
```

**Visualization:** Table or Bar chart

#### Tile 3: Digital Leakage

```sql
SELECT 
  failed_interaction_type as `Digital Journey`,
  call_topic as `Resulting Call`,
  SUM(leakage_count) as `Leakage Count`,
  SUM(total_cost) as `Total Cost`
FROM insurance_command_center.gold.digital_call_leakage
WHERE call_date >= date_sub(current_date(), 30)
  AND failed_interaction_type IS NOT NULL
GROUP BY failed_interaction_type, call_topic
ORDER BY SUM(leakage_count) DESC
LIMIT 10
```

**Visualization:** Stacked bar chart or Sankey diagram

#### Tile 4: Segment Health

```sql
SELECT 
  segment,
  plan_type,
  SUM(total_calls) as `Total Calls`,
  ROUND(AVG(repeat_call_rate), 1) as `Repeat Rate %`,
  ROUND(AVG(negative_sentiment_pct), 1) as `Negative %`
FROM insurance_command_center.gold.repeat_calls_analysis
WHERE call_date >= date_sub(current_date(), 30)
GROUP BY segment, plan_type
ORDER BY AVG(negative_sentiment_pct) DESC
```

**Visualization:** Heatmap

### Refresh Schedule

Set dashboard to refresh:
- Every 1 hour during business hours
- Or manually after pipeline runs

## ✅ Step 6: Validation Checklist

Run these queries to validate everything works:

### Bronze Layer
```sql
-- Check all bronze tables exist
SHOW TABLES IN insurance_command_center.bronze;

-- Verify row counts
SELECT 'members_raw' as table_name, COUNT(*) as rows FROM insurance_command_center.bronze.members_raw
UNION ALL
SELECT 'claims_raw', COUNT(*) FROM insurance_command_center.bronze.claims_raw
UNION ALL
SELECT 'call_center_raw', COUNT(*) FROM insurance_command_center.bronze.call_center_raw;
```

Expected:
- members_raw: 10,000
- claims_raw: 50,000
- call_center_raw: 100,000
- digital_interactions_raw: 150,000
- surveys_raw: 25,000
- pharmacy_raw: 75,000
- enrollment_raw: 15,000

### Silver Layer
```sql
-- Check data quality
SELECT 
  'Digital Leakage Detected' as check_name,
  COUNT(*) as count
FROM insurance_command_center.silver.call_center_clean
WHERE is_digital_leakage = true;
```

Expected: ~40,000 (40% of calls show digital leakage)

### Gold Layer
```sql
-- Check top opportunity
SELECT 
  call_topic,
  estimated_savings,
  primary_lever
FROM insurance_command_center.gold.top_opportunities
WHERE opportunity_rank = 1;
```

Expected: Should show highest-cost opportunity

## 🔄 Step 7: Schedule Daily Runs (Optional)

### Create Databricks Job

1. Go to **Workflows** → **Create Job**
2. Job Name: "Insurance Command Center Daily Pipeline"
3. Add Tasks:

**Task 1: Bronze Ingestion**
- Type: Notebook
- Path: `01_bronze_ingestion.py`
- Cluster: Shared job cluster
- Depends on: (none)

**Task 2: Silver Transformation**
- Type: Notebook
- Path: `02_silver_transformation.py`
- Cluster: Shared job cluster
- Depends on: Task 1

**Task 3: Gold Analytics**
- Type: Notebook
- Path: `03_gold_analytics.py`
- Cluster: Shared job cluster
- Depends on: Task 2

**Task 4: Dashboard Refresh**
- Type: Notebook
- Path: `04_command_center_dashboard.py`
- Cluster: Shared job cluster
- Depends on: Task 3

4. Schedule: Daily at 2:00 AM (or preferred time)
5. Alerts: Email on failure
6. Click **Create**

## 🎯 Key Metrics to Monitor

After setup, monitor these daily:

| Metric | Target | Alert If |
|--------|--------|----------|
| Repeat Call % | < 20% | > 25% |
| Negative Emotion % | < 25% | > 35% |
| Digital Success Rate | > 80% | < 70% |
| Avg Handle Time | < 12 min | > 15 min |
| Digital Leakage Rate | < 35% | > 45% |

## 🆘 Troubleshooting

### Issue: "Catalog not found"
**Solution:**
```sql
CREATE CATALOG IF NOT EXISTS insurance_command_center;
USE CATALOG insurance_command_center;
```

### Issue: "File not found" error
**Solution:** Verify data path in notebook widget:
```python
# Check this matches your upload location
data_path = "/dbfs/FileStore/data"  # Default
# OR
data_path = "dbfs:/FileStore/data"   # Alternative
```

### Issue: Notebooks run slowly
**Solution:**
- Increase cluster size (16GB+ memory)
- Enable auto-scaling
- Check if cluster is in the same region as workspace

### Issue: Permission denied
**Solution:**
```sql
GRANT CREATE ON CATALOG insurance_command_center TO `your.email@company.com`;
GRANT USAGE ON CATALOG insurance_command_center TO `your.email@company.com`;
GRANT SELECT ON CATALOG insurance_command_center TO `your.email@company.com`;
```

### Issue: Out of memory
**Solution:**
- Use larger cluster (Standard_D16s_v3 or similar)
- Enable disk spilling in Spark config
- Process data in smaller batches

## 📊 Sample Queries for Exploration

### Find High-Cost Call Topics
```sql
SELECT 
  call_topic,
  SUM(estimated_call_cost) as total_cost,
  COUNT(*) as call_count
FROM insurance_command_center.silver.call_center_clean
GROUP BY call_topic
ORDER BY total_cost DESC
LIMIT 10;
```

### Identify Problem Digital Journeys
```sql
SELECT 
  interaction_type,
  outcome,
  COUNT(*) as count
FROM insurance_command_center.silver.digital_interactions_clean
GROUP BY interaction_type, outcome
ORDER BY COUNT(*) DESC;
```

### Member Journey Analysis
```sql
SELECT 
  member_id,
  event_type,
  event_timestamp,
  event_outcome
FROM insurance_command_center.gold.member_journey_timeline
WHERE member_id = 'M00000042'  -- Replace with any member ID
ORDER BY event_timestamp
LIMIT 50;
```

## 🎓 Next Steps

1. **Explore the data** - Run sample queries to understand patterns
2. **Create custom dashboards** - Build visualizations for your use cases
3. **Share with stakeholders** - Demo the command center to executives
4. **Iterate on insights** - Use findings to improve member experience
5. **Plan Phase 2** - Consider agentic AI and real-time capabilities

## 📞 Support

- Check `README.md` for detailed documentation
- Review `ARCHITECTURE.md` for system design
- Search Databricks documentation for platform help
- Open GitHub issues for bugs or questions

---

## ⏱️ Total Setup Time

- **Prerequisites & Setup:** 10 minutes
- **First Pipeline Run:** 30-45 minutes
- **Dashboard Creation:** 10 minutes
- **Total:** ~1 hour to fully operational command center

---

**🎉 Congratulations! Your Insurance Command Center is live!**

You now have a complete Medallion Architecture implementation with:
- ✅ 425,000+ synthetic records
- ✅ 7 data sources ingested
- ✅ 3-layer data architecture (Bronze/Silver/Gold)
- ✅ 5 business analytics tables
- ✅ Executive dashboard answering 5 critical questions
- ✅ Real-time insights into member experience

Start exploring and driving action! 🚀
