# 📊 Tableau Business Intelligence System
### Multi-Source Enterprise BI Dashboard - E-Commerce Analytics

---

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Tools & Technologies](#tools--technologies)
3. [Dataset Description](#dataset-description)
4. [Data Quality & Engineering](#data-quality--engineering)
5. [Star Schema Architecture](#star-schema-architecture)
6. [Advanced Analytics](#advanced-analytics)
   - [LOD Expressions](#lod-expressions)
   - [Cohort Analysis](#cohort-analysis)
   - [Predictive Forecasting](#predictive-forecasting)
   - [Clustering & RFM Segmentation](#clustering--rfm-segmentation)
   - [Statistical KPIs](#statistical-kpis)
7. [Dashboard Design](#dashboard-design)
8. [Performance Optimization](#performance-optimization)
9. [Theoretical Concepts](#theoretical-concepts)
10. [Business Insights Summary](#business-insights-summary)

---

## Project Overview

This project implements a **multi-source, enterprise-level Business Intelligence system** using Tableau Desktop. It demonstrates real-world data modeling, advanced analytics, predictive forecasting, cohort retention analysis, K-Means clustering, RFM segmentation, and performance optimization - all applied to a structured e-commerce dataset.

The end result is a suite of **three fully interactive dashboards** targeting different business audiences: executive management, customer analytics teams, and operational risk teams.

---

## Tools & Technologies

| Tool | Purpose |
|------|---------|
| **Tableau Desktop** | Primary BI and visualization platform |
| **Python** (`pandas`, `numpy`) | Data preprocessing and engineering |
| **Microsoft Excel** | Data storage format (.xlsx) |

---

## Dataset Description

The dataset is a structured multi-table e-commerce dataset covering **2021–2025**, organized into a **star schema** with 7 tables.

### Data Sources

| Table | Type | Rows | Key Fields |
|-------|------|------|-----------|
| Sales Fact | Fact | 8,014 | Order ID, Customer ID, Product ID, Sales, Profit, Discount |
| Customer Transactions | Fact | 750 | Customer ID, Total Orders, Total Sales, Avg Order Value |
| Customer Dimension | Dimension | 810 | Customer ID, Name, Segment, Signup Date |
| Product Dimension | Dimension | 170 | Product ID, Name, Category, Sub Category, Price |
| Geography Dimension | Dimension | 43 | Geography ID, Country, Region, State, City |
| Time Dimension | Dimension | 1,826 | Time ID, Date, Month, Quarter, Year |
| Marketing Campaign | Dimension | 30 | Campaign ID, Campaign Name, Campaign Type |

### Dataset Scale at a Glance

- 🧑‍🤝‍🧑 **750 customers** across 4 segments (Corporate, Consumer, Home Office, Unknown)
- 📦 **170 products** across 4 categories
- 🛒 **8,000+ orders** spanning 4 years
- 📣 **30 marketing campaigns** across **43 geographic locations** in **5 countries**

---

## Data Quality & Engineering

### Issues Identified & Resolved

| Issue | Table | Count | Resolution |
|-------|-------|-------|-----------|
| Duplicate rows | Sales Fact | 146 | Removed (kept first occurrence) |
| Missing Discount | Sales Fact | 163 | Filled with `0` |
| Missing Ship Mode | Sales Fact | 163 | Filled with `"Standard Class"` |
| Missing Segment | Customer Dim | 22 | Filled with `"Unknown"` |
| Missing Sub Category | Product Dim | 3 | Filled with `"Uncategorized"` |
| Missing Email | Customer Dim | 22 | Filled with placeholder |

### Deduplication

146 duplicate transaction rows were identified and removed using Python's `drop_duplicates()`:

```
Before: 8,160 rows  →  After: 8,014 rows  (146 removed)
```

### Slowly Changing Dimensions - SCD Type 2

SCD Type 2 was implemented to **preserve complete history** of customer segment changes instead of overwriting records.

- **60 customers (~8%)** had segment changes over the data period
- Original rows were closed: `Effective_End_Date` set, `Is_Current = False`
- New rows created with updated segment and `Is_Current = True`
- Each row received a unique **Surrogate Key** (`Customer_SK`)

```
Example: A customer who moved from "Consumer" → "Corporate"
→ Row 1: Consumer | Effective: 2021-01-01 → 2023-06-15 | Is_Current: FALSE
→ Row 2: Corporate | Effective: 2023-06-16 → 9999-12-31 | Is_Current: TRUE
```

### Surrogate Keys

Integer surrogate keys were added to **all dimension tables** for:
- ⚡ Faster joins (integer vs. string comparison)
- 🕰️ SCD Type 2 version row support
- 🔒 Source-system independence

### Live vs. Extract Connections

| Feature | Live | Extract |
|---------|------|---------|
| Data freshness | Real-time | Snapshot (refresh needed) |
| Speed | Slower for large data | Much faster |
| Offline access | ❌ Not possible | ✅ Yes |
| Best for | Small data, real-time | Large data, dashboards |

> **Choice for this project:** Extract connection was used for dashboard performance.

### Hyper Engine Compression

Tableau's Hyper Engine applies:
- **Columnar storage** - optimized for analytical queries
- **Dictionary encoding** - deduplicates repeated string values
- **Run-length encoding** - compresses sequential repeating values
- **Bit packing** - reduces integer storage size

**Result:** 10MB Excel file → ~2–3MB `.hyper` extract with **10–100× faster queries**

---

## Star Schema Architecture

The data model follows a **Star Schema** with `Sales_Fact` at the center, connected to five dimension tables via foreign keys using Tableau's **Logical Layer (Relationships)**.

```
                                        ┌─────────────────────────┐
                                   ┌───▶│   Customer_Dimension    │
                                   │    └─────────────────────────┘
                                   │
                                   │    ┌─────────────────────────┐
                                   ├───▶│  Customer_Transactions  │
                                   │    └─────────────────────────┘
                                   │
                                   │    ┌─────────────────────────┐
┌──────────────────┐               ├───▶│   Geography_Dimension   │
│    Sales_Fact    │───────────────┤    └─────────────────────────┘
└──────────────────┘               │
                                   │    ┌─────────────────────────┐
                                   ├───▶│   Marketing_Campaign    │
                                   │    └─────────────────────────┘
                                   │
                                   │    ┌─────────────────────────┐
                                   ├───▶│   Product_Dimension     │
                                   │    └─────────────────────────┘
                                   │
                                   │    ┌─────────────────────────┐
                                   └───▶│   Time_Dimension        │
                                        └─────────────────────────┘
```

### Why Relationships over Joins?

- Automatically handle join types per query context
- Avoid data duplication in memory
- Respect the grain of each table independently
- Handle null foreign keys gracefully (e.g., organic orders with no `Campaign_ID`)

### Referential Integrity

- ✅ 0 orphan Customer IDs
- ✅ 0 orphan Product IDs
- ℹ️ `Campaign_ID` nulls represent organic (non-campaign) orders → labeled `"Organic / No Campaign"` via Tableau aliases

---

## Advanced Analytics

### LOD Expressions

LOD (Level of Detail) expressions calculate at a **different granularity** than the current visualization. Three types were implemented:

#### FIXED - Customer Lifetime Value
```
{ FIXED [Customer ID] : SUM([Sales]) }
```
Calculates total lifetime revenue per customer at a fixed level, **independent of view filters** (except Context and Data Source filters).

#### INCLUDE - Regional Sales Granularity
```
{ INCLUDE [Region] : SUM([Sales]) }
```
Adds dimensions **beyond** what is in the view, making the calculation more granular.

#### EXCLUDE - Overall Average Benchmark
```
{ EXCLUDE [Category] : AVG([Sales]) }
```
Removes dimensions from the view to compare detail-level data against an overall average.

#### LOD Performance Comparison

| LOD Type | Granularity | Performance | Best Use Case |
|----------|-------------|-------------|---------------|
| `FIXED` | Independent of view | ⚡ Fastest | Customer-level metrics, KPIs |
| `INCLUDE` | Finer than view | ⚙️ Moderate | Adding hidden detail dimensions |
| `EXCLUDE` | Coarser than view | 🐢 Slowest | Comparing to overall averages |

#### Tableau Order of Operations

```
1. Extract Filters
2. Data Source Filters
3. Context Filters
4. FIXED LOD Calculations      ← not affected by dimension filters
5. Dimension Filters
6. INCLUDE / EXCLUDE LOD Calculations
7. Measure Filters
8. Table Calculations
9. Trend Lines, Reference Lines
```

---

### Cohort Analysis

#### Acquisition Month Assignment
Each customer is assigned to a cohort based on their **first purchase month**:

```
DATETRUNC('month', { FIXED [Customer ID] : MIN([Order Date]) })
```

#### Retention Rate Formula

```
Retention Rate = COUNTD([Customer ID]) / ATTR([Cohort Size])

Where:
Cohort Size = { FIXED [Acquisition Month] : COUNTD([Customer ID]) }
```

The heatmap is structured as:
- **Rows** → Acquisition Month (cohort)
- **Columns** → Cohort Age (months since first purchase)
- **Color** → Retention percentage (darker = higher)

#### Survival Curve

A survival curve was also built to show cumulative retention - the percentage of each cohort that remains active at each month interval.

#### Key Findings from Cohort Analysis

| Insight | Detail |
|---------|--------|
| Early drop-off | All cohorts show rapid decline from Month 0 to Month 5 |
| Long-term stability | Retention stabilizes after Month 5–6 |
| Older cohorts | 2021–2022 cohorts show steadier long-term retention |
| Q4 cohorts | Slightly higher retention, likely due to holiday promotions |
| 12-month average | Retention across all cohorts settles at **~10–15%** |

---

### Predictive Forecasting

#### Setup

| Parameter | Value |
|-----------|-------|
| Method | Exponential Smoothing (Tableau built-in) |
| Data | Monthly aggregated sales |
| Forecast period | 6–12 months ahead |
| Confidence band | 95% |
| Seasonal pattern | None detected |

#### Forecast Model Output

```
Initial value (December 2025): 91,969 ± 28,299
Change from initial: 0
Trend Effect: 0.0%
Quality: OK
```

#### MAPE - Forecast Accuracy Metric

$$\text{MAPE} = \frac{1}{n} \sum_{i=1}^{n} \left| \frac{A_i - F_i}{A_i} \right| \times 100$$

| MAPE Range | Rating |
|------------|--------|
| < 10% | ✅ Excellent |
| 10–20% | 👍 Good |
| 20–50% | ⚠️ Reasonable |
| > 50% | ❌ Inaccurate |

---

### Clustering & RFM Segmentation

#### K-Means Clustering

Tableau's built-in clustering (K-Means algorithm) was applied on three measures:

| Feature | Meaning |
|---------|---------|
| **Recency** | Days since last purchase |
| **Frequency** | Number of orders |
| **Monetary** | Total spend |

Four clusters were identified, visualized as a scatter plot (Recency vs. Monetary, sized by Frequency).

#### RFM Scoring

Each customer receives a score of 1–5 on each dimension:

| Component | Measure | Score Range |
|-----------|---------|-------------|
| Recency (R) | Days since last purchase | 1 (old) → 5 (recent) |
| Frequency (F) | Count of orders | 1 (rare) → 5 (frequent) |
| Monetary (M) | Total revenue | 1 (low) → 5 (high) |

**Composite Score:**
```
RFM Score = R × 100 + F × 10 + M
e.g., 555 = Best customer,  111 = Worst customer
```

**Weighted RFM Index:**
```
RFM Index = (R × 0.35) + (F × 0.35) + (M × 0.30)
```
Produces a single 1–5 score for overall customer ranking.

#### RFM Segments & Actions

| Segment | Criteria | Recommended Action |
|---------|----------|--------------------|
| **Champions** | R ≥ 4, F ≥ 4, M ≥ 4 | Reward, upsell |
| **Loyal Customers** | R ≥ 4, F ≥ 2 | Loyalty programs |
| **At Risk** | R ≤ 2, F ≥ 3, M ≥ 3 | Win-back campaigns |
| **Hibernating** | R ≤ 2, F ≤ 2 | Re-engagement or release |

#### RFM Distribution (approximate)

| Segment | Share |
|---------|-------|
| Champions | ~15% |
| Loyal Customers | ~25% |
| At Risk | ~20% |
| Hibernating | ~15% |
| Others (Promising, New, etc.) | ~25% |

---

### Statistical KPIs

All implemented as **calculated fields** in Tableau:

#### Z-Score Anomaly Detection

$$Z = \frac{X - \mu}{\sigma}$$

```tableau
(SUM([Sales]) - WINDOW_AVG(SUM([Sales]))) / WINDOW_STDEV(SUM([Sales]))
```
> |Z| > 2 flags anomalous months requiring investigation.

#### 95% Confidence Intervals

$$\text{Upper CI} = \bar{X} + 1.96 \times \frac{\sigma}{\sqrt{n}} \quad \text{Lower CI} = \bar{X} - 1.96 \times \frac{\sigma}{\sqrt{n}}$$

#### Additional Calculated KPIs

| KPI | Formula |
|-----|---------|
| 3-Month Moving Average | `WINDOW_AVG(SUM([Sales]), -2, 0)` |
| Rolling Std Dev | `WINDOW_STDEV(SUM([Sales]), -5, 0)` |
| Profit Margin | `SUM([Profit]) / SUM([Sales])` |
| Average Order Value (AOV) | `SUM([Sales]) / COUNTD([Order ID])` |

---

## Dashboard Design

Three interactive dashboards were built, each targeting a different business audience.

---

### 1. Executive Control Panel

**Audience:** Senior management / C-Suite  
**Purpose:** High-level KPI overview with interactive drill-down

**Components:**
- KPI Cards: Revenue, Profit, Orders, Customers, AOV
- Monthly Sales Trend (line chart)
- Top N Products (parameter-driven - adjustable N)
- Category Treemap
- Regional Sales Performance (bar chart)

**Key Metrics Displayed:**

| KPI | Value |
|-----|-------|
| Total Revenue | ₹5,397,039 |
| Total Profit | ₹2,014,046 |
| Total Orders | 8,000 |
| Total Customers | 750 |
| Average Order Value | ₹675 |

**Key Insights:**
- Technology dominates revenue; Furniture shows higher profit margins
- Surface Laptop and Laptop Sleeve are top revenue-generating products
- Top N parameter allows dynamic exploration of product rankings

---

### 2. Customer Intelligence Lab

**Audience:** Marketing and customer success teams  
**Purpose:** Deep customer behavioral analytics

**Components:**
- Cohort Retention Heatmap
- K-Means Cluster Scatter Plot (Recency vs. Monetary)
- RFM Segment Distribution (bar chart)
- Survival Curve (cumulative retention by cohort)
- CLV Distribution (histogram)

**Key Insights:**
- Champions and Loyal Customers represent the highest-value segments
- Retention drops sharply in the first 3 months, then stabilizes
- K-Means clusters align well with RFM-based segmentation

---

### 3. Operational Risk Dashboard

**Audience:** Operations and finance teams  
**Purpose:** Identify anomalies, profit leakage, and discount abuse

**Components:**
- Z-Score Anomaly Detection (time series with ±2σ bands)
- Discount Abuse Scatter (Discount vs. Profit)
- Profit Leakage Heatmap (Category × Region)
- Loss Orders Table
- Shipping Mode Performance (avg. profit by ship mode)

**Key Insights:**
- Discounts >20% correlate strongly with **negative profit margins**
- Specific category–region combinations show **persistent profit leakage**
- Anomalous sales months coincide with major campaign periods

---

## Performance Optimization

### Performance Recording

Tableau's built-in Performance Recording was used to profile dashboard rendering:
1. Started recording via `Help → Settings and Performance`
2. Interacted with all three dashboards
3. Reviewed the auto-generated performance workbook analyzing rendering, query execution, and layout computation times

### Context Filters

Context filters execute **before** all other filters, creating a temporary materialized table that speeds up subsequent filtering.

> Applied context filter on: `Year` (high-cardinality field)

### Filter Execution Hierarchy

| Filter Type | Execution Timing | Speed Impact |
|-------------|-----------------|--------------|
| Extract Filter | When extract is created | ⚡ Fastest |
| Data Source Filter | At query time | 🚀 Fast |
| Context Filter | Before other filters | ⚙️ Medium |
| Dimension Filter | Per visualization | 📊 Standard |

### Optimization Techniques Applied

| Technique | Benefit |
|-----------|---------|
| Extract connection (vs. Live) | Faster query execution |
| Context filters on Year field | Reduces data scope early |
| Integer surrogate keys | Faster joins vs. string comparisons |
| Discrete dates where possible | Reduces cardinality |
| FIXED LOD preference | Best LOD performance |

---

## Theoretical Concepts

### Tableau Architecture (Multi-Tier)

```
┌─────────────────────────────────────────────────────┐
│               Presentation Layer                    │
│        Worksheets | Dashboards | Stories            │
├─────────────────────────────────────────────────────┤
│               Analytics Layer (VizQL)               │
│   Drag-and-drop → Optimized queries → Rendering     │
│   Handles: LOD expressions, Calculations, Pipeline  │
├─────────────────────────────────────────────────────┤
│                  Data Layer                         │
│   Live connections | .hyper Extracts | Hyper Engine │
└─────────────────────────────────────────────────────┘
```

### Hyper Engine vs. TDE

| Feature | Hyper Engine |
|---------|-------------|
| Storage | Columnar |
| Querying | Multi-threaded, compiled to native machine code |
| Compression | Dictionary encoding, run-length encoding, bit packing |
| Extract speed | 3× faster than TDE |
| Query speed | 5–10× faster than TDE |
| File format | `.hyper` |
| Capacity | Supports billions of rows |

### Data Blending vs. Joining

| Feature | Joining | Data Blending |
|---------|---------|---------------|
| Scope | Within same data source | Across different sources |
| Granularity | Row-level detail preserved | Aggregated to common dimension |
| Performance | Faster (single query) | Slower (two queries) |
| When to use | Same source, need row detail | Different sources, different granularity |

> **Recommendation:** Use Relationships/Joins for same-source data. Use Blending only when sources are incompatible.

### Tableau vs. Power BI vs. Qlik Sense

| Feature | Tableau | Power BI | Qlik Sense |
|---------|---------|----------|------------|
| Vendor | Salesforce | Microsoft | Qlik |
| Core Strength | Best-in-class visualization | MS ecosystem integration | Associative data model |
| Query Language | VizQL, Calculated Fields | DAX, M (Power Query) | Set Analysis, Script |
| Cost | ~$70/user/mo | $10–20/user/mo | ~$30/user/mo |
| Visualization | Superior, most customizable | Good, less flexible | Good, smart viz |
| ETL | Basic (Tableau Prep) | Built-in Power Query | Built-in data manager |
| AI/ML | Explain Data, Clustering | AI Insights, AutoML | Insight Advisor |
| Best For | Complex dashboards & analysis | Budget BI, MS shops | Guided analytics |

---

## Business Insights Summary

| # | Area | Insight |
|---|------|---------|
| 1 | **Revenue Trend** | Steady growth from 2021–2025 with seasonal Q4 peaks driven by holiday demand |
| 2 | **Top Products** | Surface Laptop and Laptop Sleeve dominate revenue; accessories yield higher margins |
| 3 | **Customer Segments** | Corporate and Consumer lead revenue; Home Office is smallest but growing |
| 4 | **Geographic Performance** | West region leads in total sales; certain Central sub-regions show profit leakage |
| 5 | **Discount Impact** | Discounts exceeding **20%** consistently produce negative profit margins |
| 6 | **Cohort Retention** | Early cohorts (2021) show better long-term retention; all cohorts stabilize at **10–15%** after 12 months |
| 7 | **Anomalies** | Sales spikes align with major marketing campaigns; unexpected drops correlate with supply chain events |
| 8 | **RFM Distribution** | ~15% Champions, ~25% Loyal, ~20% At Risk, ~15% Hibernating |
| 9 | **Forecast** | Exponential smoothing predicts continued moderate growth with seasonal Q4 peaks |
| 10 | **Operational Risk** | High-discount Office Supplies orders in specific regions are the primary profit leakage drivers |

---

## Project Structure

```
Tableau-BI-Project/
│
├── data/
│   ├── sales_fact_clean.xlsx
│   ├── customer_dimension_scd2.xlsx
│   ├── product_dimension_clean.xlsx
│   ├── geography_dimension_clean.xlsx
│   ├── time_dimension_clean.xlsx
│   ├── marketing_dimension_clean.xlsx
│   └── customer_transactions_clean.xlsx
│
├── scripts/
│   └── data_engineering.py          # Deduplication, SCD2, surrogate keys
│
├── tableau/
│   └── Tableau_BI_Project.twbx      # Packaged Tableau workbook
│
└── README.md
```

---
