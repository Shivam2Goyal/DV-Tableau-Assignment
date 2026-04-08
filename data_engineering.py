"""
PHASE 2: Data Engineering Pipeline
====================================
This script performs all data engineering tasks required by the assignment:

  1. DEDUPLICATION — Remove duplicate transaction records from Sales_Fact
  2. MISSING VALUE HANDLING — Fill/flag nulls in all tables
  3. SCD TYPE 2 SIMULATION — Slowly Changing Dimension on Customer_Dimension
  4. SURROGATE KEY CREATION — Integer surrogate keys for all dimension tables
  5. DATA QUALITY REPORT — Automated before/after quality summary

Input:  datasets/*.xlsx  (raw data from Phase 1)
Output: processed_data/*.xlsx  (cleaned, enriched data)
        processed_data/data_quality_report.txt
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

random.seed(42)
np.random.seed(42)

os.makedirs("processed_data", exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════════
# LOAD RAW DATA
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("PHASE 2: DATA ENGINEERING PIPELINE")
print("=" * 70)

print("\n[1/6] Loading raw datasets...")
sales_raw = pd.read_excel("datasets/sales_fact.xlsx")
customers_raw = pd.read_excel("datasets/customer_dimension.xlsx")
products_raw = pd.read_excel("datasets/product_dimension.xlsx")
geography_raw = pd.read_excel("datasets/geography_dimension.xlsx")
time_raw = pd.read_excel("datasets/time_dimension.xlsx")
campaigns_raw = pd.read_excel("datasets/marketing_dimension.xlsx")
cust_txn_raw = pd.read_excel("datasets/customer_transactions.xlsx")

report_lines = []  # Collect lines for the data quality report


def log(msg):
    """Print and save to report."""
    print(msg)
    report_lines.append(msg)


log(f"  Sales_Fact:        {len(sales_raw)} rows")
log(f"  Customer_Dim:      {len(customers_raw)} rows")
log(f"  Product_Dim:       {len(products_raw)} rows")
log(f"  Geography_Dim:     {len(geography_raw)} rows")
log(f"  Time_Dim:          {len(time_raw)} rows")
log(f"  Campaign_Dim:      {len(campaigns_raw)} rows")
log(f"  Customer_Txn:      {len(cust_txn_raw)} rows")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: DEDUPLICATION
# ══════════════════════════════════════════════════════════════════════════════
# WHY: Real-world transactional data often contains duplicate records caused by
#      system retries, ETL failures, or double-submissions. Removing them ensures
#      accurate aggregation (sum of sales, order counts, etc.).

log("\n" + "─" * 70)
log("[2/6] DEDUPLICATION — Removing duplicate transactions")
log("─" * 70)

# Count duplicates BEFORE removal
dupes_before = sales_raw.duplicated().sum()
log(f"  Duplicates found:  {dupes_before}")

# Strategy: Keep the FIRST occurrence, remove subsequent duplicates.
# We check ALL columns (a true duplicate has identical values in every field).
sales_deduped = sales_raw.drop_duplicates(keep="first").reset_index(drop=True)

log(f"  Rows before:       {len(sales_raw)}")
log(f"  Rows after:        {len(sales_deduped)}")
log(f"  Rows removed:      {len(sales_raw) - len(sales_deduped)}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: MISSING VALUE HANDLING
# ══════════════════════════════════════════════════════════════════════════════
# WHY: Missing values can break calculations (averages, sums) and cause Tableau
#      to exclude rows from visualizations. We need a strategy for each column.

log("\n" + "─" * 70)
log("[3/6] MISSING VALUE HANDLING")
log("─" * 70)

# --- Sales Fact ---
log("\n  Sales_Fact:")
null_discount = sales_deduped["Discount"].isna().sum()
null_shipmode = sales_deduped["Ship_Mode"].isna().sum()
log(f"    Null Discount:   {null_discount} → Fill with 0 (no discount given)")
log(f"    Null Ship_Mode:  {null_shipmode} → Fill with 'Standard Class' (most common)")

sales_clean = sales_deduped.copy()
sales_clean["Discount"] = sales_clean["Discount"].fillna(0)
sales_clean["Ship_Mode"] = sales_clean["Ship_Mode"].fillna("Standard Class")

# --- Customer Dimension ---
log("\n  Customer_Dimension:")
null_segment = customers_raw["Segment"].isna().sum()
null_email = customers_raw["Email"].isna().sum()
log(f"    Null Segment:    {null_segment} → Fill with 'Unknown'")
log(f"    Null Email:      {null_email} → Fill with 'not_provided@unknown.com'")

customers_clean = customers_raw.copy()
customers_clean["Segment"] = customers_clean["Segment"].fillna("Unknown")
customers_clean["Email"] = customers_clean["Email"].fillna("not_provided@unknown.com")

# --- Product Dimension ---
log("\n  Product_Dimension:")
null_subcat = products_raw["Sub_Category"].isna().sum()
log(f"    Null Sub_Category: {null_subcat} → Fill with 'Uncategorized'")

products_clean = products_raw.copy()
products_clean["Sub_Category"] = products_clean["Sub_Category"].fillna("Uncategorized")

# Verify no remaining nulls in critical columns
log("\n  Post-cleaning null check:")
log(f"    Sales.Discount nulls:     {sales_clean['Discount'].isna().sum()}")
log(f"    Sales.Ship_Mode nulls:    {sales_clean['Ship_Mode'].isna().sum()}")
log(f"    Customer.Segment nulls:   {customers_clean['Segment'].isna().sum()}")
log(f"    Product.Sub_Cat nulls:    {products_clean['Sub_Category'].isna().sum()}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: SCD TYPE 2 SIMULATION
# ══════════════════════════════════════════════════════════════════════════════
# WHY: In a real data warehouse, customer attributes change over time (e.g., a
#      customer moves from "Consumer" to "Corporate" segment). SCD Type 2 keeps
#      a HISTORY of changes by adding new rows with effective dates, rather than
#      overwriting the old value. This lets analysts see what segment a customer
#      was in at the time of each order.
#
# HOW IT WORKS:
#   - Original row gets an End_Date (when the change happened) and Is_Current = False
#   - A new row is inserted with the NEW value, Start_Date = change date, Is_Current = True
#   - Each row gets a unique Surrogate_Key (SK) so the fact table can point to the
#     specific version of the customer dimension that was active at order time.

log("\n" + "─" * 70)
log("[4/6] SCD TYPE 2 — Slowly Changing Dimension Simulation")
log("─" * 70)

# Add SCD columns to customer dimension
customers_scd = customers_clean.copy()
customers_scd["Effective_Start_Date"] = customers_scd["Signup_Date"]
customers_scd["Effective_End_Date"] = pd.Timestamp("9999-12-31")  # Still current
customers_scd["Is_Current"] = True
customers_scd["Version"] = 1

# Simulate segment changes for ~8% of customers
# This mimics real scenarios: consumer upgrades to corporate, home office shifts, etc.
n_changes = int(len(customers_scd) * 0.08)
change_indices = np.random.choice(len(customers_scd), size=n_changes, replace=False)

segment_transitions = {
    "Consumer": "Corporate",
    "Corporate": "Small Business",
    "Home Office": "Consumer",
    "Small Business": "Corporate",
    "Unknown": "Consumer",
}

new_rows = []
for idx in change_indices:
    row = customers_scd.iloc[idx].copy()
    old_segment = row["Segment"]
    new_segment = segment_transitions.get(old_segment, "Corporate")

    # The change happened at a random date between signup and 2025-12-31
    signup = pd.Timestamp(row["Signup_Date"])
    change_date = signup + timedelta(days=random.randint(180, 1200))
    if change_date > pd.Timestamp("2025-12-31"):
        change_date = pd.Timestamp("2025-06-01")

    # Close the old record
    customers_scd.at[customers_scd.index[idx], "Effective_End_Date"] = change_date
    customers_scd.at[customers_scd.index[idx], "Is_Current"] = False

    # Create new version
    new_row = row.copy()
    new_row["Segment"] = new_segment
    new_row["Effective_Start_Date"] = change_date
    new_row["Effective_End_Date"] = pd.Timestamp("9999-12-31")
    new_row["Is_Current"] = True
    new_row["Version"] = 2
    new_rows.append(new_row)

customers_scd = pd.concat([customers_scd, pd.DataFrame(new_rows)], ignore_index=True)
customers_scd = customers_scd.sort_values(["Customer_ID", "Version"]).reset_index(
    drop=True
)

log(f"  Customers with segment changes: {n_changes}")
log(f"  Total SCD rows (incl. history): {len(customers_scd)}")
log(f"  Current records:                {customers_scd['Is_Current'].sum()}")
log(f"  Historical records:             {(~customers_scd['Is_Current']).sum()}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4: SURROGATE KEY CREATION
# ══════════════════════════════════════════════════════════════════════════════
# WHY: Business keys (like "CUST-0001") come from source systems and can change
#      or collide across systems. Surrogate keys are database-generated integers
#      that uniquely identify each row — they're faster for joins, smaller in
#      storage, and essential for SCD Type 2 (where one customer can have
#      multiple rows with the same business key).

log("\n" + "─" * 70)
log("[5/6] SURROGATE KEY CREATION")
log("─" * 70)

# Customer Dimension (SCD version — each row gets unique SK)
customers_scd.insert(0, "Customer_SK", range(1, len(customers_scd) + 1))
log(f"  Customer_SK: 1 to {len(customers_scd)} (one per SCD row)")

# Product Dimension
products_clean.insert(0, "Product_SK", range(1, len(products_clean) + 1))
log(f"  Product_SK:  1 to {len(products_clean)}")

# Geography Dimension (already has Geography_ID, add SK)
geography_clean = geography_raw.copy()
geography_clean.insert(0, "Geography_SK", range(1, len(geography_clean) + 1))
log(f"  Geography_SK: 1 to {len(geography_clean)}")

# Time Dimension (already has Time_ID, add SK)
time_clean = time_raw.copy()
time_clean.insert(0, "Time_SK", range(1, len(time_clean) + 1))
log(f"  Time_SK:     1 to {len(time_clean)}")

# Campaign Dimension
campaigns_clean = campaigns_raw.copy()
campaigns_clean.insert(0, "Campaign_SK", range(1, len(campaigns_clean) + 1))
log(f"  Campaign_SK: 1 to {len(campaigns_clean)}")

# Update fact table — add surrogate key lookups
# For Customer_SK, use the CURRENT version of each customer
current_cust_map = customers_scd[customers_scd["Is_Current"] == True][
    ["Customer_ID", "Customer_SK"]
]
current_cust_map = current_cust_map.drop_duplicates("Customer_ID")

sales_clean = sales_clean.merge(current_cust_map, on="Customer_ID", how="left")
sales_clean = sales_clean.merge(
    products_clean[["Product_ID", "Product_SK"]], on="Product_ID", how="left"
)
sales_clean = sales_clean.merge(
    geography_clean[["Geography_ID", "Geography_SK"]], on="Geography_ID", how="left"
)
sales_clean = sales_clean.merge(
    campaigns_clean[["Campaign_ID", "Campaign_SK"]], on="Campaign_ID", how="left"
)

log(
    f"\n  Sales_Fact now includes SK columns: Customer_SK, Product_SK, Geography_SK, Campaign_SK"
)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5: REBUILD CUSTOMER TRANSACTIONS (from cleaned data)
# ══════════════════════════════════════════════════════════════════════════════

log("\n" + "─" * 70)
log("[6/6] REBUILDING CUSTOMER TRANSACTIONS from cleaned Sales data")
log("─" * 70)

cust_txn_clean = (
    sales_clean.groupby("Customer_ID")
    .agg(
        Total_Orders=("Order_ID", "nunique"),
        Total_Quantity=("Quantity", "sum"),
        Total_Sales=("Sales", "sum"),
        Total_Profit=("Profit", "sum"),
        Avg_Discount=("Discount", "mean"),
        First_Order_Date=("Order_Date", "min"),
        Last_Order_Date=("Order_Date", "max"),
    )
    .reset_index()
)

cust_txn_clean["Avg_Order_Value"] = round(
    cust_txn_clean["Total_Sales"] / cust_txn_clean["Total_Orders"], 2
)
cust_txn_clean["Total_Sales"] = cust_txn_clean["Total_Sales"].round(2)
cust_txn_clean["Total_Profit"] = cust_txn_clean["Total_Profit"].round(2)
cust_txn_clean["Avg_Discount"] = cust_txn_clean["Avg_Discount"].round(4)

# Add Customer_SK
cust_txn_clean = cust_txn_clean.merge(current_cust_map, on="Customer_ID", how="left")

log(f"  Customer Transactions rebuilt: {len(cust_txn_clean)} customers")

# ══════════════════════════════════════════════════════════════════════════════
# EXPORT PROCESSED DATA
# ══════════════════════════════════════════════════════════════════════════════

log("\n" + "=" * 70)
log("EXPORTING PROCESSED DATA")
log("=" * 70)

exports = {
    "processed_data/sales_fact_clean.xlsx": sales_clean,
    "processed_data/customer_dimension_scd2.xlsx": customers_scd,
    "processed_data/product_dimension_clean.xlsx": products_clean,
    "processed_data/geography_dimension_clean.xlsx": geography_clean,
    "processed_data/time_dimension_clean.xlsx": time_clean,
    "processed_data/marketing_dimension_clean.xlsx": campaigns_clean,
    "processed_data/customer_transactions_clean.xlsx": cust_txn_clean,
}

for path, df in exports.items():
    df.to_excel(path, index=False, engine="openpyxl")
    log(f"  ✓ {path}  ({len(df)} rows × {len(df.columns)} cols)")

# ══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY REPORT
# ══════════════════════════════════════════════════════════════════════════════

log("\n" + "=" * 70)
log("DATA QUALITY SUMMARY")
log("=" * 70)

log(
    f"""
┌─────────────────────────────────────────────────────────────────┐
│  BEFORE vs AFTER Comparison                                     │
├─────────────────────────┬──────────────┬────────────────────────┤
│  Metric                 │  Before      │  After                 │
├─────────────────────────┼──────────────┼────────────────────────┤
│  Sales rows             │  {len(sales_raw):<12}│  {len(sales_clean):<22}│
│  Duplicate rows         │  {dupes_before:<12}│  0                     │
│  Null Discounts         │  {null_discount:<12}│  0                     │
│  Null Ship_Modes        │  {null_shipmode:<12}│  0                     │
│  Null Segments          │  {null_segment:<12}│  0                     │
│  Null Sub_Categories    │  {null_subcat:<12}│  0                     │
│  Null Emails            │  {null_email:<12}│  0                     │
│  Customer SCD rows      │  {len(customers_raw):<12}│  {len(customers_scd):<22}│
│  Surrogate keys added   │  No           │  Yes (all dimensions)  │
└─────────────────────────┴──────────────┴────────────────────────┘
"""
)

# Save report
report_path = "processed_data/data_quality_report.txt"
with open(report_path, "w", encoding="utf-8") as f:
    f.write("DATA ENGINEERING — QUALITY REPORT\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("=" * 70 + "\n\n")
    for line in report_lines:
        f.write(line + "\n")

log(f"\n  ✓ Report saved: {report_path}")
log("\nPHASE 2 COMPLETE ✓")
