# Databricks notebook source
# MAGIC %md
# MAGIC # Main Pipeline
# MAGIC **The only notebook you run.**
# MAGIC
# MAGIC Loads all supporting notebooks, runs the pipeline through `main()`,
# MAGIC then writes every table to the database in the Write Data section below.
# MAGIC
# MAGIC ```
# MAGIC main_pipeline
# MAGIC     ├── 00_config                 constants
# MAGIC     ├── etl_utils                 generic helpers
# MAGIC     ├── 01_schema                 table DDLs
# MAGIC     ├── 02_read_data              raw DataFrames
# MAGIC     ├── 03_data_cleaning          run_cleaning()
# MAGIC     ├── 04_data_transformations   run_transforms()
# MAGIC     ├── 05_dim_transforms         build_dimensions()
# MAGIC     └── 06_fact_transforms        build_facts()
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Load Supporting Notebooks

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./etl_utils

# COMMAND ----------

# MAGIC %run ./01_schema

# COMMAND ----------

# MAGIC %run ./02_read_data

# COMMAND ----------

# MAGIC %run ./03_data_cleaning

# COMMAND ----------

# MAGIC %run ./04_data_transformations

# COMMAND ----------

# MAGIC %run ./05_dim_transforms

# COMMAND ----------

# MAGIC %run ./06_fact_transforms

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Main Function

# COMMAND ----------

def main():
    """
    Runs the full pipeline from raw data to a clean dimensional model.

    One call per stage. The details of what happens inside each stage
    live in the dedicated notebooks where they belong.
    """
    run_id = get_run_id()
    run_ts = get_run_timestamp()

    print(f"Starting {PIPELINE_NAME} v{PIPELINE_VERSION}")
    print(f"Run ID : {run_id}")

    section("Step 1. Clean and validate the source data")
    ingested = run_cleaning(run_ts)

    section("Step 2. Apply types and business rules")
    cleaned = run_transforms(ingested, run_ts)

    section("Step 3. Build the dimensional model")
    dimensions = build_dimensions(cleaned, run_ts)
    facts      = build_facts(cleaned, dimensions, run_ts)

    return ingested, cleaned, dimensions, facts, run_id, run_ts

# COMMAND ----------

ingested, cleaned, dimensions, facts, run_id, run_ts = main()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Analytics . 5 Business Metrics
# MAGIC
# MAGIC All queries run against the final tables in the database.
# MAGIC Table paths come from `00_config`. Update `CATALOG` once and every query follows.

# COMMAND ----------

# MAGIC %md ### 1. Daily Active Users by Region
# MAGIC How many unique customers placed an order each day, broken down by region.

# COMMAND ----------

df_metric_1 = spark.sql(f"""
    SELECT
        d.full_date                      AS order_date,
        c.region,
        COUNT(DISTINCT f.cust_key)       AS active_users
    FROM {TBL_GOLD_FACT_ORDERS}   f
    JOIN {TBL_GOLD_DIM_DATE}      d ON f.date_key = d.date_key
    JOIN {TBL_GOLD_DIM_CUSTOMER}  c ON f.cust_key = c.cust_key
    GROUP BY d.full_date, c.region
    ORDER BY d.full_date, c.region
""")
display(df_metric_1)

# COMMAND ----------

# MAGIC %md ### 2. Revenue, Order Count & Quantity for the Sweet Category

# COMMAND ----------

df_metric_2 = spark.sql(f"""
    SELECT
        d.year,
        d.month_name,
        p.top_category,
        COUNT(DISTINCT f.order_id)       AS order_count,
        SUM(f.quantity)                  AS total_quantity,
        ROUND(SUM(f.revenue_usd), 2)     AS total_revenue_usd
    FROM {TBL_GOLD_FACT_ORDERS}   f
    JOIN {TBL_GOLD_DIM_DATE}      d ON f.date_key  = d.date_key
    JOIN {TBL_GOLD_DIM_PRODUCT}   p ON f.prod_key  = p.prod_key
    WHERE LOWER(p.top_category) = 'sweet'
    GROUP BY d.year, d.month_name, d.month, p.top_category
    ORDER BY d.year, d.month
""")
display(df_metric_2)

# COMMAND ----------

# MAGIC %md ### 3. Top 3 Products by Revenue per Region

# COMMAND ----------

df_metric_3 = spark.sql(f"""
    WITH ranked AS (
        SELECT
            c.region,
            p.prod_name,
            p.top_category,
            ROUND(SUM(f.revenue_usd), 2) AS total_revenue_usd,
            RANK() OVER (
                PARTITION BY c.region
                ORDER BY SUM(f.revenue_usd) DESC
            )                            AS revenue_rank
        FROM {TBL_GOLD_FACT_ORDERS}   f
        JOIN {TBL_GOLD_DIM_CUSTOMER}  c ON f.cust_key = c.cust_key
        JOIN {TBL_GOLD_DIM_PRODUCT}   p ON f.prod_key = p.prod_key
        GROUP BY c.region, p.prod_name, p.top_category
    )
    SELECT region, revenue_rank, prod_name, top_category, total_revenue_usd
    FROM   ranked
    WHERE  revenue_rank <= 3
    ORDER BY region, revenue_rank
""")
display(df_metric_3)

# COMMAND ----------

# MAGIC %md ### 4. Customer Lifetime Value  (active vs inactive)

# COMMAND ----------

df_metric_4 = spark.sql(f"""
    SELECT
        c.cust_key,
        c.region,
        c.customer_type,
        CASE WHEN c.is_active
             THEN 'active'
             ELSE 'inactive'
        END                              AS customer_status,
        COUNT(DISTINCT f.order_id)       AS total_orders,
        SUM(f.quantity)                  AS total_units,
        ROUND(SUM(f.revenue_usd), 2)     AS lifetime_revenue_usd,
        ROUND(AVG(f.revenue_usd), 2)     AS avg_order_line_value_usd
    FROM {TBL_GOLD_DIM_CUSTOMER}     c
    LEFT JOIN {TBL_GOLD_FACT_ORDERS} f ON c.cust_key = f.cust_key
    GROUP BY c.cust_key, c.region, c.customer_type, c.is_active
    ORDER BY lifetime_revenue_usd DESC
""")
display(df_metric_4)

# COMMAND ----------

# MAGIC %md ### 5. Duplicate Orders & Faulty Transactions Audit

# COMMAND ----------

df_metric_5 = spark.sql(f"""
    SELECT
        'multi_line_order'               AS flag_type,
        order_id,
        COUNT(*)                         AS line_count,
        COLLECT_SET(prod_key)            AS prod_keys,
        ROUND(SUM(revenue_usd), 2)       AS total_revenue_usd,
        NULL                             AS error_reason
    FROM {TBL_GOLD_FACT_ORDERS}
    GROUP BY order_id
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        _error_code                      AS flag_type,
        order_id,
        1                                AS line_count,
        NULL                             AS prod_keys,
        NULL                             AS total_revenue_usd,
        _error_message                   AS error_reason
    FROM {TBL_ERR_ORDERS}

    ORDER BY flag_type, order_id
""")
display(df_metric_5)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Write Data
# MAGIC Write every clean DataFrame to its Delta table in the database.
# MAGIC Runs in layer order. Bronze first, errors last.

# COMMAND ----------

section("Writing all tables to the database")

# ── Bronze (raw ingested) ──────────────────────────────────────────────────────

write_delta(ingested["customers"],   TBL_BRONZE_CUSTOMER)
write_delta(ingested["orders"],      TBL_BRONZE_ORDERS)
write_delta(ingested["products"],    TBL_BRONZE_PRODUCT)
write_delta(ingested["categories"],  TBL_BRONZE_CAT_TREE)
write_delta(ingested["fx_rates"],    TBL_BRONZE_FX)

# ── Silver (typed & enriched) ─────────────────────────────────────────────────

write_delta(cleaned["customers"],   TBL_SILVER_CUSTOMER)
write_delta(cleaned["orders"],      TBL_SILVER_ORDERS)
write_delta(cleaned["products"],    TBL_SILVER_PRODUCT)
write_delta(cleaned["fx_rates"],    TBL_SILVER_FX)
write_delta(cleaned["order_items"], TBL_SILVER_ORDER_ITEMS)

# ── Gold (dimensional model) ───────────────────────────────────────────────────

write_delta(dimensions["dim_customer"], TBL_GOLD_DIM_CUSTOMER)
write_delta(dimensions["dim_product"],  TBL_GOLD_DIM_PRODUCT)
write_delta(dimensions["dim_date"],     TBL_GOLD_DIM_DATE)
write_delta(facts["fact_orders"],       TBL_GOLD_FACT_ORDERS)

# ── Quarantined rows ───────────────────────────────────────────────────────────

write_errors(
    cleaned["err_no_customer"], TBL_ERR_ORDERS,
    "NULL_CUST_ID",
    "No customer ID. Order cannot be linked to any customer, "
    "region, or segment. Excluded from all business metrics.",
    SRC_ORDERS, run_ts,
    mode="overwrite")  # overwrite clears stale rows from previous runs

write_errors(
    cleaned["err_bad_qty"], TBL_ERR_ORDERS,
    "INVALID_QTY",
    "Quantity is fractional (0.1). Rounding up or down would "
    "fabricate revenue. Excluded from all revenue metrics.",
    SRC_ORDERS, run_ts,
    mode="append")  # append so NULL_CUST_ID rows written above are kept

# COMMAND ----------

# MAGIC %md ## Run Summary

# COMMAND ----------

run_summary({
    "Bronze": [
        (TBL_BRONZE_CUSTOMER, "customers"),
        (TBL_BRONZE_ORDERS,   "orders"),
        (TBL_BRONZE_PRODUCT,  "products"),
        (TBL_BRONZE_CAT_TREE, "categories"),
        (TBL_BRONZE_FX,       "fx rates"),
    ],
    "Silver": [
        (TBL_SILVER_CUSTOMER,    "customers"),
        (TBL_SILVER_ORDERS,      "orders"),
        (TBL_SILVER_PRODUCT,     "products"),
        (TBL_SILVER_FX,          "fx rates"),
        (TBL_SILVER_ORDER_ITEMS, "order items"),
    ],
    "Gold": [
        (TBL_GOLD_DIM_CUSTOMER, "customer dimension"),
        (TBL_GOLD_DIM_PRODUCT,  "product dimension"),
        (TBL_GOLD_DIM_DATE,     "date dimension"),
        (TBL_GOLD_FACT_ORDERS,  "orders fact"),
    ],
    "Error": [
        (TBL_ERR_ORDERS, "quarantined orders"),
    ],
    "Audit": [
        (TBL_AUDIT_LOG, "pipeline run log"),
    ],
})