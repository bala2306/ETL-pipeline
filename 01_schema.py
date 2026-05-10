# Databricks notebook source
# MAGIC %md
# MAGIC # Schema
# MAGIC Creates all schemas and the Bronze Delta table DDLs.
# MAGIC Bronze columns are explicitly typed. No `inferSchema` anywhere.
# MAGIC Silver and Gold schemas emerge directly from the transformation code
# MAGIC and are created automatically on first write.
# MAGIC
# MAGIC Safe to re-run at any time (`CREATE TABLE IF NOT EXISTS` throughout).
# MAGIC
# MAGIC > Called via `%run` from `main_pipeline`. Never run directly.

# COMMAND ----------
# MAGIC %md ## Schemas

# COMMAND ----------

for schema in [SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD, SCHEMA_ERROR, SCHEMA_AUDIT]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"  Schema ready: {CATALOG}.{schema}")

# COMMAND ----------
# MAGIC %md ## Bronze (Raw Ingest)
# MAGIC All columns are STRING here. Bronze mirrors the source exactly.
# MAGIC Type casting happens in Silver, not here.

# COMMAND ----------

# Customer. Raw snapshot rows from customer.csv
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_BRONZE_CUSTOMER} (
        cust_id       STRING   COMMENT 'Customer identifier (raw, as in source)',
        created_at    STRING   COMMENT 'Customer creation date (raw string M/D/YYYY)',
        country       STRING   COMMENT 'Country code',
        region        STRING   COMMENT 'Geographic region',
        type          STRING   COMMENT 'Customer segment type',
        zip           STRING   COMMENT 'Postal zip code (kept as string)',
        active        STRING   COMMENT 'Active flag from source: y or n',
        _source_file  STRING   COMMENT 'Full path of the source file this row came from',
        _ingested_at  TIMESTAMP COMMENT 'Timestamp when this row was written to Bronze'
    ) USING DELTA
    COMMENT 'Raw customer data. One row per latest snapshot per customer after dedup'
""")

# Orders. Raw order rows from orders.csv
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_BRONZE_ORDERS} (
        order_id       STRING   COMMENT 'Order identifier',
        cust_id        STRING   COMMENT 'Customer identifier (raw)',
        order_date_raw STRING   COMMENT 'Order date and time as raw string',
        prod_id        STRING   COMMENT 'Product identifier (raw)',
        quantity_raw   STRING   COMMENT 'Ordered quantity (raw string)',
        status         STRING   COMMENT 'Order status: shipped, paid, created, cancelled',
        _source_file   STRING   COMMENT 'Source file path',
        _ingested_at   TIMESTAMP COMMENT 'Timestamp when this row was written to Bronze'
    ) USING DELTA
    COMMENT 'Raw order data. Exact duplicates and unrecoverable rows removed'
""")

# Product. Raw product rows from product.csv
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_BRONZE_PRODUCT} (
        prod_id        STRING   COMMENT 'Product identifier (raw)',
        cat_id         STRING   COMMENT 'Category identifier (raw)',
        prod_name      STRING   COMMENT 'Product name',
        price          STRING   COMMENT 'Product price (raw string)',
        currency       STRING   COMMENT 'Price currency as found in source (may be mixed case)',
        _currency_note STRING   COMMENT 'Flag if currency casing is non-standard',
        _source_file   STRING   COMMENT 'Source file path',
        _ingested_at   TIMESTAMP COMMENT 'Timestamp when this row was written to Bronze'
    ) USING DELTA
    COMMENT 'Raw product data. All 14 products loaded as-is'
""")

# Product category tree. Raw hierarchy from prod_cat_tree.csv
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_BRONZE_CAT_TREE} (
        cat_id       STRING   COMMENT 'Category identifier',
        child        STRING   COMMENT 'Category name at this level',
        parent       STRING   COMMENT 'Parent category name (all = root)',
        _source_file STRING   COMMENT 'Source file path',
        _ingested_at TIMESTAMP COMMENT 'Timestamp when this row was written to Bronze'
    ) USING DELTA
    COMMENT 'Raw product category hierarchy . 5 categories, 2 levels deep'
""")

# FX rates. Raw currency conversion rates from currency_conversion.json
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_BRONZE_FX} (
        date         STRING   COMMENT 'Rate date as raw string (yyyy-MM-dd)',
        currency     STRING   COMMENT 'Currency code: USD, YEN, POUND',
        conversion   DOUBLE   COMMENT 'Conversion rate to USD',
        _source_file STRING   COMMENT 'Source file path',
        _ingested_at TIMESTAMP COMMENT 'Timestamp when this row was written to Bronze'
    ) USING DELTA
    COMMENT 'Raw FX conversion rates. Duplicate date entries removed, last entry kept'
""")

print("Bronze tables ready")

# COMMAND ----------
# MAGIC %md ## Error & Audit

# COMMAND ----------

# Error table: unrecoverable order rows quarantined during Bronze
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_ERR_ORDERS} (
        order_id       STRING    COMMENT 'Order identifier',
        cust_id        STRING    COMMENT 'Customer identifier (may be null)',
        order_date_raw STRING    COMMENT 'Raw order date string',
        prod_id        STRING    COMMENT 'Product identifier',
        quantity_raw   STRING    COMMENT 'Raw quantity string',
        status         STRING    COMMENT 'Order status',
        _error_code    STRING    COMMENT 'Error code: NULL_CUST_ID or INVALID_QTY',
        _error_message STRING    COMMENT 'Human readable explanation of why this row was rejected',
        _source_file   STRING    COMMENT 'Source file this row came from',
        _ingested_at   TIMESTAMP COMMENT 'Timestamp when this row was quarantined'
    ) USING DELTA
    COMMENT 'Quarantine table. Rows that cannot be recovered without fabricating data'
""")

# Audit log: one record per table write per pipeline run
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_AUDIT_LOG} (
        run_id     STRING  COMMENT 'Unique identifier for this pipeline run',
        pipeline   STRING  COMMENT 'Pipeline name',
        version    STRING  COMMENT 'Pipeline version',
        layer      STRING  COMMENT 'BRONZE, SILVER, GOLD, or ERROR',
        table_name STRING  COMMENT 'Full table name written',
        row_count  LONG    COMMENT 'Number of rows written',
        status     STRING  COMMENT 'SUCCESS or FAILED',
        notes      STRING  COMMENT 'Optional notes about this write',
        logged_at  STRING  COMMENT 'UTC timestamp of this audit record'
    ) USING DELTA
    COMMENT 'Pipeline run audit log. History of every table write across every run'
""")

print("Error and audit tables ready")

# COMMAND ----------

print()
print("All schemas and tables created successfully")
