# Databricks notebook source
# MAGIC %md
# MAGIC # Config
# MAGIC Pure constants only. No functions, no imports, no Spark operations.
# MAGIC Every path, name, and identifier used across the pipeline lives here.
# MAGIC Update one value → it flows everywhere automatically.
# MAGIC
# MAGIC > Called via `%run` from `main_pipeline`. Never run directly.

# COMMAND ----------

# ── Catalog & Volume ───────────────────────────────────────────────────────────
CATALOG     = "workspace"
VOLUME_PATH = "/Volumes/workspace/default/ecommerce_assignment"

# ── Schema names ───────────────────────────────────────────────────────────────
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"
SCHEMA_ERROR  = "error_tables"
SCHEMA_AUDIT  = "audit"

# ── Source file paths ──────────────────────────────────────────────────────────
SRC_CUSTOMER = f"{VOLUME_PATH}/customer.csv"
SRC_ORDERS   = f"{VOLUME_PATH}/orders.csv"
SRC_PRODUCT  = f"{VOLUME_PATH}/product.csv"
SRC_CAT_TREE = f"{VOLUME_PATH}/prod_cat_tree.csv"
SRC_FX       = f"{VOLUME_PATH}/currency_conversion.json"

# ── Bronze table names ─────────────────────────────────────────────────────────
TBL_BRONZE_CUSTOMER = f"{CATALOG}.{SCHEMA_BRONZE}.customer"
TBL_BRONZE_ORDERS   = f"{CATALOG}.{SCHEMA_BRONZE}.orders"
TBL_BRONZE_PRODUCT  = f"{CATALOG}.{SCHEMA_BRONZE}.product"
TBL_BRONZE_CAT_TREE = f"{CATALOG}.{SCHEMA_BRONZE}.cat_tree"
TBL_BRONZE_FX       = f"{CATALOG}.{SCHEMA_BRONZE}.fx_rates"

# ── Silver table names ─────────────────────────────────────────────────────────
TBL_SILVER_CUSTOMER    = f"{CATALOG}.{SCHEMA_SILVER}.customer"
TBL_SILVER_ORDERS      = f"{CATALOG}.{SCHEMA_SILVER}.orders"
TBL_SILVER_PRODUCT     = f"{CATALOG}.{SCHEMA_SILVER}.product"
TBL_SILVER_FX          = f"{CATALOG}.{SCHEMA_SILVER}.fx_rates"
TBL_SILVER_ORDER_ITEMS = f"{CATALOG}.{SCHEMA_SILVER}.order_items"

# ── Gold table names ───────────────────────────────────────────────────────────
TBL_GOLD_DIM_CUSTOMER = f"{CATALOG}.{SCHEMA_GOLD}.dim_customer"
TBL_GOLD_DIM_PRODUCT  = f"{CATALOG}.{SCHEMA_GOLD}.dim_product"
TBL_GOLD_DIM_DATE     = f"{CATALOG}.{SCHEMA_GOLD}.dim_date"
TBL_GOLD_FACT_ORDERS  = f"{CATALOG}.{SCHEMA_GOLD}.fact_orders"

# ── Error & Audit table names ──────────────────────────────────────────────────
TBL_ERR_ORDERS = f"{CATALOG}.{SCHEMA_ERROR}.err_orders"
TBL_AUDIT_LOG  = f"{CATALOG}.{SCHEMA_AUDIT}.pipeline_run_log"

# ── Pipeline metadata ──────────────────────────────────────────────────────────
PIPELINE_NAME    = "ecommerce_etl"
PIPELINE_VERSION = "1.0.0"

# COMMAND ----------

print("Config loaded successfully")
print(f"  Catalog  : {CATALOG}")
print(f"  Volume   : {VOLUME_PATH}")
print(f"  Pipeline : {PIPELINE_NAME} v{PIPELINE_VERSION}")
