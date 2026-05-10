# Databricks notebook source
# MAGIC %md
# MAGIC # Data Cleaning
# MAGIC Handles all data quality checks and validation on raw source files .
# MAGIC deciding what to keep, what to fix inline, and what to quarantine.
# MAGIC
# MAGIC The only function `main_pipeline` ever calls from here is `run_cleaning()`.
# MAGIC Everything else is an internal detail.
# MAGIC
# MAGIC > Called via `%run` from `main_pipeline`. Never run directly.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 1. Ingest
# MAGIC
# MAGIC Reads from the raw DataFrames loaded by `02_read_data`.
# MAGIC Decides what is safe to keep, what can be fixed inline,
# MAGIC and what is genuinely unrecoverable.
# MAGIC
# MAGIC **Why only 3 rows go to the error table:**
# MAGIC
# MAGIC | Row | Problem | Why we can't fix it |
# MAGIC |---|---|---|
# MAGIC | A-21 | No customer ID | Can't link to any customer. Revenue becomes unattributable |
# MAGIC | A-22 | No customer ID | Same reason |
# MAGIC | A-013 | Quantity is 0.1 | Rounding up or down fabricates revenue |
# MAGIC
# MAGIC Everything else has a clear deterministic fix. No guessing required.

# COMMAND ----------

def _ingest_customers():
    """
    The source has 3 snapshot rows per customer (12 rows for 4 customers),
    sometimes with conflicting active flags across snapshots.
    We keep the most recent snapshot. Latest created_at wins.
    """
    return dedup_by_window(
        df             = df_raw_customer,
        partition_cols = ["cust_id"],
        order_col      = F.to_date(F.trim(F.col("created_at")), "M/d/yyyy"),
        order_desc     = True)


def _ingest_orders():
    """
    Three things happen here:

    1. Rows with no customer ID go to the error table. They are useless
       for every business metric this pipeline produces.

    2. Rows with a fractional quantity go to the error table. We cannot
       round without knowing what the correct value should have been.

    3. Exact duplicate rows are collapsed to one copy. Note that orders
       with the same order_id but different products are NOT duplicates .
       those are valid multi-line orders and are kept as separate rows.

    Returns the clean orders plus the two error batches separately
    so the caller can quarantine them with the right error codes.
    """
    # Split: rows with no customer ID
    no_customer  = df_raw_orders.filter(
        F.col("cust_id").isNull() | (F.trim(F.col("cust_id")) == ""))
    has_customer = df_raw_orders.filter(
        F.col("cust_id").isNotNull() & (F.trim(F.col("cust_id")) != ""))

    # Split: rows with fractional quantity
    bad_qty  = has_customer.filter((F.col("quantity_raw").cast("double") % 1) != 0)
    good_qty = has_customer.filter((F.col("quantity_raw").cast("double") % 1) == 0)

    # Collapse exact duplicates. Partitioning on all columns ensures
    # multi-line orders (different prod_id) are never touched
    clean = dedup_by_window(
        df             = good_qty,
        partition_cols = good_qty.columns,
        order_col      = None)

    return clean, no_customer, bad_qty


def _ingest_products():
    """
    No rows need removing. The source uses 'Yen' (mixed case) while
    the FX table uses 'YEN'. We flag it here and fix it in transform.
    """
    return df_raw_product.withColumn(
        "_currency_note",
        F.when(F.col("currency") != F.upper(F.col("currency")),
               F.lit("Mixed case. Normalised to uppercase in transform stage"))
        .otherwise(F.lit(None).cast("string")))


def _ingest_categories():
    """No issues found. Pass through as-is."""
    return df_raw_cat_tree


def _ingest_fx_rates():
    """
    The source has two entries for 2020-01-28 per currency with different rates.
    We keep the last one. Last row in the file is the most recent write.
    """
    return dedup_by_window(
        df             = df_raw_fx,
        partition_cols = ["date", "currency"],
        order_col      = F.monotonically_increasing_id(),
        order_desc     = True)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Entry Point
# MAGIC
# MAGIC `run_cleaning()` is the only function `main_pipeline` calls from this notebook.
# MAGIC It runs the ingest stage and returns raw DataFrames ready for type casting.
# MAGIC Type casting and business rules are applied in `04_data_transformations`.

# COMMAND ----------

def run_cleaning(run_ts):
    """
    Runs all ingest-stage data quality checks in one call.

    Handles: deduplication, null checks, error splits.
    Does NOT cast types or apply business rules. That is run_transforms().

    Returns a dict with raw-typed DataFrames and error batches,
    keyed by simple human-readable names.
    """

    # ── Stage 1: Ingest ───────────────────────────────────────────────────────
    print("  Reading and validating source data...")

    customers  = _ingest_customers()
    orders, err_no_customer, err_bad_qty = _ingest_orders()
    products   = _ingest_products()
    categories = _ingest_categories()
    fx_rates   = _ingest_fx_rates()

    print()
    print("  Ingest complete")
    print(f"    customers   : {customers.count()} rows")
    print(f"    orders      : {orders.count()} rows  "
          f"({err_no_customer.count() + err_bad_qty.count()} quarantined)")
    print(f"    products    : {products.count()} rows")
    print(f"    fx rates    : {fx_rates.count()} rows")

    return {
        # Ingested data. Passed to run_transforms() for type casting
        "customers"   : customers,
        "orders"      : orders,
        "products"    : products,
        "categories"  : categories,
        "fx_rates"    : fx_rates,
        # Error batches. Written separately to the error table
        "err_no_customer" : err_no_customer,
        "err_bad_qty"     : err_bad_qty,
    }

# COMMAND ----------

print("Data cleaning notebook loaded")
print("  Call run_cleaning(run_ts) to execute")
