# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transformations
# MAGIC Applies type casting, business rules, and enrichment
# MAGIC to the ingested DataFrames produced by `run_cleaning()`.
# MAGIC
# MAGIC The only function `main_pipeline` ever calls from here is `run_transforms()`.
# MAGIC Everything else is an internal detail.
# MAGIC
# MAGIC > Called via `%run` from `main_pipeline`. Never run directly.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 2. Transform
# MAGIC
# MAGIC Reads from the ingested DataFrames produced by `03_data_cleaning`.
# MAGIC Casts every column to its correct type and applies business rules.
# MAGIC
# MAGIC **Key decisions made here:**
# MAGIC - `zip` stays as a string. Casting to integer destroys leading zeros
# MAGIC - `price` is NOT converted to USD on the product. Conversion happens
# MAGIC   per order using the rate for that specific order date
# MAGIC - FX date gaps are forward-filled with the last known rate (the standard
# MAGIC   approach for FX reference data. The rate didn't change, it just wasn't recorded)
# MAGIC - `top_category` is resolved here by walking up the category tree so that
# MAGIC   every downstream query gets it for free without repeating the logic

# COMMAND ----------

def _transform_customers(raw):
    """
    Cast types and standardise:
        created_at  string  → date
        active y/n  string  → boolean
        type        string  → customer_type, lowercase
        cust_id     string  → long integer
        zip         string  → trimmed string (never cast to integer)
    """
    return (raw
        .withColumn("created_at",
            F.to_date(F.trim(F.col("created_at")), "M/d/yyyy"))
        .withColumn("is_active",
            F.when(F.lower(F.trim(F.col("active"))) == "y", True)
             .when(F.lower(F.trim(F.col("active"))) == "n", False)
             .otherwise(None).cast("boolean"))
        .withColumn("customer_type",
            F.lower(F.trim(F.col("type"))))
        .withColumn("cust_id",
            F.col("cust_id").cast("long"))
        .withColumn("zip",
            F.trim(F.col("zip")))
        .select(
            "cust_id", "created_at", "country",
            "region", "customer_type", "zip", "is_active"))


def _transform_orders(raw):
    """
    Cast types and standardise:
        order_date_raw  string  → timestamp + date
        quantity_raw    string  → integer
        status          string  → lowercase
        cust_id         string  → long integer
        prod_id         string  → integer
    """
    return (raw
        .withColumn("order_timestamp",
            F.to_timestamp(F.trim(F.col("order_date_raw")), "M/d/yyyy H:mm"))
        .withColumn("order_date",
            F.to_date(F.col("order_timestamp")))
        .withColumn("quantity",
            F.expr("try_cast(quantity_raw as int)"))
        .withColumn("status",
            F.lower(F.trim(F.col("status"))))
        .withColumn("cust_id",
            F.col("cust_id").cast("long"))
        .withColumn("prod_id",
            F.col("prod_id").cast("integer"))
        .select(
            "order_id", "cust_id", "prod_id",
            "order_timestamp", "order_date",
            "quantity", "status"))


def _transform_products(raw, categories):
    """
    Cast types, normalise currency to uppercase, and resolve
    the top-level category for each product by walking up the tree.

    The category tree is two levels deep:
        chocolate → Sweet → (root)  →  top_category = Sweet
        candy     → Sweet → (root)  →  top_category = Sweet
        crisp     → salt  → (root)  →  top_category = salt
        chip      → salt  → (root)  →  top_category = salt
        Sweet     → (root)          →  top_category = Sweet
    """
    # Walk one level up in the tree to find each category's parent
    tree = (categories
        .alias("child")
        .join(categories.alias("parent"),
              F.col("child.parent") == F.col("parent.child"),
              how="left")
        .select(
            F.col("child.cat_id").cast("integer").alias("cat_id"),
            F.col("child.child").alias("category_name"),
            F.coalesce(
                F.col("parent.child"),
                F.col("child.child")).alias("top_category")))

    return (raw
        .withColumn("prod_id",  F.col("prod_id").cast("integer"))
        .withColumn("cat_id",   F.col("cat_id").cast("integer"))
        .withColumn("price",    F.col("price").cast("double"))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .join(tree, on="cat_id", how="left")
        .select(
            "prod_id", "cat_id", "prod_name",
            "price", "currency",
            "category_name", "top_category"))


def _transform_fx_rates(raw):
    """
    Cast types, normalise currency to uppercase, and forward-fill
    any gaps in the date coverage.

    The source has a 342-day gap between January 2019 and January 2020.
    We build a full calendar spine for the entire range and fill any
    missing dates with the last known rate for that currency.
    """
    typed = (raw
        .withColumn("rate_date",   F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("currency",    F.upper(F.trim(F.col("currency"))))
        .withColumn("rate_to_usd", F.col("conversion").cast("double"))
        .select("rate_date", "currency", "rate_to_usd"))

    bounds = typed.agg(
        F.min("rate_date").alias("min_date"),
        F.max("rate_date").alias("max_date")).collect()[0]

    # Full calendar spine. One row per date per currency
    spine = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{bounds.min_date}'),
            to_date('{bounds.max_date}'),
            interval 1 day)) AS rate_date
    """).crossJoin(typed.select("currency").distinct())

    actual_dates = [r.rate_date for r in typed.select("rate_date").distinct().collect()]

    fill_window = (Window
        .partitionBy("currency")
        .orderBy("rate_date")
        .rowsBetween(Window.unboundedPreceding, 0))

    return (spine
        .join(typed, on=["rate_date", "currency"], how="left")
        .withColumn("rate_to_usd",
            F.last("rate_to_usd", ignorenulls=True).over(fill_window))
        .withColumn("_fx_fill_method",
            F.when(F.col("rate_to_usd").isNull(),        F.lit("no_rate_available"))
             .when(F.col("rate_date").isin(actual_dates), F.lit("actual"))
             .otherwise(                                  F.lit("forward_filled"))))


def _build_order_items(orders, products, fx_rates):
    """
    Join orders to products and FX rates to produce one enriched row
    per order line, including revenue converted to USD.

    Revenue = price × fx_rate_for_that_date × quantity

    The FX rate is matched on both date and currency so each order
    gets the historically correct rate, not a fixed conversion.
    """
    with_product = orders.join(products, on="prod_id", how="inner")

    # Alias fx_rates currency to avoid ambiguous column reference on the join condition
    fx_for_join = (fx_rates
        .select(
            F.col("rate_date"),
            F.col("currency").alias("fx_currency"),
            F.col("rate_to_usd"),
            F.col("_fx_fill_method")))

    return (with_product
        .join(
            fx_for_join,
            on=[
                F.col("order_date") == F.col("rate_date"),
                F.col("currency")   == F.col("fx_currency")],
            how="left")
        .drop("rate_date", "fx_currency")
        .withColumn("revenue_usd",
            F.round(F.col("price") * F.col("rate_to_usd") * F.col("quantity"), 4))
        .select(
            "order_id", "cust_id", "prod_id", "prod_name",
            "order_timestamp", "order_date", "quantity", "status",
            "price", "currency", "cat_id", "category_name", "top_category",
            "rate_to_usd", "revenue_usd", "_fx_fill_method"))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Entry Point
# MAGIC
# MAGIC `run_transforms()` is the only function `main_pipeline` calls from this notebook.
# MAGIC It takes the raw ingested DataFrames from `run_cleaning()` and returns
# MAGIC fully typed, enriched DataFrames ready for the dimensional model.

# COMMAND ----------

def run_transforms(ingested, run_ts):
    """
    Applies all type casting and business rules in one call.

    Takes the dict returned by run_cleaning() and returns a dict
    of fully typed and enriched DataFrames, plus the error batches
    passed through for writing.
    """

    # ── Stage 2: Transform ────────────────────────────────────────────────────
    print("  Applying types and business rules...")

    customers  = _transform_customers(ingested["customers"]).withColumn("_updated_at", run_ts)
    orders     = _transform_orders(ingested["orders"]).withColumn("_updated_at", run_ts)
    fx_rates   = _transform_fx_rates(ingested["fx_rates"]).withColumn("_updated_at", run_ts)
    products   = _transform_products(ingested["products"], ingested["categories"]).withColumn("_updated_at", run_ts)
    order_items = _build_order_items(orders, products, fx_rates).withColumn("_updated_at", run_ts)

    # ── Quick sanity checks ───────────────────────────────────────────────────
    check_nulls(customers,   ["cust_id", "created_at", "is_active"],  "customers")
    check_nulls(orders,      ["order_id", "cust_id", "order_timestamp", "quantity"], "orders")
    check_nulls(products,    ["prod_id", "price", "currency", "top_category"], "products")
    check_nulls(order_items, ["revenue_usd"], "order items")

    print()
    print("  Transforms complete")
    print(f"    customers   : {customers.count()} rows")
    print(f"    orders      : {orders.count()} rows")
    print(f"    products    : {products.count()} rows")
    print(f"    fx rates    : {fx_rates.count()} rows")
    print(f"    order items : {order_items.count()} rows")

    return {
        # Typed & enriched data. Used for Silver writes and building Gold
        "customers"   : customers,
        "orders"      : orders,
        "products"    : products,
        "categories"  : ingested["categories"],
        "fx_rates"    : fx_rates,
        "order_items" : order_items,
        # Error batches. Passed through from ingest for writing
        "err_no_customer" : ingested["err_no_customer"],
        "err_bad_qty"     : ingested["err_bad_qty"],
    }

# COMMAND ----------

print("Data transformations notebook loaded")
print("  Call run_transforms(ingested, run_ts) to execute")
