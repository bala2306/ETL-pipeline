# Databricks notebook source
# MAGIC %md
# MAGIC # Fact Transforms
# MAGIC Builds the fact table for the analytical model.
# MAGIC
# MAGIC Takes clean order items from `run_transforms()` and the date dimension
# MAGIC from `build_dimensions()`, joins in the date surrogate key, and
# MAGIC returns the final fact table ready to write.
# MAGIC
# MAGIC The only function `main_pipeline` calls from here is `build_facts()`.
# MAGIC
# MAGIC > Called via `%run` from `main_pipeline`. Never run directly.

# COMMAND ----------

def _orders_fact(order_items, dim_date):
    """
    One row per order line. The grain is order_id + product.

    This correctly preserves multi-line orders:
        A-005 has two product lines  (products 2 and 3)
        A-009 has three product lines (products 4, 7, and 11)

    Foreign keys point to each dimension table:
        cust_key → customer dimension
        prod_key → product dimension
        date_key → date dimension

    Revenue is carried over from the order items table where it was
    already computed using the historically correct FX rate.
    """
    return (order_items
        .join(
            dim_date.select("full_date", "date_key"),
            F.col("order_date") == F.col("full_date"),
            how="left")
        .drop("full_date")
        .select(
            F.col("order_id"),
            F.col("cust_id").alias("cust_key"),
            F.col("prod_id").alias("prod_key"),
            F.col("date_key"),
            F.col("quantity"),
            F.col("revenue_usd"),
            F.col("status")))

# COMMAND ----------
# MAGIC %md ## Entry Point

# COMMAND ----------

def build_facts(cleaned, dimensions, run_ts):
    """
    Builds all fact tables in one call.

    Takes the dicts returned by run_transforms() and build_dimensions()
    and returns a dict of fact DataFrames keyed by simple human-readable names.
    """
    print("  Building orders fact table...")

    orders_fact = (_orders_fact(cleaned["order_items"], dimensions["dim_date"])
                   .withColumn("_updated_at", run_ts))

    check_nulls(orders_fact,
                ["order_id", "cust_key", "prod_key", "date_key", "revenue_usd"],
                "orders fact")
    check_row_count(orders_fact, min_rows=1, table_label="orders fact")

    print()
    print("  Facts built")
    print(f"    orders : {orders_fact.count()} rows")

    return {
        "fact_orders" : orders_fact,
    }

# COMMAND ----------

print("Fact transforms notebook loaded")
print("  Call build_facts(cleaned, dimensions, run_ts) to execute")
