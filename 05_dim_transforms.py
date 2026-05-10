# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension Transforms
# MAGIC Builds all dimension tables for the analytical model.
# MAGIC
# MAGIC Takes the clean data from `run_transforms()` and shapes it into
# MAGIC the dimensions a BI tool or analyst would query against.
# MAGIC
# MAGIC The only function `main_pipeline` calls from here is `build_dimensions()`.
# MAGIC
# MAGIC > Called via `%run` from `main_pipeline`. Never run directly.

# COMMAND ----------

def _customer_dimension(customers):
    """
    One row per customer with a surrogate key.

    The customer ID is stable and unique in this dataset so it doubles
    as the surrogate key. In a production system with history tracking
    you would generate a separate integer key per version.
    """
    return (customers
        .withColumnRenamed("cust_id", "cust_key")
        .withColumn("cust_id", F.col("cust_key"))
        .select(
            "cust_key", "cust_id",
            "country", "region",
            "customer_type", "zip",
            "is_active", "created_at"))


def _product_dimension(products, fx_rates):
    """
    One row per product with a USD display price.

    The display price uses the latest available FX rate for each currency.
    This is for presentation only. Actual revenue in the fact table always
    uses the FX rate for the specific order date.
    """
    latest_rates = (fx_rates
        .groupBy("currency")
        .agg(F.last("rate_to_usd", ignorenulls=True).alias("latest_rate")))

    return (products
        .join(latest_rates, on="currency", how="left")
        .withColumn("price_usd",
            F.round(F.col("price") * F.col("latest_rate"), 4))
        .withColumnRenamed("prod_id", "prod_key")
        .withColumn("prod_id", F.col("prod_key"))
        .select(
            "prod_key", "prod_id",
            "prod_name", "category_name", "top_category",
            "price_usd"))


def _date_dimension(order_items):
    """
    One row per distinct order date with calendar attributes.

    Built from actual order dates rather than a pre-generated calendar
    so it only contains dates that are meaningful to the business.

    The surrogate key is in YYYYMMDD format. Human-readable and
    sorts correctly as an integer without needing a lookup.
    """
    return (order_items
        .select(F.col("order_date").alias("full_date"))
        .distinct()
        .withColumn("date_key",
            F.date_format("full_date", "yyyyMMdd").cast("integer"))
        .withColumn("year",        F.year("full_date"))
        .withColumn("quarter",     F.quarter("full_date"))
        .withColumn("month",       F.month("full_date"))
        .withColumn("month_name",  F.date_format("full_date", "MMMM"))
        .withColumn("day",         F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend",  F.dayofweek("full_date").isin([1, 7]))
        .orderBy("full_date"))

# COMMAND ----------
# MAGIC %md ## Entry Point

# COMMAND ----------

def build_dimensions(cleaned, run_ts):
    """
    Builds all dimension tables in one call.

    Takes the dict returned by run_transforms() and returns a dict
    of dimension DataFrames keyed by simple human-readable names.
    """
    print("  Building customer dimension...")
    dim_customer = (_customer_dimension(cleaned["customers"])
                    .withColumn("_updated_at", run_ts))
    check_row_count(dim_customer, min_rows=1, table_label="customer dimension")

    print("  Building product dimension...")
    dim_product = (_product_dimension(cleaned["products"], cleaned["fx_rates"])
                   .withColumn("_updated_at", run_ts))
    check_row_count(dim_product, min_rows=1, table_label="product dimension")

    print("  Building date dimension...")
    dim_date = (_date_dimension(cleaned["order_items"])
                .withColumn("_updated_at", run_ts))
    check_row_count(dim_date, min_rows=1, table_label="date dimension")

    print()
    print("  Dimensions built")
    print(f"    customer : {dim_customer.count()} rows")
    print(f"    product  : {dim_product.count()} rows")
    print(f"    date     : {dim_date.count()} rows")

    return {
        "dim_customer" : dim_customer,
        "dim_product"  : dim_product,
        "dim_date"     : dim_date,
    }

# COMMAND ----------

print("Dimension transforms notebook loaded")
print("  Call build_dimensions(cleaned, run_ts) to execute")
