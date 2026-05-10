# Databricks notebook source
# MAGIC %md
# MAGIC # Read Data
# MAGIC Reads every source file from the Volume into a raw DataFrame.
# MAGIC Every read uses an **explicit schema**. No `inferSchema` anywhere.
# MAGIC
# MAGIC No transformations or writes happen here. Only reads.
# MAGIC All DataFrames are available to every notebook in the shared session.
# MAGIC
# MAGIC > Called via `%run` from `main_pipeline`. Never run directly.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

# COMMAND ----------
# MAGIC %md ## Customer

# COMMAND ----------

# All columns STRING. Bronze always reflects source faithfully
customer_schema = StructType([
    StructField("cust_id",    StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("country",    StringType(), nullable=True),
    StructField("region",     StringType(), nullable=True),
    StructField("type",       StringType(), nullable=True),
    StructField("zip",        StringType(), nullable=True),
    StructField("active",     StringType(), nullable=True),
])

df_raw_customer = (spark.read
    .option("header", True)
    .schema(customer_schema)
    .csv(SRC_CUSTOMER))

print(f"customer.csv           {df_raw_customer.count()} rows")

# COMMAND ----------
# MAGIC %md ## Orders

# COMMAND ----------

# "order date" has a space in the source header. Renamed on read
orders_schema = StructType([
    StructField("order_id",       StringType(), nullable=True),
    StructField("cust_id",        StringType(), nullable=True),
    StructField("order_date_raw", StringType(), nullable=True),  # renamed from "order date"
    StructField("prod_id",        StringType(), nullable=True),
    StructField("quantity_raw",   StringType(), nullable=True),  # renamed from "quantity"
    StructField("status",         StringType(), nullable=True),
])

df_raw_orders = (spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(SRC_ORDERS))

print(f"orders.csv             {df_raw_orders.count()} rows")

# COMMAND ----------
# MAGIC %md ## Product

# COMMAND ----------

product_schema = StructType([
    StructField("prod_id",   StringType(), nullable=True),
    StructField("cat_id",    StringType(), nullable=True),
    StructField("prod_name", StringType(), nullable=True),
    StructField("price",     StringType(), nullable=True),
    StructField("currency",  StringType(), nullable=True),
])

df_raw_product = (spark.read
    .option("header", True)
    .schema(product_schema)
    .csv(SRC_PRODUCT))

print(f"product.csv            {df_raw_product.count()} rows")

# COMMAND ----------
# MAGIC %md ## Product Category Tree

# COMMAND ----------

cat_tree_schema = StructType([
    StructField("cat_id", StringType(), nullable=True),
    StructField("child",  StringType(), nullable=True),
    StructField("parent", StringType(), nullable=True),
])

df_raw_cat_tree = (spark.read
    .option("header", True)
    .schema(cat_tree_schema)
    .csv(SRC_CAT_TREE))

print(f"prod_cat_tree.csv      {df_raw_cat_tree.count()} rows")

# COMMAND ----------
# MAGIC %md ## FX / Currency Rates

# COMMAND ----------

fx_schema = StructType([
    StructField("date",       StringType(), nullable=True),
    StructField("currency",   StringType(), nullable=True),
    StructField("conversion", DoubleType(), nullable=True),
])

df_raw_fx = (spark.read
    .option("multiline", True)
    .schema(fx_schema)
    .json(SRC_FX))

print(f"currency_conversion.json {df_raw_fx.count()} rows")

# COMMAND ----------

print()
print("All source files loaded into DataFrames")
print("Available: df_raw_customer, df_raw_orders, df_raw_product,")
print("           df_raw_cat_tree, df_raw_fx")
