# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Utils
# MAGIC Generic reusable helper functions for the ecommerce ETL pipeline.
# MAGIC Nothing in here is specific to any one table or business rule .
# MAGIC these are the building blocks every other notebook depends on.
# MAGIC
# MAGIC > Called via `%run` from `main_pipeline`. Never run directly.
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |---|---|
# MAGIC | `get_run_timestamp()` | Single consistent timestamp for the entire pipeline run |
# MAGIC | `get_run_id()` | Unique identifier for this pipeline run |
# MAGIC | `write_delta()` | Write a DataFrame to a Delta table (overwrite, idempotent) |
# MAGIC | `write_errors()` | Append unrecoverable rows to the error table with metadata |
# MAGIC | `dedup_by_window()` | Deduplicate a DataFrame using row_number over a window |
# MAGIC | `check_nulls()` | Assert no nulls in critical columns. Raises if any found |
# MAGIC | `check_row_count()` | Assert row count meets a minimum. Raises if not |
# MAGIC | `log_audit()` | Append one audit record to the pipeline run log |
# MAGIC | `section()` | Print a clean section header during pipeline run |

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------
# MAGIC %md ## Run Identifiers

# COMMAND ----------

def get_run_timestamp():
    """
    Returns the current UTC time as a Spark literal timestamp.
    Call once at the start of main() and pass the result through
    so every table written in one run gets the exact same timestamp.
    """
    return F.lit(datetime.utcnow()).cast("timestamp")


def get_run_id() -> str:
    """
    Returns a unique UUID string identifying this pipeline run.
    Used in the audit log to group all table writes for one run together.
    """
    return str(uuid.uuid4())

# COMMAND ----------
# MAGIC %md ## Write Helpers

# COMMAND ----------

def write_delta(df: DataFrame, table_name: str, mode: str = "overwrite") -> int:
    """
    Write a DataFrame to a Delta table.

    Defaults to overwrite so the pipeline is safe to re-run
    without producing duplicate data.

    Returns:
        Number of rows written.
    """
    count = df.count()
    (df.write
       .format("delta")
       .mode(mode)
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))
    print(f"    Wrote {count:>5} rows  →  {table_name}")
    return count


def write_errors(
        df          : DataFrame,
        table_name  : str,
        error_code  : str,
        error_message: str,
        source_file : str,
        run_ts,
        mode        : str = "append") -> int:
    """
    Write rows that cannot be recovered to the error / quarantine table.
    Tags every row with error metadata before writing.

    Only call this for truly unrecoverable rows. Rows where there is
    no deterministic business rule that can produce a correct value.

    Use mode="overwrite" on the first write_errors call in a pipeline run
    to clear stale data from previous runs, then mode="append" for any
    subsequent calls that write to the same table in the same run.

    Returns:
        Number of rows quarantined (0 if none).
    """
    count = df.count()
    if count == 0:
        print(f"    No rows quarantined for [{error_code}]")
        return 0
    (df
     .withColumn("_error_code",    F.lit(error_code))
     .withColumn("_error_message", F.lit(error_message))
     .withColumn("_source_file",   F.lit(source_file))
     .withColumn("_ingested_at",   run_ts)
     .write.format("delta").mode(mode)
     .option("overwriteSchema", "true")
     .saveAsTable(table_name))
    print(f"    Quarantined {count} row(s)  →  {table_name}  [{error_code}]")
    return count

# COMMAND ----------
# MAGIC %md ## Deduplication

# COMMAND ----------

def dedup_by_window(
        df             : DataFrame,
        partition_cols : list,
        order_col      = None,
        order_desc     : bool = True) -> DataFrame:
    """
    Deduplicate a DataFrame by keeping one row per group defined by
    partition_cols, choosing the winner based on order_col.

    Args:
        df             : Input DataFrame to deduplicate.
        partition_cols : Columns that define what makes a duplicate group.
        order_col      : Spark column expression to order rows within each group.
                         Pass None for exact-row dedup (arbitrary stable order).
        order_desc     : If True the highest value wins (e.g. latest date).
                         If False the lowest value wins. Default is True.

    Returns:
        DataFrame with exactly one row per partition group.

    Examples:
        # Keep the latest snapshot per customer
        dedup_by_window(
            df, ["cust_id"],
            order_col=F.to_date(F.col("created_at"), "M/d/yyyy"))

        # Remove exact row duplicates (all columns identical)
        dedup_by_window(df, df.columns, order_col=None)
    """
    if order_col is None:
        order_expr = F.lit(1)
    else:
        order_expr = order_col.desc() if order_desc else order_col.asc()

    window = Window.partitionBy(partition_cols).orderBy(order_expr)

    return (df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn"))

# COMMAND ----------
# MAGIC %md ## Data Quality Checks

# COMMAND ----------

def check_nulls(df: DataFrame, columns: list, table_label: str) -> None:
    """
    Verify that none of the specified columns contain null values.

    Prints a pass or fail line per column.
    Raises ValueError if any nulls are found. This stops the pipeline
    immediately rather than letting bad data silently reach Gold.

    Typical use: call after every Silver and Gold transformation
    to catch any type-casting failures early.
    """
    print(f"    Null check on {table_label}:")
    failures = []
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        status = "pass" if null_count == 0 else "FAIL"
        print(f"      [{status}]  {col:<25}  nulls: {null_count}")
        if null_count > 0:
            failures.append((col, null_count))

    if failures:
        detail = ", ".join([f"{c}={n}" for c, n in failures])
        raise ValueError(
            f"Null check failed in {table_label}: {detail}. "
            f"Fix source data or casting logic before proceeding.")


def check_row_count(
        df          : DataFrame,
        min_rows    : int,
        table_label : str) -> int:
    """
    Verify that a DataFrame has at least min_rows rows.

    Raises ValueError if the count falls short. Stops the pipeline
    before an empty or near-empty table can silently produce wrong metrics.

    Returns:
        The actual row count.
    """
    count = df.count()
    if count < min_rows:
        raise ValueError(
            f"Row count check failed for {table_label}: "
            f"got {count}, expected at least {min_rows}. "
            f"Check source data and upstream transformations.")
    print(f"    Row count [{table_label}]: {count} rows (minimum {min_rows} required. Pass)")
    return count

# COMMAND ----------
# MAGIC %md ## Audit Logging

# COMMAND ----------

def log_audit(
        table_name : str,
        layer      : str,
        row_count  : int,
        status     : str,
        run_id     : str,
        run_ts,
        notes      : str = "") -> None:
    """
    Append one record to the pipeline audit log table.

    Every table write in the pipeline calls this so you have a
    complete history of every run. Which tables were written,
    how many rows, whether it succeeded, and when.

    This makes it easy to detect regressions: if bronze.orders
    suddenly drops from 21 rows to 5, the audit log shows it.

    Args:
        table_name : Full table name e.g. workspace.bronze.orders
        layer      : BRONZE, SILVER, GOLD, or ERROR
        row_count  : Number of rows written
        status     : SUCCESS or FAILED
        run_id     : Unique run identifier from get_run_id()
        run_ts     : Pipeline run timestamp from get_run_timestamp()
        notes      : Optional free-text note about this write
    """
    record = spark.createDataFrame([{
        "run_id"     : run_id,
        "pipeline"   : PIPELINE_NAME,
        "version"    : PIPELINE_VERSION,
        "layer"      : layer,
        "table_name" : table_name,
        "row_count"  : row_count,
        "status"     : status,
        "notes"      : notes,
        "logged_at"  : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }])
    (record.write
        .format("delta")
        .mode("append")
        .saveAsTable(TBL_AUDIT_LOG))

# COMMAND ----------
# MAGIC %md ## Print Helpers

# COMMAND ----------

def section(title: str) -> None:
    """Print a clean section header to mark the start of each pipeline phase."""
    width = 65
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def run_summary(layer_table_map: dict) -> None:
    """
    Print a clean end-of-run summary showing every table and its final row count.

    Args:
        layer_table_map: dict of { "LAYER_NAME": [(full_table_name, display_label)] }
    """
    print()
    print("=" * 65)
    print(f"  Pipeline Complete  .  {PIPELINE_NAME} v{PIPELINE_VERSION}")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 65)
    for layer, tables in layer_table_map.items():
        print(f"\n  {layer}")
        for full_name, label in tables:
            try:
                count = spark.table(full_name).count()
                print(f"    {label:<35} {count:>5} rows")
            except Exception:
                print(f"    {label:<35}  not found")

# COMMAND ----------

print("ETL utils loaded successfully")
print("  Available: get_run_timestamp, get_run_id, write_delta, write_errors,")
print("             dedup_by_window, check_nulls, check_row_count,")
print("             log_audit, section, run_summary")
