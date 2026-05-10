# Ecommerce ETL Pipeline

End-to-end ETL pipeline built on **Databricks (PySpark + Delta Lake)** that ingests raw ecommerce data, cleans it, builds a star schema, and exposes 5 business metrics via a SQL dashboard.

---

## Stack
Python · PySpark · SQL · Delta Lake · Databricks

---

## Architecture

```
Raw Files → Bronze (raw) → Silver (cleaned) → Gold (star schema) → Dashboard
```

| Layer  | What it holds |
|--------|--------------|
| Bronze | Raw data, exactly as in source files |
| Silver | Type-cast, deduplicated, FX-enriched |
| Gold   | dim_customer, dim_product, dim_date, fact_orders |
| Error  | Quarantined rows that can't be recovered |
| Audit  | Row counts and status for every pipeline run |

---

## Source Files

| File | Rows | Notes |
|------|------|-------|
| `orders.csv` | 24 | Has duplicates, null customer IDs, fractional quantity |
| `customer.csv` | 12 | 3 snapshots per customer, conflicting active flags |
| `product.csv` | 14 | Mixed-case currency (Yen → YEN) |
| `prod_cat_tree.csv` | 5 | 2-level category hierarchy |
| `currency_conversion.json` | 189 | USD / YEN / POUND rates, 342-day gap forward-filled |

---

## Notebooks (run in order via `main_pipeline.py`)

| Notebook | Purpose |
|----------|---------|
| `00_config` | All constants — paths, table names, schema names |
| `etl_utils` | Shared helpers — dedup, write, audit, null checks |
| `01_schema` | Creates all Delta table schemas |
| `02_read_data` | Reads source files with explicit schemas |
| `03_data_cleaning` | Deduplicates, validates, quarantines bad rows |
| `04_data_transformations` | Type casting, FX enrichment, category resolution |
| `05_dim_transforms` | Builds dim_customer, dim_product, dim_date |
| `06_fact_transforms` | Builds fact_orders |

---

## Data Quality Decisions

**Quarantined (3 rows → error table)**
- `A-21`, `A-22` - no customer ID, cannot attribute to any region
- `A-013` - quantity is 0.1, rounding would fabricate revenue

**Fixed inline**
- Customer dedup → keep latest snapshot per customer
- Exact duplicate order rows → collapsed to one
- `Yen` → normalized to `YEN`
- Duplicate FX entries on 2020-01-28 → keep last
- 342-day FX gap → forward-fill last known rate

---

## Business Metrics (Dashboard)

1. **Daily Active Users by Region** - unique customers per day, by region
2. **Sweet Category** - total revenue ($981.61), orders (8), quantity (27)
3. **Top 3 Products by Region** - ranked by revenue using `RANK()` window function
4. **Customer Lifetime Value** - total revenue per customer, active vs inactive
5. **Data Quality Audit** - quarantined rows + multi-line orders in one view

---

## How to Run

1. Upload all files in `Data/` to a Databricks Volume at `/Volumes/workspace/default/ecommerce_assignment/`
2. Import the `etl_pipeline/` notebooks into Databricks
3. Run `main_pipeline` — it orchestrates everything end to end

---

## Documentation

Full technical write-up (data model, quality decisions, trade-offs, SQL queries) → [`ThoughtFocus.pdf`](./ThoughtFocus.pdf)
