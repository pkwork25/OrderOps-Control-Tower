# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup Unity Catalog objects
# MAGIC Creates the schema, checkpoint volume and Delta tables for OrderOps.
# MAGIC Both `orders_source` and `orders_silver` have **Change Data Feed** enabled:
# MAGIC the source so we can read its row-level changes incrementally, and silver so
# MAGIC downstream consumers can chain CDC further. Idempotent - safe to run every time.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "orderops")
dbutils.widgets.text("app_sp", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
app_sp = dbutils.widgets.get("app_sp").strip()
fq = f"`{catalog}`.`{schema}`"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq} COMMENT 'OrderOps Control Tower - CDC order operations'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq}.checkpoints COMMENT 'Structured Streaming checkpoints for CDF readers'")

ORDER_COLS = """
    order_id              STRING NOT NULL COMMENT 'Business key, e.g. ORD-1A2B3C4D',
    customer_id           STRING          COMMENT 'Customer key, e.g. CUST-00042',
    region                STRING          COMMENT 'North | South | East | West',
    channel               STRING          COMMENT 'web | app | partner',
    status                STRING          COMMENT 'PLACED > PAID > PACKED > SHIPPED > DELIVERED, or CANCELLED',
    payment_method        STRING          COMMENT 'UPI | CARD | NETBANKING | COD',
    item_count            INT,
    order_amount          DECIMAL(12,2)   COMMENT 'Order value in INR',
    promised_delivery_ts  TIMESTAMP       COMMENT 'Delivery SLA promised to the customer',
    created_ts            TIMESTAMP,
    updated_ts            TIMESTAMP
"""

# Bronze: mirror of the OLTP orders table (in prod, written by Lakeflow Connect / Debezium / DMS)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.orders_source ({ORDER_COLS})
COMMENT 'Raw order rows as landed from the OLTP system. CDF is the CDC contract.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Silver: current state of every live order, maintained only from CDF
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.orders_silver (
    {ORDER_COLS},
    _cdc_commit_version BIGINT    COMMENT 'Source Delta version that produced this row state',
    _cdc_commit_ts      TIMESTAMP COMMENT 'Source commit timestamp - drives freshness SLOs',
    _ingested_ts        TIMESTAMP
)
COMMENT 'Current state of orders, incrementally maintained from orders_source CDF (inserts, updates, deletes).'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Audit trail of every change event (never deleted) - powers "what happened to order X?"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.order_change_log (
    order_id       STRING,
    change_type    STRING COMMENT 'insert | update_postimage | delete',
    status         STRING,
    order_amount   DECIMAL(12,2),
    commit_version BIGINT,
    commit_ts      TIMESTAMP,
    ingested_ts    TIMESTAMP
)
COMMENT 'Append-only history of CDF events propagated from orders_source.'
""")

# Gold: operational KPIs
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.order_ops_kpis (
    region       STRING,
    status       STRING,
    orders       BIGINT,
    gmv_inr      DECIMAL(18,2),
    sla_breaches BIGINT,
    refreshed_at TIMESTAMP
)
COMMENT 'Order counts, GMV and SLA breaches by region and status.'
""")

# COMMAND ----------

# MAGIC %md ### Least-privilege grants for the copilot app's service principal

# COMMAND ----------

if app_sp:
    grants = [
        f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{app_sp}`",
        f"GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA {fq} TO `{app_sp}`",
    ]
    for g in grants:
        try:
            spark.sql(g)
            print("OK  ", g)
        except Exception as e:  # e.g. you don't own the catalog - ask an admin to run it
            print("SKIP", g, "->", str(e).splitlines()[0])
else:
    print("app_sp not set - skipping grants (deploy the app first so the bundle can resolve it).")
