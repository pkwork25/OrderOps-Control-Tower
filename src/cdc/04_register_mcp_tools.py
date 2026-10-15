# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Register MCP tools (Unity Catalog SQL functions)
# MAGIC Every function in this schema is automatically exposed by the **Databricks managed MCP server**:
# MAGIC
# MAGIC `https://<workspace>/api/2.0/mcp/functions/<catalog>/<schema>`
# MAGIC
# MAGIC The function `COMMENT` becomes the tool description and parameter comments become the
# MAGIC argument descriptions the LLM sees - so they are written for the model, not for humans.
# MAGIC Tools are **read-only by construction** (SELECT-only bodies) and governed by UC grants.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "orderops")
fq = f"`{dbutils.widgets.get('catalog')}`.`{dbutils.widgets.get('schema')}`"

ORDER_ROW = """order_id STRING, customer_id STRING, region STRING, channel STRING, status STRING,
               payment_method STRING, order_amount DECIMAL(12,2), created_ts TIMESTAMP,
               promised_delivery_ts TIMESTAMP, updated_ts TIMESTAMP, is_sla_breached BOOLEAN"""
ORDER_SELECT = """order_id, customer_id, region, channel, status, payment_method, order_amount,
                  created_ts, promised_delivery_ts, updated_ts,
                  (status NOT IN ('DELIVERED','CANCELLED') AND promised_delivery_ts < current_timestamp())"""

FUNCTIONS = {
"get_order": f"""
CREATE OR REPLACE FUNCTION {fq}.get_order(
  order_id_in STRING COMMENT 'Exact order ID, format ORD- followed by 8 hex characters, e.g. ORD-1A2B3C4D')
RETURNS TABLE ({ORDER_ROW})
COMMENT 'Get the CURRENT state of one order (status, amount, payment method, SLA). Returns no rows if the order does not exist or was deleted - then call get_order_history.'
RETURN SELECT {ORDER_SELECT} FROM {fq}.orders_silver WHERE order_id = upper(trim(order_id_in))
""",

"get_order_history": f"""
CREATE OR REPLACE FUNCTION {fq}.get_order_history(
  order_id_in STRING COMMENT 'Exact order ID, e.g. ORD-1A2B3C4D')
RETURNS TABLE (order_id STRING, change_type STRING, status STRING, order_amount DECIMAL(12,2),
               commit_version BIGINT, commit_ts TIMESTAMP)
COMMENT 'Full change history (CDC events: insert, update, delete) for one order, oldest first. Use for questions like what happened to an order or when it was deleted/cancelled.'
RETURN SELECT order_id, change_type, status, order_amount, commit_version, commit_ts
       FROM {fq}.order_change_log WHERE order_id = upper(trim(order_id_in))
       ORDER BY commit_version
""",

"list_sla_breaches": f"""
CREATE OR REPLACE FUNCTION {fq}.list_sla_breaches(
  region_in STRING COMMENT 'One of North, South, East, West - or ALL for every region')
RETURNS TABLE ({ORDER_ROW}, hours_overdue DOUBLE)
COMMENT 'Open orders (not delivered or cancelled) whose promised delivery time has passed, most overdue first, max 50 rows.'
RETURN SELECT {ORDER_SELECT},
              round((unix_timestamp(current_timestamp()) - unix_timestamp(promised_delivery_ts)) / 3600.0, 1)
       FROM {fq}.orders_silver
       WHERE status NOT IN ('DELIVERED','CANCELLED')
         AND promised_delivery_ts < current_timestamp()
         AND (upper(region_in) = 'ALL' OR lower(region) = lower(region_in))
       ORDER BY promised_delivery_ts
       LIMIT 50
""",

"order_status_summary": f"""
CREATE OR REPLACE FUNCTION {fq}.order_status_summary(
  region_in STRING COMMENT 'One of North, South, East, West - or ALL for every region')
RETURNS TABLE (region STRING, status STRING, orders BIGINT, gmv_inr DECIMAL(18,2),
               sla_breaches BIGINT, refreshed_at TIMESTAMP)
COMMENT 'Aggregated KPIs: order counts, GMV in INR and SLA breaches by region and status. Use for overview / how-are-we-doing questions.'
RETURN SELECT region, status, orders, gmv_inr, sla_breaches, refreshed_at
       FROM {fq}.order_ops_kpis
       WHERE upper(region_in) = 'ALL' OR lower(region) = lower(region_in)
       ORDER BY region, status
""",

"get_customer_orders": f"""
CREATE OR REPLACE FUNCTION {fq}.get_customer_orders(
  customer_id_in STRING COMMENT 'Customer ID, format CUST- followed by 5 digits, e.g. CUST-00042')
RETURNS TABLE ({ORDER_ROW})
COMMENT 'Current orders for one customer, newest first, max 25 rows.'
RETURN SELECT {ORDER_SELECT} FROM {fq}.orders_silver
       WHERE customer_id = upper(trim(customer_id_in))
       ORDER BY created_ts DESC LIMIT 25
""",

"pipeline_freshness": f"""
CREATE OR REPLACE FUNCTION {fq}.pipeline_freshness()
RETURNS TABLE (live_orders BIGINT, last_source_version BIGINT, last_source_commit TIMESTAMP,
               lag_minutes BIGINT)
COMMENT 'Health of the CDC pipeline: how many live orders silver holds and how stale it is versus the source. Call this before answering if the user asks whether data is up to date.'
RETURN SELECT count(*), max(_cdc_commit_version), max(_cdc_commit_ts),
              timestampdiff(MINUTE, max(_cdc_commit_ts), current_timestamp())
       FROM {fq}.orders_silver
""",
}

for name, ddl in FUNCTIONS.items():
    spark.sql(ddl)
    print("registered", name)

# COMMAND ----------

# Smoke test the tools the copilot will call
display(spark.sql(f"SELECT * FROM {fq}.pipeline_freshness()"))
display(spark.sql(f"SELECT * FROM {fq}.order_status_summary('ALL')"))
display(spark.sql(f"SELECT * FROM {fq}.list_sla_breaches('ALL') LIMIT 5"))
