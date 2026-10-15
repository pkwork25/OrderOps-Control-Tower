# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold KPIs
# MAGIC Recomputes `order_ops_kpis` from silver. Silver is small (current state only), so a full
# MAGIC `INSERT OVERWRITE` is cheaper and simpler than incremental aggregation here.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "orderops")
fq = f"`{dbutils.widgets.get('catalog')}`.`{dbutils.widgets.get('schema')}`"

spark.sql(f"""
INSERT OVERWRITE {fq}.order_ops_kpis
SELECT
  region,
  status,
  count(*)                                                       AS orders,
  CAST(sum(order_amount) AS DECIMAL(18,2))                       AS gmv_inr,
  count_if(status NOT IN ('DELIVERED','CANCELLED')
           AND promised_delivery_ts < current_timestamp())       AS sla_breaches,
  current_timestamp()                                            AS refreshed_at
FROM {fq}.orders_silver
GROUP BY region, status
""")

display(spark.table(f"{fq}.order_ops_kpis").orderBy("region", "status"))
