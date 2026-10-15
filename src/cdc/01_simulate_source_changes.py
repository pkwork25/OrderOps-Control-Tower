# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Simulate OLTP changes on `orders_source`
# MAGIC Stands in for a real order system so the pipeline has **inserts, updates and deletes** to propagate.
# MAGIC Each run: places new orders, advances a sample of open orders through the lifecycle
# MAGIC (with a small cancel rate), and hard-deletes a few cancelled orders (e.g. test/GDPR purge).
# MAGIC In production set the job parameter `simulate=false` and land real data via
# MAGIC Lakeflow Connect / Debezium / DMS - nothing downstream changes.

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import functions as F

for name, default in [("catalog", "main"), ("schema", "orderops"), ("simulate", "true"),
                      ("new_orders", "40"), ("updates", "60"), ("deletes", "5")]:
    dbutils.widgets.text(name, default)

catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
src = f"`{catalog}`.`{schema}`.orders_source"

if dbutils.widgets.get("simulate").lower() != "true":
    dbutils.notebook.exit("simulate=false - skipping synthetic changes")

n_new = int(dbutils.widgets.get("new_orders"))
n_upd = int(dbutils.widgets.get("updates"))
n_del = int(dbutils.widgets.get("deletes"))

# COMMAND ----------

# MAGIC %md ### 1 · INSERT new orders

# COMMAND ----------

REGIONS = ["North", "South", "East", "West"]
CHANNELS = ["web", "app", "partner"]
PAYMENTS = ["UPI", "UPI", "UPI", "CARD", "NETBANKING", "COD"]  # UPI-heavy, like Indian e-commerce

now = datetime.utcnow()
rows = []
for _ in range(n_new):
    created = now - timedelta(hours=random.uniform(0, 96))
    items = random.randint(1, 6)
    rows.append((
        f"ORD-{uuid.uuid4().hex[:8].upper()}",
        f"CUST-{random.randint(1, 500):05d}",
        random.choice(REGIONS),
        random.choice(CHANNELS),
        "PLACED",
        random.choice(PAYMENTS),
        items,
        Decimal(str(round(random.uniform(199, 2499) * items, 2))),
        created + timedelta(hours=random.choice([24, 48, 72])),
        created,
        now,
    ))

target_schema = spark.table(src).schema
spark.createDataFrame(rows, schema=target_schema).write.mode("append").saveAsTable(src)
print(f"inserted {n_new} orders")

# COMMAND ----------

# MAGIC %md ### 2 · UPDATE - advance open orders one lifecycle step (5% get cancelled)

# COMMAND ----------

(spark.table(src)
     .where("status NOT IN ('DELIVERED', 'CANCELLED')")
     .orderBy(F.rand())
     .limit(n_upd)
     .select("order_id", "status")
     .withColumn(
         "new_status",
         F.when(F.rand() < 0.05, F.lit("CANCELLED"))
          .when(F.col("status") == "PLACED", "PAID")
          .when(F.col("status") == "PAID", "PACKED")
          .when(F.col("status") == "PACKED", "SHIPPED")
          .when(F.col("status") == "SHIPPED", "DELIVERED"))
     .createOrReplaceTempView("status_transitions"))

res = spark.sql(f"""
MERGE INTO {src} t
USING status_transitions s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET t.status = s.new_status, t.updated_ts = current_timestamp()
""")
display(res)

# COMMAND ----------

# MAGIC %md ### 3 · DELETE - purge a few cancelled orders

# COMMAND ----------

to_delete = [r.order_id for r in
             spark.table(src).where("status = 'CANCELLED'").orderBy(F.rand()).limit(n_del).collect()]
if to_delete:
    ids = ", ".join(f"'{i}'" for i in to_delete)
    spark.sql(f"DELETE FROM {src} WHERE order_id IN ({ids})")
print(f"deleted {len(to_delete)} orders: {to_delete}")
