# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Propagate CDC: `orders_source` CDF -> `orders_silver`
# MAGIC
# MAGIC * Reads the source table's **Change Data Feed** as a stream with `trigger(availableNow=True)`,
# MAGIC   so each job run processes exactly the commits since the last checkpoint, then stops.
# MAGIC * Per micro-batch:
# MAGIC   1. drops `update_preimage` rows,
# MAGIC   2. appends every event to `order_change_log` (idempotent via `txnAppId`/`txnVersion`),
# MAGIC   3. collapses to the **latest change per order_id** (by `_commit_version`),
# MAGIC   4. `MERGE`s into silver: delete on `delete`, update only if the change is newer
# MAGIC      than what silver holds (out-of-order / replay safe), insert otherwise.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "orderops")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

fq = f"`{catalog}`.`{schema}`"
SOURCE = f"{fq}.orders_source"
SILVER = f"{fq}.orders_silver"
CHANGE_LOG = f"{fq}.order_change_log"
CHECKPOINT = f"/Volumes/{catalog}/{schema}/checkpoints/orders_silver_cdf"

BUSINESS_COLS = [
    "order_id", "customer_id", "region", "channel", "status", "payment_method",
    "item_count", "order_amount", "promised_delivery_ts", "created_ts", "updated_ts",
]

# COMMAND ----------

def latest_change_per_key(changes: DataFrame) -> DataFrame:
    """Keep one row per order_id: the highest commit version wins.
    On a tie inside one commit, a delete outranks a post-image."""
    w = Window.partitionBy("order_id").orderBy(
        F.col("_commit_version").desc(),
        F.when(F.col("_change_type") == "delete", 0).otherwise(1),
    )
    return changes.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
    changes = batch_df.where("_change_type != 'update_preimage'")
    if changes.isEmpty():
        return

    # (a) audit trail - txnAppId/txnVersion make the append exactly-once on retries
    (changes.select(
        "order_id",
        F.col("_change_type").alias("change_type"),
        "status",
        "order_amount",
        F.col("_commit_version").alias("commit_version"),
        F.col("_commit_timestamp").alias("commit_ts"),
        F.current_timestamp().alias("ingested_ts"),
    ).write.format("delta")
     .option("txnAppId", "orderops_change_log")
     .option("txnVersion", batch_id)
     .mode("append")
     .saveAsTable(CHANGE_LOG))

    # (b) current-state MERGE
    latest = latest_change_per_key(changes)
    row_values = {c: f"s.{c}" for c in BUSINESS_COLS}
    row_values.update({
        "_cdc_commit_version": "s._commit_version",
        "_cdc_commit_ts": "s._commit_timestamp",
        "_ingested_ts": "current_timestamp()",
    })

    (DeltaTable.forName(batch_df.sparkSession, SILVER).alias("t")
        .merge(latest.alias("s"), "t.order_id = s.order_id")
        .whenMatchedDelete(condition="s._change_type = 'delete'")
        .whenMatchedUpdate(
            condition="s._change_type != 'delete' AND s._commit_version > t._cdc_commit_version",
            set=row_values)
        .whenNotMatchedInsert(condition="s._change_type != 'delete'", values=row_values)
        .execute())

# COMMAND ----------

query = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)          # only used on the very first run; afterwards the checkpoint wins
    .table(SOURCE)
    .writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .queryName("orders_cdf_to_silver")
    .start()
)
query.awaitTermination()

# COMMAND ----------

rows_in = sum(p.get("numInputRows", 0) for p in query.recentProgress)
print(f"CDF rows processed this run: {rows_in}")
display(spark.sql(f"""
  SELECT count(*) AS live_orders,
         max(_cdc_commit_version) AS last_source_version,
         max(_cdc_commit_ts)      AS last_source_commit
  FROM {SILVER}"""))
