"""Incremental, idempotent batch load of orders into a Delta table with MERGE.
Usage (Databricks job task): orders_batch.py --load_date 2026-10-06"""
import argparse

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


def main(load_date: str, landing: str, target: str) -> None:
    spark = SparkSession.builder.getOrCreate()
    updates = (spark.read.parquet(f"{landing}/load_date={load_date}")
               .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
               .dropDuplicates(["order_id"]))

    if not spark.catalog.tableExists(target):
        updates.write.format("delta").partitionBy("order_date").saveAsTable(target)
        return

    (DeltaTable.forName(spark, target).alias("t")
        .merge(updates.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.modified_at > t.modified_at")
        .whenNotMatchedInsertAll()
        .execute())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--load_date", required=True)
    p.add_argument("--landing", default="abfss://landing@lake.dfs.core.windows.net/orders")
    p.add_argument("--target", default="silver.orders")
    a = p.parse_args()
    main(a.load_date, a.landing, a.target)
