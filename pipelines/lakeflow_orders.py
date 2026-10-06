"""Lakeflow Declarative Pipeline (formerly Delta Live Tables): bronze -> silver -> gold.
Runs inside a Databricks pipeline; `dlt` and `spark` are provided by the runtime."""
import dlt
from pyspark.sql import functions as F

LANDING = spark.conf.get("orders.landing_path", "abfss://landing@lake.dfs.core.windows.net/orders/")


@dlt.table(comment="Raw orders, append-only, ingested incrementally with Auto Loader")
def bronze_orders():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", LANDING + "_schema")
            .load(LANDING)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path")))


@dlt.table(comment="Cleaned, typed, de-duplicated orders")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect("known_currency", "currency IN ('EUR','USD','GBP','INR')")
def silver_orders():
    return (dlt.read_stream("bronze_orders")
            .withColumn("order_ts", F.to_timestamp("order_ts"))
            .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
            .withWatermark("order_ts", "2 days")
            .dropDuplicates(["order_id"]))


@dlt.table(comment="Daily revenue per customer for BI")
def gold_daily_revenue():
    return (dlt.read("silver_orders")
            .groupBy(F.to_date("order_ts").alias("order_date"), "customer_id")
            .agg(F.sum("amount").alias("revenue"), F.countDistinct("order_id").alias("orders")))
