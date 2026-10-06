"""Minimal metadata-driven ingestion framework: one config entry per source,
generic read -> standardise -> write. Most enterprise Databricks projects are a
bigger version of this, so understanding it unlocks the whole codebase."""
from dataclasses import dataclass, field
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, functions as F


@dataclass
class SourceConfig:
    name: str
    path: str
    fmt: str = "parquet"
    keys: List[str] = field(default_factory=list)
    renames: Dict[str, str] = field(default_factory=dict)
    target: str = ""


def read_source(spark: SparkSession, cfg: SourceConfig) -> DataFrame:
    return spark.read.format(cfg.fmt).load(cfg.path)


def standardise(df: DataFrame, cfg: SourceConfig) -> DataFrame:
    for old, new in cfg.renames.items():
        df = df.withColumnRenamed(old, new)
    df = df.toDF(*[c.strip().lower() for c in df.columns])
    if cfg.keys:
        df = df.dropna(subset=cfg.keys).dropDuplicates(cfg.keys)
    return df.withColumn("_loaded_at", F.current_timestamp())


def write_target(df: DataFrame, cfg: SourceConfig, mode: str = "overwrite") -> None:
    df.write.format("delta").mode(mode).saveAsTable(cfg.target or f"bronze.{cfg.name}")


def run(spark: SparkSession, configs: List[SourceConfig]) -> None:
    for cfg in configs:
        write_target(standardise(read_source(spark, cfg), cfg), cfg)
