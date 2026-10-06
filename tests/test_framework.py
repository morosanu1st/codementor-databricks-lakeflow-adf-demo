import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipelines"))
from framework import SourceConfig, standardise  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield s
    s.stop()


def test_standardise_renames_lowercases_and_dedups(spark):
    df = spark.createDataFrame(
        [(1, "a"), (1, "a"), (None, "b")], ["OrderID", "Customer "])
    cfg = SourceConfig(name="orders", path="", keys=["order_id"],
                       renames={"OrderID": "order_id"})
    out = standardise(df, cfg)
    assert "customer" in out.columns and "_loaded_at" in out.columns
    assert out.count() == 1
