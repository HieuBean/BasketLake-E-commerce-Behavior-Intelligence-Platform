"""
tests/test_invariants.py
Owner: A
Kiểm tra bất biến pipeline: row_in = row_clean + row_quarantine.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from src.processing.bronze_to_silver import (
    apply_r1_null_keys,
    apply_r4_price,
)

SCHEMA = StructType([
    StructField("event_time",    StringType(),  True),
    StructField("event_type",    StringType(),  True),
    StructField("product_id",    LongType(),    True),
    StructField("category_id",   LongType(),    True),
    StructField("category_code", StringType(),  True),
    StructField("brand",         StringType(),  True),
    StructField("price",         DoubleType(),  True),
    StructField("user_id",       LongType(),    True),
    StructField("user_session",  StringType(),  True),
    StructField("source_file",   StringType(),  True),
    StructField("source_month",  StringType(),  True),
])


def make_row(user_id=1, price=10.0, event_type="view", product_id=1):
    return ("2019-10-01 00:00:00 UTC", event_type, product_id, 100, "cat", "brand", price, user_id, "s", "f.csv", "2019-10")


def test_r1_invariant(spark: SparkSession):
    rows = [make_row(user_id=1), make_row(user_id=None), make_row(user_id=2)]
    df = spark.createDataFrame(rows, schema=SCHEMA)
    total_in = df.count()
    clean, q = apply_r1_null_keys(df)
    assert total_in == clean.count() + q.count(), "Bất biến R1 vi phạm"


def test_r4_invariant(spark: SparkSession):
    rows = [make_row(price=50.0), make_row(price=-1.0), make_row(price=0.0), make_row(price=None)]
    df = spark.createDataFrame(rows, schema=SCHEMA)
    total_in = df.count()
    clean, q = apply_r4_price(df)
    assert total_in == clean.count() + q.count(), "Bất biến R4 vi phạm"


def test_r7_invariant(spark: SparkSession):
    from src.processing.bronze_to_silver import apply_r7_dedupe
    rows = [
        make_row(user_id=1, product_id=10),
        make_row(user_id=1, product_id=10),  # duplicate
        make_row(user_id=2, product_id=20),
    ]
    df = spark.createDataFrame(rows, schema=SCHEMA)
    total_in = df.count()
    clean, q = apply_r7_dedupe(df)
    assert total_in == clean.count() + q.count(), "Bất biến R7 vi phạm"

