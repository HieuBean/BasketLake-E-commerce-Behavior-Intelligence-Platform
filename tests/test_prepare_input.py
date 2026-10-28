"""
tests/test_prepare_input.py
Owner: M1 (Hiếu)
Test module prepare_input: schema, sampling, amplification, month extraction.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from src.prepare_input.main import (
    RAW_SCHEMA,
    sample_tier_s,
    amplify_xl,
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
])


def test_raw_schema_columns():
    """Kiểm tra RAW_SCHEMA có đủ 9 trường gốc theo hợp đồng dữ liệu."""
    field_names = [f.name for f in RAW_SCHEMA.fields]
    expected = [
        "event_time", "event_type", "product_id", "category_id",
        "category_code", "brand", "price", "user_id", "user_session",
    ]
    assert field_names == expected


def test_sample_tier_s(spark: SparkSession):
    """Kiểm tra hàm sample_tier_s giảm số dòng đúng kỳ vọng."""
    rows = [
        ("2019-10-01 00:00:00 UTC", "view", i, 100, "cat", "b", 10.0, i, "s")
        for i in range(100)
    ]
    df = spark.createDataFrame(rows, schema=SCHEMA)
    sampled = sample_tier_s(df, fraction=0.1, seed=42)
    count = sampled.count()
    assert 0 < count < 100


def test_amplify_xl(spark: SparkSession):
    """Kiểm tra amplify_xl nhân bản đúng factor và offset user_id, product_id."""
    rows = [
        ("2019-10-01 00:00:00 UTC", "view", 1, 100, "cat", "b", 10.0, 10, "s"),
        ("2019-10-01 00:00:01 UTC", "purchase", 2, 100, "cat", "b", 20.0, 20, "s"),
    ]
    df = spark.createDataFrame(rows, schema=SCHEMA)
    amplified = amplify_xl(df, factor=3)
    assert amplified.count() == 6

    # Kiểm tra offset user_id
    user_ids = [r["user_id"] for r in amplified.collect()]
    assert 10 in user_ids
    assert 10_000_010 in user_ids
    assert 20_000_010 in user_ids
