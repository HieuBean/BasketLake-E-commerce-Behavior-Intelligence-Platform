"""
tests/test_cleaning_rules.py
Owner: B
Test từng luật R1–R9 trên dữ liệu tay.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from src.processing.bronze_to_silver import (
    apply_r1_null_keys,
    apply_r2_event_type,
    apply_r4_price,
    apply_r5_brand,
    apply_r6_category,
    VALID_EVENT_TYPES,
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


def make_row(**kwargs):
    defaults = {
        "event_time":    "2019-10-01 00:00:00 UTC",
        "event_type":    "view",
        "product_id":    1,
        "category_id":   100,
        "category_code": "electronics.smartphone",
        "brand":         "samsung",
        "price":         199.0,
        "user_id":       999,
        "user_session":  "session_abc",
        "source_file":   "2019-Oct.csv",
        "source_month":  "2019-10",
    }
    defaults.update(kwargs)
    return tuple(defaults.values())


# ---- R1 ----

def test_r1_keeps_valid_row(spark: SparkSession):
    df = spark.createDataFrame([make_row()], schema=SCHEMA)
    clean, q = apply_r1_null_keys(df)
    assert clean.count() == 1
    assert q.count() == 0


def test_r1_quarantines_null_user_id(spark: SparkSession):
    df = spark.createDataFrame([make_row(user_id=None)], schema=SCHEMA)
    clean, q = apply_r1_null_keys(df)
    assert clean.count() == 0
    assert q.count() == 1
    assert q.collect()[0]["reject_reason"] == "null_key_field"


# ---- R2 ----

def test_r2_keeps_valid_event_type(spark: SparkSession):
    df = spark.createDataFrame([make_row(event_type="purchase")], schema=SCHEMA)
    clean, q = apply_r2_event_type(df, VALID_EVENT_TYPES)
    assert clean.count() == 1


def test_r2_quarantines_unknown_event_type(spark: SparkSession):
    df = spark.createDataFrame([make_row(event_type="unknown_event")], schema=SCHEMA)
    clean, q = apply_r2_event_type(df, VALID_EVENT_TYPES)
    assert clean.count() == 0
    assert q.count() == 1


# ---- R4 ----

def test_r4_quarantines_negative_price(spark: SparkSession):
    df = spark.createDataFrame([make_row(price=-5.0)], schema=SCHEMA)
    clean, q = apply_r4_price(df)
    assert clean.count() == 0
    assert q.count() == 1


def test_r4_flags_zero_price(spark: SparkSession):
    df = spark.createDataFrame([make_row(price=0.0)], schema=SCHEMA)
    clean, q = apply_r4_price(df)
    assert clean.count() == 1
    assert clean.collect()[0]["price_valid"] == False  # noqa: E712


# ---- R5 ----

def test_r5_fills_null_brand(spark: SparkSession):
    df = spark.createDataFrame([make_row(brand=None)], schema=SCHEMA)
    clean, _ = apply_r5_brand(df)
    row = clean.collect()[0]
    assert row["brand"] == "unknown"
    assert row["brand_missing"] == True  # noqa: E712


# ---- R6 ----

def test_r6_splits_category_code(spark: SparkSession):
    df = spark.createDataFrame([make_row(category_code="electronics.smartphone.apple")], schema=SCHEMA)
    clean, _ = apply_r6_category(df)
    row = clean.collect()[0]
    assert row["category_l1"] == "electronics"
    assert row["category_l2"] == "smartphone"
    assert row["category_l3"] == "apple"


def test_r6_handles_null_category(spark: SparkSession):
    df = spark.createDataFrame([make_row(category_code=None)], schema=SCHEMA)
    clean, _ = apply_r6_category(df)
    row = clean.collect()[0]
    assert row["category_l1"] == "unknown"
