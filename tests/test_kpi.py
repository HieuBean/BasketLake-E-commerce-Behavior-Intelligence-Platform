"""
tests/test_kpi.py
Owner: D
Test định nghĩa KPI, funnel, cohort trên dữ liệu tay.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType, IntegerType, TimestampType,
)
from pyspark.sql import functions as F

from src.analytics.kpi import (
    compute_kpi_summary,
    funnel_by_category,
    compute_cohort_retention,
)

# ---- Schemas ----

FACT_SCHEMA = StructType([
    StructField("user_id",        LongType(),    False),
    StructField("user_session",   StringType(),  False),
    StructField("view_count",     IntegerType(), True),
    StructField("cart_count",     IntegerType(), True),
    StructField("purchase_count", IntegerType(), True),
    StructField("revenue",        DoubleType(),  True),
    StructField("converted",      BooleanType(), True),
    StructField("session_start",  StringType(),  True),
    StructField("category_l1",   StringType(),  True),
])

AGG_DAILY_SCHEMA = StructType([
    StructField("event_date",   StringType(), True),
    StructField("category_l1", StringType(), True),
    StructField("revenue",     DoubleType(), True),
    StructField("purchases",   LongType(),   True),
    StructField("sessions",    LongType(),   True),
])

AGG_FUNNEL_SCHEMA = StructType([
    StructField("category_l1",          StringType(), True),
    StructField("sessions_with_view",   LongType(),   True),
    StructField("sessions_with_cart",   LongType(),   True),
    StructField("sessions_with_purchase", LongType(), True),
])


# =============================================================================
# KPI Summary
# =============================================================================

def test_conversion_rate(spark: SparkSession):
    rows = [
        (1, "s1", 3, 1, 1, 99.0,  True,  "2019-10-01", "electronics"),
        (2, "s2", 5, 0, 0,  0.0,  False, "2019-10-01", "electronics"),
        (3, "s3", 2, 2, 0,  0.0,  False, "2019-10-01", "electronics"),
    ]
    fact = spark.createDataFrame(rows, schema=FACT_SCHEMA)
    agg  = spark.createDataFrame(
        [("2019-10-01", "electronics", 99.0, 1, 3)], schema=AGG_DAILY_SCHEMA
    )
    summary = compute_kpi_summary(fact, agg)
    # 1 converted / 3 sessions_with_view = 0.3333
    assert abs(summary["conversion_rate"] - (1 / 3)) < 0.001
    assert summary["total_revenue"] == 99.0
    assert summary["total_sessions"] == 3


def test_cart_abandonment(spark: SparkSession):
    rows = [
        (1, "s1", 1, 1, 0, 0.0,  False, "2019-10-01", "cat"),  # có cart, không mua
        (2, "s2", 1, 1, 1, 50.0, True,  "2019-10-01", "cat"),  # có cart, mua
        (3, "s3", 1, 0, 0, 0.0,  False, "2019-10-01", "cat"),  # không cart
    ]
    fact = spark.createDataFrame(rows, schema=FACT_SCHEMA)
    agg  = spark.createDataFrame(
        [("2019-10-01", "cat", 50.0, 1, 3)], schema=AGG_DAILY_SCHEMA
    )
    summary = compute_kpi_summary(fact, agg)
    # 1 abandoned / 2 sessions_with_cart = 0.5
    assert abs(summary["cart_abandonment"] - 0.5) < 0.001


def test_aov_proxy(spark: SparkSession):
    rows = [
        (1, "s1", 1, 1, 2, 200.0, True,  "2019-10-01", "cat"),
        (2, "s2", 1, 1, 1, 100.0, True,  "2019-10-01", "cat"),
    ]
    fact = spark.createDataFrame(rows, schema=FACT_SCHEMA)
    agg  = spark.createDataFrame(
        [("2019-10-01", "cat", 300.0, 2, 2)], schema=AGG_DAILY_SCHEMA
    )
    summary = compute_kpi_summary(fact, agg)
    # AOV = 300 / 2 converted sessions = 150
    assert summary["aov_proxy"] == 150.0


def test_zero_sessions(spark: SparkSession):
    """Đảm bảo không chia cho 0."""
    fact = spark.createDataFrame([], schema=FACT_SCHEMA)
    agg  = spark.createDataFrame([], schema=AGG_DAILY_SCHEMA)
    summary = compute_kpi_summary(fact, agg)
    assert summary["conversion_rate"] == 0.0
    assert summary["cart_abandonment"] == 0.0
    assert summary["aov_proxy"] == 0.0


# =============================================================================
# Funnel
# =============================================================================

def test_funnel_rates(spark: SparkSession):
    rows = [
        ("electronics", 100, 60, 30),
        ("fashion",     50,  20, 10),
    ]
    agg_funnel = spark.createDataFrame(rows, schema=AGG_FUNNEL_SCHEMA)
    result = funnel_by_category(agg_funnel)
    rows_out = {r["category_l1"]: r for r in result.collect()}

    elec = rows_out["electronics"]
    assert abs(elec["view_to_cart_rate"]    - 0.6)  < 0.001
    assert abs(elec["cart_to_purchase_rate"] - 0.5) < 0.001
    assert abs(elec["overall_conversion"]    - 0.3) < 0.001


# =============================================================================
# Cohort retention
# =============================================================================

def test_cohort_week0_is_full(spark: SparkSession):
    """
    Week 0 (cùng tuần mua đầu tiên) luôn có retention = 1.0.
    """
    from datetime import datetime

    rows = [
        # user 1: mua tuần 2019-10-07 (cohort), rồi mua lại tuần 2019-10-14
        (1, "s1", 0, 0, 1, 50.0, True, "2019-10-07", "cat"),
        (1, "s2", 0, 0, 1, 50.0, True, "2019-10-14", "cat"),
        # user 2: mua tuần 2019-10-07 chỉ một lần
        (2, "s3", 0, 0, 1, 30.0, True, "2019-10-07", "cat"),
    ]
    fact = spark.createDataFrame(rows, schema=FACT_SCHEMA)
    # Thêm cột session_start dạng timestamp
    fact = fact.withColumn("session_start", F.to_timestamp("session_start", "yyyy-MM-dd"))

    cohort = compute_cohort_retention(fact)
    week0 = cohort.filter(F.col("week_number") == 0).collect()

    assert len(week0) == 1
    assert week0[0]["retention_rate"] == 1.0
