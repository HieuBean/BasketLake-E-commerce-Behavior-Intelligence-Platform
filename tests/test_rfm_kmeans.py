"""
tests/test_rfm_kmeans.py
Owner: M5 (Analytics 2)
Test tính năng RFM và phân cụm KMeans trên dữ liệu mẫu.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.analytics.rfm_kmeans import (
    assign_business_labels,
    build_rfm,
)
from src.processing.silver_to_gold import build_fact_sessions

FACT_SCHEMA = StructType([
    StructField("user_id",        LongType(),    False),
    StructField("user_session",   StringType(),  False),
    StructField("view_count",     IntegerType(), True),
    StructField("cart_count",     IntegerType(), True),
    StructField("purchase_count", IntegerType(), True),
    StructField("revenue",        DoubleType(),  True),
    StructField("converted",      BooleanType(), True),
    StructField("session_start",  StringType(),  True),
    StructField("category_l1",    StringType(),  True),
    StructField("last_purchase_ts", StringType(), True),
])


def test_recency_uses_last_purchase_even_when_session_crosses_midnight(spark: SparkSession):
    events = spark.createDataFrame([
        (7, "s1", "view", "2019-11-27 23:50:00", 0.0, 10, "electronics"),
        (7, "s1", "purchase", "2019-11-28 00:05:00", 25.0, 10, "electronics"),
    ], ["user_id", "user_session", "event_type", "event_ts", "price", "product_id", "category_l1"])
    fact = build_fact_sessions(events)
    row = fact.select("last_purchase_ts").first()
    assert row["last_purchase_ts"] == "2019-11-28 00:05:00"
    rfm = build_rfm(fact, as_of_date="2019-11-30").first()
    assert rfm["recency"] == 2


def test_build_rfm_filters_only_purchasers(spark: SparkSession):
    rows = [
        # User 1: 2 sessions, cả 2 đều mua
        (1, "s1", 2, 1, 1, 100.0, True, "2019-11-20 10:00:00", "electronics", "2019-11-20 11:00:00"),
        (1, "s2", 1, 1, 1,  50.0, True, "2019-11-25 10:00:00", "electronics", "2019-11-25 10:05:00"),
        # User 2: 1 session, mua 1 lần
        (2, "s3", 5, 2, 1, 200.0, True, "2019-11-27 23:50:00", "appliances", "2019-11-28 00:05:00"),
        # User 3: Chỉ view, không mua (converted = False)
        (3, "s4", 10, 0, 0,  0.0, False, "2019-11-29 10:00:00", "electronics", None),
    ]
    df = spark.createDataFrame(rows, schema=FACT_SCHEMA)
    rfm = build_rfm(df, as_of_date="2019-11-30")

    data = {row["user_id"]: row.asDict() for row in rfm.collect()}
    # User 3 không được có trong RFM
    assert 3 not in data
    assert 1 in data
    assert 2 in data

    # Kiểm tra User 1: 2 phiên mua, tổng tiền = 150
    assert data[1]["frequency"] == 2
    assert data[1]["monetary"] == 150.0
    # Mua lần cuối 2019-11-25 -> cách 2019-11-30 là 5 ngày
    assert data[1]["recency"] == 5

    # Phiên User 2 bắt đầu ngày 27, nhưng mua ngày 28 -> recency = 2.
    assert data[2]["frequency"] == 1
    assert data[2]["monetary"] == 200.0
    assert data[2]["recency"] == 2


def test_assign_business_labels(spark: SparkSession):
    PROFILE_SCHEMA = StructType([
        StructField("cluster",          IntegerType(), True),
        StructField("user_count",       LongType(),    True),
        StructField("median_recency",   DoubleType(),  True),
        StructField("median_frequency", DoubleType(),  True),
        StructField("median_monetary",  DoubleType(),  True),
    ])
    rows = [
        # Cụm VIP: Recency thấp (mới mua), Monetary cao
        (0, 50, 3.0, 5.0, 1000.0),
        # Cụm Ngủ đông: Recency cao, Monetary thấp
        (1, 100, 45.0, 1.0, 50.0),
    ]
    profiles_df = spark.createDataFrame(rows, schema=PROFILE_SCHEMA)
    labeled = assign_business_labels(profiles_df)

    pdf = labeled.toPandas()
    assert "segment_name" in pdf.columns
    assert len(pdf) == 2
    # Cụm 0 phải là VIP
    vip_row = pdf[pdf["cluster"] == 0].iloc[0]
    assert "VIP" in vip_row["segment_name"]
    other_row = pdf[pdf["cluster"] == 1].iloc[0]
    assert "Other buyers" in other_row["segment_name"]
