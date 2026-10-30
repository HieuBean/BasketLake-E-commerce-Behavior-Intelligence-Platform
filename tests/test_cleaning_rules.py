"""
tests/test_cleaning_rules.py
Owner: M2
Test từng luật R1–R9 + DQ report + bất biến toàn pipeline trên dữ liệu tay.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from src.processing.bronze_to_silver import (
    apply_r1_null_keys,
    apply_r2_event_type,
    apply_r3_parse_timestamp,
    apply_r4_price,
    apply_r5_brand,
    apply_r6_category,
    apply_r7_dedupe,
    apply_r8_out_of_month,
    apply_r9_session,
    build_dq_report,
    clean_pipeline,
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


def df_of(spark, *rows):
    return spark.createDataFrame(list(rows), schema=SCHEMA)


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


def test_r2_normalizes_case_and_whitespace(spark: SparkSession):
    df = df_of(spark, make_row(event_type="  Purchase "))
    clean, q = apply_r2_event_type(df, VALID_EVENT_TYPES)
    assert q.count() == 0
    assert clean.collect()[0]["event_type"] == "purchase"


# ---- R3 ----

def test_r3_parses_utc_timestamp(spark: SparkSession):
    df = df_of(spark, make_row(event_time="2019-10-05 13:45:10 UTC"))
    clean, q = apply_r3_parse_timestamp(df)
    assert q.count() == 0
    # So sánh bằng chuỗi format trong Spark (session TZ = UTC, xem conftest)
    # thay vì datetime Python (bị đổi theo múi giờ máy chạy test)
    from pyspark.sql import functions as F
    s = clean.select(F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss").alias("s")).first()["s"]
    assert s == "2019-10-05 13:45:10"


def test_r3_quarantines_bad_timestamp_without_crashing(spark: SparkSession):
    # ANSI mode bật: to_timestamp sẽ làm chết job, try_to_timestamp thì không
    df = df_of(spark, make_row(event_time="not-a-date"), make_row())
    clean, q = apply_r3_parse_timestamp(df)
    assert clean.count() == 1
    assert q.count() == 1
    assert q.collect()[0]["reject_reason"] == "timestamp_parse_error"


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


def test_r4_null_price_is_kept_not_lost(spark: SparkSession):
    df = df_of(spark, make_row(price=None))
    clean, q = apply_r4_price(df)
    assert clean.count() + q.count() == 1, "dòng price null bị mất ở cả 2 nhánh"
    assert clean.collect()[0]["price_valid"] == False  # noqa: E712


# ---- R5 ----

def test_r5_fills_null_brand(spark: SparkSession):
    df = spark.createDataFrame([make_row(brand=None)], schema=SCHEMA)
    clean, _ = apply_r5_brand(df)
    row = clean.collect()[0]
    assert row["brand"] == "unknown"
    assert row["brand_missing"] == True  # noqa: E712


def test_r5_keeps_existing_brand(spark: SparkSession):
    clean, _ = apply_r5_brand(df_of(spark, make_row(brand="apple")))
    row = clean.collect()[0]
    assert row["brand"] == "apple"
    assert row["brand_missing"] == False  # noqa: E712


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
    assert row["category_missing"] == True  # noqa: E712


def test_r6_two_levels_does_not_crash_under_ansi(spark: SparkSession):
    # getItem(2) trên mảng 2 phần tử sẽ ném INVALID_ARRAY_INDEX khi ANSI bật
    clean, _ = apply_r6_category(df_of(spark, make_row(category_code="appliances.kitchen")))
    row = clean.collect()[0]
    assert row["category_l2"] == "kitchen"
    assert row["category_l3"] is None


# ---- R7 ----

def test_r7_moves_duplicates_to_quarantine(spark: SparkSession):
    df = df_of(spark, make_row(), make_row(), make_row(), make_row(product_id=2))
    clean, q = apply_r7_dedupe(df)
    assert clean.count() == 2
    assert q.count() == 2
    assert {r["reject_reason"] for r in q.collect()} == {"duplicate_row"}


def test_r7_ignores_source_file_when_comparing(spark: SparkSession):
    df = df_of(spark, make_row(source_file="a.csv"), make_row(source_file="b.csv"))
    clean, q = apply_r7_dedupe(df)
    assert clean.count() == 1
    assert q.count() == 1


# ---- R8 ----

def test_r8_flags_out_of_month(spark: SparkSession):
    df = df_of(
        spark,
        make_row(event_time="2019-10-31 23:59:59 UTC"),
        make_row(event_time="2019-11-01 00:00:01 UTC"),
    )
    df, _ = apply_r3_parse_timestamp(df)
    out, _ = apply_r8_out_of_month(df)
    flags = sorted(r["out_of_month"] for r in out.collect())
    assert flags == [False, True]


# ---- R9 ----

def test_r9_keeps_original_session(spark: SparkSession):
    df, _ = apply_r3_parse_timestamp(df_of(spark, make_row(user_session="s1")))
    out, _ = apply_r9_session(df)
    row = out.collect()[0]
    assert row["session_id"] == "s1"
    assert row["session_generated"] == False  # noqa: E712


def test_r9_generates_sessions_by_gap(spark: SparkSession):
    df = df_of(
        spark,
        make_row(user_session=None, event_time="2019-10-01 10:00:00 UTC"),
        make_row(user_session=None, event_time="2019-10-01 10:20:00 UTC"),  # +20' → cùng phiên
        make_row(user_session=None, event_time="2019-10-01 11:30:00 UTC"),  # +70' → phiên mới
    )
    df, _ = apply_r3_parse_timestamp(df)
    out, _ = apply_r9_session(df, gap_minutes=30)
    rows = sorted(out.collect(), key=lambda r: r["event_ts"])
    ids = [r["session_id"] for r in rows]
    assert ids[0] == ids[1]
    assert ids[1] != ids[2]
    assert all(r["session_generated"] for r in rows)
    assert ids[0].startswith("gen_999_")


# ---- Toàn pipeline ----

def test_pipeline_invariant_and_quarantine_schema(spark: SparkSession):
    rows = [
        make_row(),                                   # sạch
        make_row(),                                   # trùng → R7
        make_row(user_id=None),                       # R1
        make_row(event_type="click"),                 # R2
        make_row(event_time="garbage"),               # R3
        make_row(price=-1.0),                         # R4 quarantine
        make_row(product_id=2, price=0.0),            # R4 flag
        make_row(product_id=3, price=None),           # price null → giữ
        make_row(product_id=4, brand=None, category_code=None),
        make_row(product_id=5, user_session=None),    # R9
        make_row(product_id=6, event_time="2019-11-02 00:00:00 UTC"),  # R8
    ]
    bronze = df_of(spark, *rows)
    silver, quarantine, _ = clean_pipeline(bronze)

    n_in, n_s, n_q = bronze.count(), silver.count(), quarantine.count()
    assert n_in == n_s + n_q, f"{n_in} != {n_s} + {n_q}"
    assert n_q == 5

    # Quarantine giữ đúng cột bronze + 2 cột lý do
    assert quarantine.columns == bronze.columns + ["reject_rule", "reject_reason"]
    reasons = {r["reject_rule"]: r["reject_reason"] for r in quarantine.collect()}
    assert reasons == {
        "R1": "null_key_field", "R2": "invalid_event_type",
        "R3": "timestamp_parse_error", "R4": "negative_price", "R7": "duplicate_row",
    }

    # Silver không còn event_time, có đủ cột dẫn xuất, session_id không null
    assert "event_time" not in silver.columns
    for c in ("event_ts", "event_date", "event_hour", "category_l1", "price_valid",
              "brand_missing", "out_of_month", "session_id"):
        assert c in silver.columns
    assert silver.filter(silver.session_id.isNull()).count() == 0


# ---- DQ report ----

def test_dq_report_chain_counts():
    rows = build_dq_report(
        total_in=1000,
        reject_counts={"R1": 10, "R3": 5, "R7": 20},
        flag_counts={"R5": 100},
    )
    by_rule = {r["rule"]: r for r in rows}
    assert by_rule["R1"]["rows_in"] == 1000 and by_rule["R1"]["rows_out"] == 990
    assert by_rule["R2"]["rows_in"] == 990 and by_rule["R2"]["rejected"] == 0
    assert by_rule["R3"]["rows_out"] == 985
    assert by_rule["R7"]["rows_out"] == 965
    assert by_rule["TOTAL"]["rows_out"] == 965
    assert by_rule["TOTAL"]["rejected"] == 35
    assert by_rule["R5"]["flagged"] == 100
