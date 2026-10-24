"""
src/processing/bronze_to_silver.py
Owner: B (Processing 1) — luật R1–R9
Reviewer: A

Bronze → Silver: làm sạch, chuẩn hóa, dedupe, phân loại quarantine.
Ghi DQ report ra results/dq_report_silver.csv.

Chạy:
    spark-submit src/processing/bronze_to_silver.py --tier M
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

from src.common.config import get_paths, load_config
from src.common.input_manifest import write_silver_manifest
from src.common.logger import get_logger
from src.common.spark_session import get_spark

log = get_logger(__name__)

# Tập event_type hợp lệ — xác nhận lại bằng profiling trước khi dùng
VALID_EVENT_TYPES = {"view", "cart", "purchase", "remove_from_cart"}


# =============================================================================
# Các luật làm sạch (R1–R9)
# Mỗi luật trả về (df_clean, df_quarantine_slice)
# =============================================================================

def apply_r1_null_keys(df: DataFrame):
    """R1: event_time, event_type, product_id, user_id không null."""
    mask = (
        F.col("event_time").isNull() |
        F.col("event_type").isNull() |
        F.col("product_id").isNull() |
        F.col("user_id").isNull()
    )
    return (
        df.filter(~mask),
        df.filter(mask).withColumn("reject_reason", F.lit("null_key_field")),
    )


def apply_r2_event_type(df: DataFrame, valid_types: set[str]):
    """R2: event_type thuộc tập hợp lệ."""
    mask = ~F.col("event_type").isin(list(valid_types))
    return (
        df.filter(~mask),
        df.filter(mask).withColumn("reject_reason", F.lit("invalid_event_type")),
    )


def apply_r3_parse_timestamp(df: DataFrame):
    """R3: Parse event_time → event_ts (UTC). Lỗi parse → quarantine."""
    df_with_ts = df.withColumn(
        "event_ts",
        F.try_to_timestamp(F.col("event_time"), F.lit("yyyy-MM-dd HH:mm:ss z")).cast(TimestampType()),
    )
    mask = F.col("event_ts").isNull()
    return (
        df_with_ts.filter(~mask).drop("event_time"),
        df_with_ts.filter(mask).drop("event_ts").withColumn("reject_reason", F.lit("timestamp_parse_error")),
    )


def apply_r4_price(df: DataFrame):
    """R4: price < 0 → quarantine; price == 0 → giữ nhưng gắn cờ."""
    quarantine = df.filter(F.col("price") < 0).withColumn("reject_reason", F.lit("negative_price"))
    clean = df.filter(~(F.col("price") < 0))
    clean = clean.withColumn("price_valid", F.col("price") > 0)
    return clean, quarantine


def apply_r5_brand(df: DataFrame):
    """R5: brand null → 'unknown', gắn cờ brand_missing."""
    df = df.withColumn("brand_missing", F.col("brand").isNull())
    df = df.withColumn("brand", F.coalesce(F.col("brand"), F.lit("unknown")))
    return df, None


def apply_r6_category(df: DataFrame):
    """R6: Tách category_code thành l1/l2/l3; thiếu → 'unknown'."""
    df = df.withColumn(
        "category_l1",
        F.coalesce(F.split(F.col("category_code"), r"\.").getItem(0), F.lit("unknown")),
    )
    df = df.withColumn(
        "category_l2",
        F.split(F.col("category_code"), r"\.").getItem(1),
    )
    df = df.withColumn(
        "category_l3",
        F.split(F.col("category_code"), r"\.").getItem(2),
    )
    return df, None


def apply_r7_dedupe(df: DataFrame):
    """R7: Xóa dòng trùng hoàn toàn, chuyển dòng trùng vào quarantine."""
    cols = [c for c in df.columns if c not in ("source_file", "ingest_ts")]
    window_spec = Window.partitionBy(cols).orderBy(F.lit(1))
    df_ranked = df.withColumn("_row_num", F.row_number().over(window_spec))
    clean = df_ranked.filter(F.col("_row_num") == 1).drop("_row_num")
    quarantine = (
        df_ranked.filter(F.col("_row_num") > 1)
        .drop("_row_num")
        .withColumn("reject_reason", F.lit("duplicate_row"))
    )
    return clean, quarantine


def apply_r8_out_of_month(df: DataFrame):
    """R8: Sự kiện ngoài tháng của file → gắn cờ (không quarantine)."""
    df = df.withColumn(
        "out_of_month",
        F.date_format(F.col("event_ts"), "yyyy-MM") != F.col("source_month"),
    )
    return df, None


def apply_r9_session(df: DataFrame, gap_minutes: int = 30):
    """R9: Sinh session_id bằng khoảng nghỉ khi user_session null."""
    # TODO: implement window-based sessionization cho trường hợp user_session null
    # Hiện tại giữ user_session gốc; sẽ so sánh với session sinh bằng gap ở gold
    return df, None


# =============================================================================
# Main
# =============================================================================

def enrich_columns(df: DataFrame):
    """Thêm các cột dẫn xuất cần thiết."""
    df = df.withColumn("event_date", F.to_date(F.col("event_ts")))
    df = df.withColumn("event_hour", F.hour(F.col("event_ts")))
    return df


def run(tier: str) -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    cleaning_cfg = cfg.get("cleaning", {})
    valid_types = set(cleaning_cfg.get("valid_event_types", list(VALID_EVENT_TYPES)))
    gap_min = cleaning_cfg.get("session_gap_minutes", 30)

    spark = get_spark(f"silver_{tier}")
    df = spark.read.parquet(paths["bronze"])
    total_in = df.count()
    log.info(f"Bronze in: {total_in:,}")

    quarantine_parts = []

    # R1
    df, q = apply_r1_null_keys(df)
    quarantine_parts.append(q)

    # R2
    df, q = apply_r2_event_type(df, valid_types)
    quarantine_parts.append(q)

    # R3
    df, q = apply_r3_parse_timestamp(df)
    quarantine_parts.append(q)

    # R4
    df, q = apply_r4_price(df)
    quarantine_parts.append(q)

    # R5 (không quarantine)
    df, _ = apply_r5_brand(df)

    # R6
    df, _ = apply_r6_category(df)

    # R7
    df, q = apply_r7_dedupe(df)
    quarantine_parts.append(q)

    # R8
    df, _ = apply_r8_out_of_month(df)

    # R9
    df, _ = apply_r9_session(df, gap_min)

    # Cột cuối
    df = enrich_columns(df)

    # Ghi silver
    log.info(f"Ghi silver → {paths['silver']}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(paths["silver"])
    )

    # Ghi quarantine
    from functools import reduce
    valid_q = [q for q in quarantine_parts if q is not None]
    if valid_q:
        quarantine = reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True),
            valid_q,
        )
    else:
        quarantine = df.limit(0).withColumn("reject_reason", F.lit(""))
    log.info(f"Ghi quarantine → {paths['quarantine']}")
    quarantine.write.mode("overwrite").parquet(paths["quarantine"])

    total_out = df.count()
    total_q = quarantine.count()
    log.info(f"Silver out: {total_out:,} | Quarantine: {total_q:,}")
    assert total_in == total_out + total_q, \
        f"Bất biến lỗi: {total_in} ≠ {total_out} + {total_q}"
    write_silver_manifest(paths["silver"], tier)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", default="S")
    args = parser.parse_args()
    run(args.tier)
