"""
src/processing/bronze_to_silver.py
Owner: B (Processing 1) — luật R1–R9
Reviewer: A (Lead)

Bronze → Silver: làm sạch, chuẩn hóa, dedupe, phân loại quarantine.
Ghi DQ report ra results/dq_report_silver.csv.

Chạy:
    spark-submit src/processing/bronze_to_silver.py --tier M
"""
from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

from src.common.config import get_paths, load_config
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
    df = df.withColumn(
        "event_ts",
        F.try_to_timestamp(F.col("event_time"), F.lit("yyyy-MM-dd HH:mm:ss z")).cast(TimestampType()),
    )
    mask = F.col("event_ts").isNull()
    return (
        df.filter(~mask).drop("event_time"),
        df.filter(mask).withColumn("reject_reason", F.lit("timestamp_parse_error")),
    )


def apply_r4_price(df: DataFrame):
    """R4: price < 0 → quarantine; price == 0 hoặc null → giữ nhưng gắn cờ price_valid=False."""
    quarantine = df.filter(F.col("price") < 0).withColumn("reject_reason", F.lit("negative_price"))
    clean = df.filter(F.col("price").isNull() | (F.col("price") >= 0))
    clean = clean.withColumn(
        "price_valid",
        F.col("price").isNotNull() & (F.col("price") > 0),
    )
    return clean, quarantine


def apply_r5_brand(df: DataFrame):
    """R5: brand null → 'unknown', gắn cờ brand_missing."""
    df = df.withColumn("brand_missing", F.col("brand").isNull())
    df = df.withColumn("brand", F.coalesce(F.col("brand"), F.lit("unknown")))
    return df, None


def apply_r6_category(df: DataFrame):
    """R6: Tách category_code thành l1/l2/l3; thiếu → 'unknown' (dùng F.get tránh lỗi ANSI)."""
    parts = F.split(F.col("category_code"), r"\.")
    df = df.withColumn(
        "category_l1",
        F.coalesce(F.get(parts, 0), F.lit("unknown")),
    )
    df = df.withColumn(
        "category_l2",
        F.get(parts, 1),
    )
    df = df.withColumn(
        "category_l3",
        F.get(parts, 2),
    )
    return df, None


def apply_r7_dedupe(df: DataFrame):
    """R7: Xóa dòng trùng hoàn toàn, chuyển các bản ghi trùng vào quarantine."""
    cols = [c for c in df.columns if c not in ("source_file", "ingest_ts")]
    order_col = "ingest_ts" if "ingest_ts" in df.columns else F.lit(1)
    window = Window.partitionBy(cols).orderBy(order_col)
    df_numbered = df.withColumn("_row_num", F.row_number().over(window))

    clean = df_numbered.filter(F.col("_row_num") == 1).drop("_row_num")
    quarantine = (
        df_numbered.filter(F.col("_row_num") > 1)
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
    # Giữ user_session gốc; sẽ so sánh với session sinh bằng gap ở gold
    return df, None


# =============================================================================
# Main
# =============================================================================

def enrich_columns(df: DataFrame):
    """Thêm các cột dẫn xuất cần thiết."""
    df = df.withColumn("event_date", F.to_date(F.col("event_ts")))
    df = df.withColumn("event_hour", F.hour(F.col("event_ts")))
    return df


def _union_quarantine(parts: list[DataFrame | None]) -> DataFrame:
    """
    Gộp các phần quarantine dùng unionByName(allowMissingColumns=True).

    Các phần quarantine có schema khác nhau vì:
    - R1/R2: schema bronze gốc + reject_reason
    - R3: thêm event_ts, bỏ event_time + reject_reason
    - R4: thêm event_ts (từ R3), bỏ event_time + reject_reason
    - R7: có thêm category_l1/l2/l3, brand_missing, price_valid + reject_reason

    unionByName + allowMissingColumns giải quyết tất cả —
    cột thiếu sẽ tự động nhận giá trị null.
    """
    valid = [q for q in parts if q is not None]
    if not valid:
        raise ValueError("Không có quarantine nào — lỗi logic pipeline")

    result = valid[0]
    for q in valid[1:]:
        result = result.unionByName(q, allowMissingColumns=True)
    return result


def _write_dq_report(dq_records: list[dict], results_path: str) -> None:
    """Ghi báo cáo chất lượng dữ liệu DQ Report ra CSV."""
    p = Path(results_path)
    p.mkdir(parents=True, exist_ok=True)
    report_file = p / "dq_report_silver.csv"
    if not dq_records:
        return
    with open(report_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(dq_records[0].keys()))
        writer.writeheader()
        writer.writerows(dq_records)
    log.info(f"Đã lưu DQ report → {report_file}")


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
    dq_records = []
    current_df = df
    current_in = total_in

    # R1: Null key fields
    t0 = time.perf_counter()
    current_df, q = apply_r1_null_keys(current_df)
    cnt_out = current_df.count()
    cnt_q = q.count()
    dur = round(time.perf_counter() - t0, 2)
    quarantine_parts.append(q)
    dq_records.append({
        "rule": "R1", "rule_name": "null_key_field",
        "rows_in": current_in, "rows_out": cnt_out, "rows_quarantine": cnt_q,
        "quarantine_pct": round(cnt_q / max(current_in, 1) * 100, 4),
        "duration_seconds": dur,
    })
    current_in = cnt_out

    # R2: Invalid event types
    t0 = time.perf_counter()
    current_df, q = apply_r2_event_type(current_df, valid_types)
    cnt_out = current_df.count()
    cnt_q = q.count()
    dur = round(time.perf_counter() - t0, 2)
    quarantine_parts.append(q)
    dq_records.append({
        "rule": "R2", "rule_name": "invalid_event_type",
        "rows_in": current_in, "rows_out": cnt_out, "rows_quarantine": cnt_q,
        "quarantine_pct": round(cnt_q / max(current_in, 1) * 100, 4),
        "duration_seconds": dur,
    })
    current_in = cnt_out

    # R3: Timestamp parsing
    t0 = time.perf_counter()
    current_df, q = apply_r3_parse_timestamp(current_df)
    cnt_out = current_df.count()
    cnt_q = q.count()
    dur = round(time.perf_counter() - t0, 2)
    quarantine_parts.append(q)
    dq_records.append({
        "rule": "R3", "rule_name": "timestamp_parse_error",
        "rows_in": current_in, "rows_out": cnt_out, "rows_quarantine": cnt_q,
        "quarantine_pct": round(cnt_q / max(current_in, 1) * 100, 4),
        "duration_seconds": dur,
    })
    current_in = cnt_out

    # R4: Price cleaning
    t0 = time.perf_counter()
    current_df, q = apply_r4_price(current_df)
    cnt_out = current_df.count()
    cnt_q = q.count()
    dur = round(time.perf_counter() - t0, 2)
    quarantine_parts.append(q)
    dq_records.append({
        "rule": "R4", "rule_name": "negative_price",
        "rows_in": current_in, "rows_out": cnt_out, "rows_quarantine": cnt_q,
        "quarantine_pct": round(cnt_q / max(current_in, 1) * 100, 4),
        "duration_seconds": dur,
    })
    current_in = cnt_out

    # R5: Brand missing flag & coalesce
    t0 = time.perf_counter()
    current_df, _ = apply_r5_brand(current_df)
    dur = round(time.perf_counter() - t0, 2)
    dq_records.append({
        "rule": "R5", "rule_name": "brand_missing",
        "rows_in": current_in, "rows_out": current_in, "rows_quarantine": 0,
        "quarantine_pct": 0.0,
        "duration_seconds": dur,
    })

    # R6: Category level splitting
    t0 = time.perf_counter()
    current_df, _ = apply_r6_category(current_df)
    dur = round(time.perf_counter() - t0, 2)
    dq_records.append({
        "rule": "R6", "rule_name": "category_split",
        "rows_in": current_in, "rows_out": current_in, "rows_quarantine": 0,
        "quarantine_pct": 0.0,
        "duration_seconds": dur,
    })

    # R7: Deduplication
    t0 = time.perf_counter()
    current_df, q = apply_r7_dedupe(current_df)
    cnt_out = current_df.count()
    cnt_q = q.count()
    dur = round(time.perf_counter() - t0, 2)
    quarantine_parts.append(q)
    dq_records.append({
        "rule": "R7", "rule_name": "duplicate_row",
        "rows_in": current_in, "rows_out": cnt_out, "rows_quarantine": cnt_q,
        "quarantine_pct": round(cnt_q / max(current_in, 1) * 100, 4),
        "duration_seconds": dur,
    })
    current_in = cnt_out

    # R8: Out of month flag
    current_df, _ = apply_r8_out_of_month(current_df)

    # R9: Session check
    current_df, _ = apply_r9_session(current_df, gap_min)

    # Enrich columns
    df_silver = enrich_columns(current_df)

    # Ghi silver
    log.info(f"Ghi silver → {paths['silver']}")
    (
        df_silver.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(paths["silver"])
    )

    # Ghi quarantine — dùng unionByName để xử lý schema khác nhau
    quarantine = _union_quarantine(quarantine_parts)
    log.info(f"Ghi quarantine → {paths['quarantine']}")
    quarantine.write.mode("overwrite").parquet(paths["quarantine"])

    # Ghi DQ Report
    _write_dq_report(dq_records, paths.get("results", "/app/results"))

    total_out = df_silver.count()
    total_q = quarantine.count()
    log.info(f"Silver out: {total_out:,} | Quarantine: {total_q:,} | Tổng: {total_out + total_q:,}")
    assert total_in == total_out + total_q, \
        f"Bất biến lỗi: {total_in} ≠ {total_out} + {total_q}"
    log.info("Bất biến bảo toàn thành công: count(bronze) == count(silver) + count(quarantine)!")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", default="S")
    args = parser.parse_args()
    run(args.tier)
