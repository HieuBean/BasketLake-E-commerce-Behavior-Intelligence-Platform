"""
src/prepare_input/profiling.py
Owner: M2

Profiling dữ liệu bronze: null rate, phân phối event_type, giá bất thường,
phân phối brand/category, timestamp format, dòng trùng, sự kiện ngoài tháng.
Kết quả ghi ra (thư mục paths.results trong configs/pipeline.yaml):
  - dq_report_bronze_null.csv   (null rate từng cột)
  - event_type_dist.csv         (phân phối event_type)
  - price_stats.csv             (thống kê giá)
  - top_brand.csv, top_category.csv
  - profiling_summary.md        (tóm tắt — copy vào docs/log/m2-w1.md)

Chạy: spark-submit src/prepare_input/profiling.py --tier M
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_paths, load_config
from src.common.logger import get_logger
from src.common.spark_session import get_spark
from src.processing.bronze_to_silver import DEDUPE_KEYS, VALID_EVENT_TYPES, parse_event_time

log = get_logger(__name__)
RESULTS = Path("/app/results")


# =============================================================================
# Các hàm profiling
# =============================================================================

def null_rate_report(df: DataFrame, total: int) -> list[dict]:
    """Tính null rate (và chuỗi rỗng) của TẤT CẢ cột trong 1 lượt quét duy nhất."""
    exprs = []
    for col, dtype in df.dtypes:
        exprs.append(F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(f"{col}__null"))
        if dtype == "string":
            exprs.append(
                F.sum(F.when(F.trim(F.col(col)) == "", 1).otherwise(0)).alias(f"{col}__empty")
            )
    row = df.agg(*exprs).collect()[0].asDict()

    rows = []
    for col, dtype in df.dtypes:
        null_count = int(row.get(f"{col}__null") or 0)
        empty_count = int(row.get(f"{col}__empty") or 0) if dtype == "string" else 0
        rows.append({
            "column":      col,
            "dtype":       dtype,
            "total":       total,
            "null_count":  null_count,
            "empty_count": empty_count,
            "null_rate":   round(null_count / total * 100, 4) if total else 0.0,
        })
    return rows


def event_type_distribution(df: DataFrame) -> DataFrame:
    """Phân phối event_type."""
    return (
        df.groupBy("event_type")
          .count()
          .withColumnRenamed("count", "event_count")
          .orderBy(F.desc("event_count"))
    )


def price_statistics(df: DataFrame) -> DataFrame:
    """Thống kê price: min, max, avg, percentiles, số âm, số = 0."""
    return df.select(
        F.min("price").alias("price_min"),
        F.max("price").alias("price_max"),
        F.avg("price").alias("price_avg"),
        F.percentile_approx("price", 0.25).alias("price_p25"),
        F.percentile_approx("price", 0.50).alias("price_p50"),
        F.percentile_approx("price", 0.75).alias("price_p75"),
        F.percentile_approx("price", 0.99).alias("price_p99"),
        F.sum(F.when(F.col("price") < 0, 1).otherwise(0)).alias("count_negative"),
        F.sum(F.when(F.col("price") == 0, 1).otherwise(0)).alias("count_zero"),
        F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("count_null"),
    )


def brand_stats(df: DataFrame) -> DataFrame:
    """Top 20 brand + null brand."""
    return (
        df.groupBy("brand")
          .count()
          .orderBy(F.desc("count"))
          .limit(20)
    )


def category_stats(df: DataFrame) -> DataFrame:
    """Top 20 category_code + null."""
    return (
        df.groupBy("category_code")
          .count()
          .orderBy(F.desc("count"))
          .limit(20)
    )


def timestamp_sample(df: DataFrame, n: int = 5) -> list[str]:
    """Lấy mẫu event_time để kiểm tra định dạng."""
    return [row["event_time"] for row in df.select("event_time").limit(n).collect()]


def timestamp_quality(df: DataFrame) -> dict:
    """Số dòng không parse được, min/max event_ts, số sự kiện ngoài tháng của file."""
    parsed = df.withColumn("_ts", parse_event_time(F.col("event_time")))
    has_month = F.col("source_month").isNotNull() & (F.col("source_month") != "")
    row = parsed.agg(
        F.sum(F.when(F.col("_ts").isNull() & F.col("event_time").isNotNull(), 1)
              .otherwise(0)).alias("parse_errors"),
        F.min("_ts").alias("ts_min"),
        F.max("_ts").alias("ts_max"),
        F.sum(F.when(has_month & (F.date_format("_ts", "yyyy-MM") != F.col("source_month")), 1)
              .otherwise(0)).alias("out_of_month"),
    ).collect()[0]
    return {
        "parse_errors": int(row["parse_errors"] or 0),
        "ts_min": str(row["ts_min"]),
        "ts_max": str(row["ts_max"]),
        "out_of_month": int(row["out_of_month"] or 0),
    }


def cardinality(df: DataFrame) -> dict:
    """Số user/product/session/category khác nhau (xấp xỉ — HyperLogLog, rẻ)."""
    row = df.agg(
        F.approx_count_distinct("user_id").alias("users"),
        F.approx_count_distinct("product_id").alias("products"),
        F.approx_count_distinct("user_session").alias("sessions"),
        F.approx_count_distinct("category_code").alias("category_codes"),
        F.approx_count_distinct("brand").alias("brands"),
    ).collect()[0]
    return {k: int(v) for k, v in row.asDict().items()}


def duplicate_count(df: DataFrame, total: int) -> int:
    """Số dòng trùng hoàn toàn (sẽ bị R7 đưa vào quarantine)."""
    keys = [c for c in DEDUPE_KEYS if c in df.columns]
    distinct = df.select(*keys).distinct().count()
    return total - distinct


# =============================================================================
# Ghi kết quả
# =============================================================================

def _write_csv(data: list[dict], filename: str) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / filename
    if not data:
        return
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)
    log.info(f"  → {out}")


def _df_to_rows(df: DataFrame) -> list[dict]:
    """DataFrame nhỏ (đã groupBy/limit) → list[dict] để ghi 1 file CSV gọn."""
    return [r.asDict() for r in df.collect()]


def _write_summary_md(
    tier: str, total: int, null_rows: list[dict], et_rows: list[dict],
    price: dict, ts_q: dict, card: dict, dups: int, samples: list[str],
) -> None:
    def pct(n):
        return f"{n / total * 100:.4f}%" if total else "0%"

    unexpected = [r["event_type"] for r in et_rows if r["event_type"] not in VALID_EVENT_TYPES]
    lines = [
        f"# Profiling bronze — tier {tier}",
        "",
        f"- Tổng số dòng: **{total:,}**",
        f"- User ≈ {card['users']:,} | Product ≈ {card['products']:,} | "
        f"Session ≈ {card['sessions']:,} | Brand ≈ {card['brands']:,} | "
        f"Category code ≈ {card['category_codes']:,}",
        "",
        "## 1. event_type",
        "",
        "| event_type | count | % |",
        "|---|---:|---:|",
        *[f"| {r['event_type']} | {r['event_count']:,} | {pct(r['event_count'])} |" for r in et_rows],
        "",
        f"Giá trị lạ (ngoài {sorted(VALID_EVENT_TYPES)}): {unexpected or 'không có'}",
        "",
        "## 2–3. Null rate",
        "",
        "| Cột | Kiểu | Null | Rỗng | Null rate |",
        "|---|---|---:|---:|---:|",
        *[f"| {r['column']} | {r['dtype']} | {r['null_count']:,} | {r['empty_count']:,} | "
          f"{r['null_rate']:.4f}% |" for r in null_rows],
        "",
        "## 4. price",
        "",
        "| Chỉ số | Giá trị |",
        "|---|---:|",
        *[f"| {k} | {v} |" for k, v in price.items()],
        "",
        "## 5. event_time",
        "",
        "Mẫu: " + ", ".join(f"`{s}`" for s in samples),
        "",
        f"- Không parse được: {ts_q['parse_errors']:,} ({pct(ts_q['parse_errors'])})",
        f"- Khoảng thời gian: {ts_q['ts_min']} → {ts_q['ts_max']} (UTC)",
        f"- Ngoài tháng của file (R8): {ts_q['out_of_month']:,} ({pct(ts_q['out_of_month'])})",
        "",
        "## 6. Dòng trùng hoàn toàn (R7)",
        "",
        f"- {dups:,} dòng ({pct(dups)})",
        "",
    ]
    out = RESULTS / "profiling_summary.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info(f"  → {out}")


# =============================================================================
# Main
# =============================================================================

def profile_bronze(tier: str, skip_duplicates: bool = False) -> None:
    global RESULTS
    cfg = load_config()
    paths = get_paths(cfg)
    RESULTS = Path(paths.get("results", str(RESULTS)))
    bronze_path = paths["bronze"]

    spark = get_spark(f"profiling_{tier}")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df = spark.read.parquet(bronze_path)

    total = df.count()
    log.info(f"=== Profiling bronze | tier={tier} | tổng dòng={total:,} ===")

    # 1. Null rate
    log.info("1. Null rate từng cột:")
    null_rows = null_rate_report(df, total)
    for r in null_rows:
        log.info(f"   {r['column']:20s}: {r['null_rate']:6.3f}%  ({r['null_count']:,} dòng)")
    _write_csv(null_rows, "dq_report_bronze_null.csv")

    # 2. Event type distribution
    log.info("2. Phân phối event_type:")
    et_rows = _df_to_rows(event_type_distribution(df))
    for r in et_rows:
        log.info(f"   {str(r['event_type']):20s}: {r['event_count']:,}")
    _write_csv(et_rows, "event_type_dist.csv")

    # 3. Price statistics
    log.info("3. Thống kê price:")
    price = price_statistics(df).collect()[0].asDict()
    for k, v in price.items():
        log.info(f"   {k:15s}: {v}")
    _write_csv([price], "price_stats.csv")

    # 4. Brand top 20
    log.info("4. Top 20 brand (null = thiếu nhãn):")
    _write_csv(_df_to_rows(brand_stats(df)), "top_brand.csv")

    # 5. Category top 20
    log.info("5. Top 20 category_code:")
    _write_csv(_df_to_rows(category_stats(df)), "top_category.csv")

    # 6. Timestamp
    log.info("6. Mẫu event_time (kiểm tra định dạng):")
    samples = timestamp_sample(df)
    for s in samples:
        log.info(f"   '{s}'")
    ts_q = timestamp_quality(df)
    log.info(f"   parse lỗi={ts_q['parse_errors']:,} | {ts_q['ts_min']} → {ts_q['ts_max']} "
             f"| ngoài tháng={ts_q['out_of_month']:,}")

    # 7. Cardinality + duplicates
    card = cardinality(df)
    log.info(f"7. Cardinality: {card}")
    dups = 0 if skip_duplicates else duplicate_count(df, total)
    log.info(f"8. Dòng trùng hoàn toàn: {dups:,}")

    _write_summary_md(tier, total, null_rows, et_rows, price, ts_q, card, dups, samples)

    # Tổng kết
    log.info("=== Tổng kết profiling ===")
    for r in null_rows:
        if r["null_rate"] > 0:
            log.info(f"  ⚠ {r['column']}: {r['null_rate']:.3f}% null → cần xử lý ở silver")

    spark.stop()
    log.info(f"Profiling xong. Kết quả tại {RESULTS}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Profiling bronze layer")
    parser.add_argument("--tier", default="M", choices=["S", "M", "L", "XL"])
    parser.add_argument("--skip-duplicates", action="store_true",
                        help="Bỏ qua đếm dòng trùng (tốn 1 lần shuffle toàn bộ)")
    args = parser.parse_args()
    profile_bronze(args.tier, skip_duplicates=args.skip_duplicates)
