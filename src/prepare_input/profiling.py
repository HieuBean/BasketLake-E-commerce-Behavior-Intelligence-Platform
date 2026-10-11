"""
src/prepare_input/profiling.py
Owner: M2

Profiling dữ liệu bronze: null rate, phân phối event_type, giá bất thường,
phân phối brand/category, timestamp format.
Kết quả ghi ra:
  - results/dq_report_bronze.csv    (null rates + stats)
  - results/event_type_dist.csv     (phân phối event_type)
  - results/price_stats.csv         (thống kê giá)

Chạy: spark-submit src/prepare_input/profiling.py --tier M
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.common.spark_session import get_spark
from src.common.config import load_config, get_paths
from src.common.logger import get_logger

log = get_logger(__name__)
RESULTS = Path("/app/results")


# =============================================================================
# Các hàm profiling
# =============================================================================

def null_rate_report(df: DataFrame, total: int) -> list[dict]:
    """Tính null rate từng cột. Trả về list[dict] để ghi CSV."""
    rows = []
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        rows.append({
            "column":     col,
            "total":      total,
            "null_count": null_count,
            "null_rate":  round(null_count / total * 100, 4),
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


# =============================================================================
# Ghi CSV
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


def _write_spark_csv(df: DataFrame, name: str) -> None:
    out = str(RESULTS / name)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)
    log.info(f"  → {out}/")


# =============================================================================
# Main
# =============================================================================

def profile_bronze(tier: str) -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    bronze_path = paths["bronze"]

    spark = get_spark(f"profiling_{tier}")
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
    et_df = event_type_distribution(df)
    et_df.show(truncate=False)
    _write_spark_csv(et_df, "event_type_dist")

    # 3. Price statistics
    log.info("3. Thống kê price:")
    price_df = price_statistics(df)
    price_df.show(truncate=False)
    price_rows = [row.asDict() for row in price_df.collect()]
    _write_csv(price_rows, "price_stats.csv")

    # 4. Brand top 20
    log.info("4. Top 20 brand (null = thiếu nhãn):")
    brand_df = brand_stats(df)
    brand_df.show(truncate=False)

    # 5. Category top 20
    log.info("5. Top 20 category_code:")
    cat_df = category_stats(df)
    cat_df.show(truncate=False)

    # 6. Timestamp sample
    log.info("6. Mẫu event_time (kiểm tra định dạng):")
    samples = timestamp_sample(df)
    for s in samples:
        log.info(f"   '{s}'")
    log.info("   → Dùng định dạng này khi viết try_to_timestamp() trong R3")

    # 7. Tổng kết
    log.info("=== Tổng kết profiling ===")
    for r in null_rows:
        if r["null_rate"] > 0:
            log.info(f"  ⚠ {r['column']}: {r['null_rate']:.3f}% null → cần xử lý ở silver")

    spark.stop()
    log.info("Profiling xong. Kết quả tại results/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Profiling bronze layer")
    parser.add_argument("--tier", default="M", choices=["S", "M", "L"])
    args = parser.parse_args()
    profile_bronze(args.tier)
