"""
experiments/e3_format.py
Owner: B
Reviewer: A

E3 — Định dạng lưu trữ.
So sánh: CSV vs Parquet-snappy vs Parquet-zstd, có/không partition theo ngày.
3 truy vấn đại diện:
  Q1 — Quét toàn bộ (tổng sự kiện theo event_type)
  Q2 — Tổng hợp 1 cột (tổng doanh thu theo category_l1)
  Q3 — Lọc theo ngày cụ thể (predicate pushdown / partition pruning)

Kết quả ghi vào results/experiment_log.csv (cột experiment = E3_<format>_<query>).
"""
from __future__ import annotations

import argparse
import os
import shutil
import time
import uuid
import statistics

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.common.spark_session import get_spark
from src.common.config import load_config, get_paths
from src.common.logger import get_logger
from src.common.metrics import RunRecord, write_record

log = get_logger(__name__)

N_RUNS = 3

# (label, format, codec, partitioned)
FORMATS = [
    ("csv",             "csv",     None,     False),
    ("parquet_snappy",  "parquet", "snappy", False),
    ("parquet_snappy_p","parquet", "snappy", True),   # partitioned by event_date
    ("parquet_zstd",    "parquet", "zstd",   False),
    ("parquet_zstd_p",  "parquet", "zstd",   True),
]

# Ngày lọc cho Q3 — lấy một ngày có nhiều dữ liệu (xác nhận bằng profiling)
FILTER_DATE = "2019-10-05"


# =============================================================================
# Ghi dữ liệu ra từng định dạng
# =============================================================================

def write_format(
    df: DataFrame,
    out_path: str,
    fmt: str,
    codec: str | None,
    partitioned: bool,
) -> int:
    """Ghi df ra định dạng chỉ định. Trả về dung lượng bytes."""
    if os.path.exists(out_path):
        shutil.rmtree(out_path)

    writer = df.write.mode("overwrite")

    if fmt == "csv":
        writer.option("header", "true").csv(out_path)
    else:
        if codec:
            writer = writer.option("compression", codec)
        if partitioned:
            writer = writer.partitionBy("event_date")
        writer.parquet(out_path)

    # Đo dung lượng
    size = _dir_size(out_path)
    log.info(f"Ghi {out_path} ({fmt}/{codec}/part={partitioned}): {size / 1024 / 1024:.1f} MB")
    return size


def _dir_size(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


# =============================================================================
# 3 truy vấn đại diện
# =============================================================================

def q1_full_scan(spark: SparkSession, path: str, fmt: str) -> int:
    """Q1: Quét toàn bộ — đếm sự kiện theo event_type."""
    df = _read(spark, path, fmt)
    return df.groupBy("event_type").count().collect().__len__()


def q2_agg_one_col(spark: SparkSession, path: str, fmt: str) -> int:
    """Q2: Tổng hợp 1 cột — tổng doanh thu theo category_l1."""
    df = _read(spark, path, fmt)
    return (
        df.filter(F.col("event_type") == "purchase")
          .groupBy("category_l1")
          .agg(F.sum("price").alias("revenue"))
          .count()
    )


def q3_filter_date(spark: SparkSession, path: str, fmt: str, date: str) -> int:
    """Q3: Lọc theo ngày — kiểm tra partition pruning."""
    df = _read(spark, path, fmt)
    return df.filter(F.col("event_date") == date).count()


def _read(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    if fmt == "csv":
        return spark.read.option("header", "true").csv(path)
    return spark.read.parquet(path)


QUERIES = {
    "Q1": q1_full_scan,
    "Q2": q2_agg_one_col,
    "Q3": lambda spark, path, fmt: q3_filter_date(spark, path, fmt, FILTER_DATE),
}


# =============================================================================
# Runner
# =============================================================================

def run_query(
    spark: SparkSession,
    label: str,
    path: str,
    fmt: str,
    query_name: str,
    query_fn,
    tier: str,
    n_runs: int,
) -> None:
    exp_id = f"E3_{label}_{query_name}"
    times = []

    for i in range(1, n_runs + 1):
        run_id = f"{exp_id}-run{i}-{uuid.uuid4().hex[:6]}"
        start = time.perf_counter()
        try:
            row_count = query_fn(spark, path, fmt)
            wall = round(time.perf_counter() - start, 3)
            success = True
            notes = ""
        except Exception as exc:
            wall = round(time.perf_counter() - start, 3)
            row_count = 0
            success = False
            notes = str(exc)
            log.error(f"{run_id} lỗi: {exc}")

        rec = RunRecord(
            run_id=run_id,
            experiment=exp_id,
            engine=f"spark_{fmt}",
            tier=tier,
            wall_time_s=wall,
            row_count=row_count,
            success=success,
            notes=notes,
        )
        write_record(rec)
        times.append(wall)
        log.info(f"  {run_id} | wall={wall}s")

    ok = [t for t in times]
    if ok:
        log.info(
            f"{exp_id} | median={statistics.median(ok):.3f}s "
            f"| min={min(ok):.3f}s | max={max(ok):.3f}s"
        )


def run(tier: str = "M") -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    silver_path = paths["silver"]
    tmp_base = paths.get("results", "/app/results") + "/e3_tmp"

    spark = get_spark("E3_format")

    # Đọc silver một lần, thêm event_date nếu chưa có
    df = spark.read.parquet(silver_path)
    if "event_date" not in df.columns:
        df = df.withColumn("event_date", F.to_date(F.col("event_ts")))

    log.info(f"E3 bắt đầu | tier={tier} | formats={[f[0] for f in FORMATS]}")

    size_report = []

    for label, fmt, codec, partitioned in FORMATS:
        out_path = f"{tmp_base}/{label}"

        # Ghi ra định dạng
        size_bytes = write_format(df, out_path, fmt, codec, partitioned)
        size_report.append({"label": label, "size_bytes": size_bytes})

        # Chạy 3 truy vấn × 3 lần
        for q_name, q_fn in QUERIES.items():
            run_query(spark, label, out_path, fmt, q_name, q_fn, tier, N_RUNS)

    # In tóm tắt dung lượng
    log.info("=== Dung lượng theo định dạng ===")
    for r in size_report:
        log.info(f"  {r['label']}: {r['size_bytes'] / 1024 / 1024:.1f} MB")

    spark.stop()
    log.info("E3 hoàn thành.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E3 — Format comparison")
    parser.add_argument("--tier", default="M", choices=["S", "M", "L"])
    parser.add_argument("--filter-date", default=FILTER_DATE,
                        help="Ngày lọc cho Q3 (YYYY-MM-DD)")
    args = parser.parse_args()
    FILTER_DATE = args.filter_date
    run(args.tier)
