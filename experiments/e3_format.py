"""
experiments/e3_format.py
Owner: M2
Reviewer: M1

E3 — Định dạng lưu trữ.
So sánh: CSV vs Parquet-snappy vs Parquet-zstd, có/không partition theo ngày.
3 truy vấn đại diện:
  Q1 — Quét toàn bộ (tổng sự kiện theo event_type)
  Q2 — Tổng hợp 1 cột (tổng doanh thu theo category_l1)
  Q3 — Lọc theo ngày cụ thể (predicate pushdown / partition pruning)

Kết quả ghi vào results/experiment_log.csv (cột experiment = E3_<format>_<query>).
Dung lượng từng định dạng ghi vào results/e3_sizes.csv.
"""
from __future__ import annotations

import argparse
import csv
import os
import shutil
import time
import uuid
import statistics
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

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
            # Bỏ file .crc / _SUCCESS — không phải dữ liệu
            if f.endswith(".crc") or f.startswith("_"):
                continue
            total += os.path.getsize(os.path.join(root, f))
    return total


def _file_count(path: str) -> int:
    return sum(
        1
        for _, _, files in os.walk(path)
        for f in files
        if not (f.endswith(".crc") or f.startswith("_"))
    )


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


# Schema của silver — dùng khi đọc CSV để so sánh công bằng
# (không inferSchema: tránh thêm 1 lượt quét; không đọc mọi cột thành string)
_CSV_SCHEMA: StructType | None = None


def _read(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    if fmt == "csv":
        reader = spark.read.option("header", "true")
        if _CSV_SCHEMA is not None:
            reader = reader.schema(_CSV_SCHEMA)
        return reader.csv(path)
    return spark.read.parquet(path)


def build_queries(filter_date: str) -> dict:
    return {
        "Q1": q1_full_scan,
        "Q2": q2_agg_one_col,
        "Q3": lambda spark, path, fmt: q3_filter_date(spark, path, fmt, filter_date),
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
        spark.catalog.clearCache()   # không để cache Spark làm sai lệch lần chạy sau
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
            notes = str(exc)[:500]
            log.error(f"{run_id} lỗi: {exc}")

        rec = RunRecord(
            run_id=run_id,
            experiment=exp_id,
            engine=f"spark_{fmt}",
            engine_version=spark.version,
            tier=tier,
            wall_time_s=wall,
            row_count=row_count,
            shuffle_partitions=int(spark.conf.get("spark.sql.shuffle.partitions")),
            aqe_enabled=spark.conf.get("spark.sql.adaptive.enabled") == "true",
            success=success,
            notes=notes,
        )
        write_record(rec)
        if success:
            times.append(wall)
        log.info(f"  {run_id} | wall={wall}s | ok={success}")

    if times:
        log.info(
            f"{exp_id} | median={statistics.median(times):.3f}s "
            f"| min={min(times):.3f}s | max={max(times):.3f}s"
        )


def run(tier: str = "M", filter_date: str = FILTER_DATE, n_runs: int = N_RUNS) -> None:
    global _CSV_SCHEMA
    cfg = load_config()
    paths = get_paths(cfg)
    silver_path = paths["silver"]
    results_dir = paths.get("results", "/app/results")
    tmp_base = results_dir + "/e3_tmp"

    spark = get_spark("E3_format")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Đọc silver một lần, thêm event_date nếu chưa có
    df = spark.read.parquet(silver_path)
    if "event_date" not in df.columns:
        df = df.withColumn("event_date", F.to_date(F.col("event_ts")))
    _CSV_SCHEMA = df.schema
    total_rows = df.count()

    queries = build_queries(filter_date)
    log.info(
        f"E3 bắt đầu | tier={tier} | rows={total_rows:,} | filter_date={filter_date} "
        f"| formats={[f[0] for f in FORMATS]}"
    )

    size_report = []

    for label, fmt, codec, partitioned in FORMATS:
        out_path = f"{tmp_base}/{label}"

        # Ghi ra định dạng (đo luôn thời gian ghi — chi phí của codec nén)
        t0 = time.perf_counter()
        size_bytes = write_format(df, out_path, fmt, codec, partitioned)
        write_s = round(time.perf_counter() - t0, 3)
        size_report.append({
            "tier": tier,
            "label": label,
            "format": fmt,
            "codec": codec or "none",
            "partitioned": partitioned,
            "rows": total_rows,
            "files": _file_count(out_path),
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / 1024 / 1024, 2),
            "write_time_s": write_s,
        })

        # Chạy 3 truy vấn × n lần
        for q_name, q_fn in queries.items():
            run_query(spark, label, out_path, fmt, q_name, q_fn, tier, n_runs)

    # Tỷ lệ nén so với CSV
    csv_size = next((r["size_bytes"] for r in size_report if r["label"] == "csv"), None)
    for r in size_report:
        r["ratio_vs_csv"] = round(csv_size / r["size_bytes"], 2) if csv_size and r["size_bytes"] else ""

    out_csv = Path(results_dir) / "e3_sizes.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(size_report[0].keys()))
        writer.writeheader()
        writer.writerows(size_report)

    log.info("=== Dung lượng theo định dạng ===")
    for r in size_report:
        log.info(
            f"  {r['label']:18s}: {r['size_mb']:>9.1f} MB | files={r['files']:>5} "
            f"| ghi {r['write_time_s']}s | nén {r['ratio_vs_csv']}× so với CSV"
        )
    log.info(f"  → {out_csv}")

    spark.stop()
    log.info("E3 hoàn thành. Vẽ biểu đồ: python experiments/plot.py --experiments E3")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E3 — Format comparison")
    parser.add_argument("--tier", default="M", choices=["S", "M", "L"])
    parser.add_argument("--filter-date", default=FILTER_DATE,
                        help="Ngày lọc cho Q3 (YYYY-MM-DD)")
    parser.add_argument("--runs", type=int, default=N_RUNS, help="Số lần chạy mỗi truy vấn")
    args = parser.parse_args()
    run(args.tier, filter_date=args.filter_date, n_runs=args.runs)
