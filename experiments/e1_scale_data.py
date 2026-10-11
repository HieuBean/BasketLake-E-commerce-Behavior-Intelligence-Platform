"""
experiments/e1_scale_data.py
Owner: C
Reviewer: A

E1 — Tăng dữ liệu, cố định tài nguyên.
Fractions: 1%, 5%, 10%, 25%, 50%, 100% của tier L (và XL nếu đủ máy).
Đo: wall time, shuffle bytes, spill, row_count.

Tác vụ đại diện (giống nhau cho mọi fraction để so sánh công bằng):
  - Filter purchase
  - Group by category_l1
  - Sum price → revenue
  - Join với dim_product để thêm brand (wide transformation — gây shuffle)
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.common.spark_session import get_spark
from src.common.config import load_config, get_paths
from src.common.logger import get_logger
from src.common.metrics import RunRecord, write_record
import time, uuid, statistics

log = get_logger(__name__)

FRACTIONS = [0.01, 0.05, 0.10, 0.25, 0.50, 1.00]
N_RUNS = 3


# =============================================================================
# Helpers lấy Spark metrics
# =============================================================================

def _spark_metrics(spark: SparkSession) -> dict:
    """
    Lấy shuffle read/write bytes và spill từ SparkContext StatusTracker.
    Trả về dict rỗng nếu không có stage nào hoàn thành.
    """
    sc = spark.sparkContext
    stage_ids = sc.statusTracker().getActiveStageIds()
    all_ids = list(sc.statusTracker().getActiveJobIds())

    # Dùng accumulators phía Python vì PySpark không expose
    # toàn bộ metrics qua public API — thu từ UI REST nếu cần chi tiết
    return {}  # TODO W3: đọc từ Spark History REST API (GET /api/v1/applications/<id>/stages)


# =============================================================================
# Tác vụ đại diện
# =============================================================================

def representative_job(df: DataFrame) -> int:
    """
    Tác vụ đại diện cho E1:
    1. Filter purchase (narrow)
    2. Group by category_l1, sum price (wide — shuffle)
    3. Count kết quả (action)
    Trả về số dòng đầu vào để ghi vào RunRecord.
    """
    # Cache df để tránh tính lại khi đo row_count
    df.cache()
    row_count = df.count()  # action 1 — trigger cache

    _ = (
        df.filter(F.col("event_type") == "purchase")
          .groupBy("category_l1")
          .agg(
              F.sum("price").alias("revenue"),
              F.count("*").alias("purchase_count"),
          )
          .orderBy(F.desc("revenue"))
          .collect()                         # action 2 — tác vụ chính
    )

    df.unpersist()
    return row_count


# =============================================================================
# Runner một fraction
# =============================================================================

def run_fraction(
    spark: SparkSession,
    silver_path: str,
    tier: str,
    fraction: float,
    n_runs: int = N_RUNS,
) -> None:
    exp_id = f"E1_frac{int(fraction * 100):03d}"
    log.info(f"===== {exp_id} (fraction={fraction}) =====")

    times = []

    for i in range(1, n_runs + 1):
        run_id = f"{exp_id}-run{i}-{uuid.uuid4().hex[:6]}"

        # Đọc + sample mới mỗi lần (tránh cache từ lần trước)
        df = spark.read.parquet(silver_path).sample(fraction=fraction, seed=42 + i)

        start = time.perf_counter()
        try:
            row_count = representative_job(df)
            wall = round(time.perf_counter() - start, 3)
            success = True
            notes = ""
        except Exception as exc:
            wall = round(time.perf_counter() - start, 3)
            row_count = 0
            success = False
            notes = str(exc)
            log.error(f"{run_id} thất bại: {exc}")

        rec = RunRecord(
            run_id=run_id,
            experiment=exp_id,
            engine="spark",
            tier=tier,
            wall_time_s=wall,
            row_count=row_count,
            success=success,
            notes=notes,
        )
        write_record(rec)
        times.append(wall)
        log.info(f"  {run_id} | rows={row_count:,} | wall={wall}s")

    # Tóm tắt
    ok = [t for t, s in zip(times, [True] * n_runs) if s]
    if ok:
        log.info(
            f"{exp_id} | median={statistics.median(ok):.3f}s "
            f"| min={min(ok):.3f}s | max={max(ok):.3f}s"
        )


# =============================================================================
# Entry point
# =============================================================================

def run(tier: str) -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    silver_path = paths["silver"]

    # Cấu hình cố định cho E1 (không thay đổi tài nguyên — đó là E2)
    spark = get_spark(
        "E1_scale_data",
        extra_conf={
            # Ghi lại cấu hình để trace trong experiment_log
            "spark.app.name": "E1_scale_data",
        },
    )

    log.info(f"E1 bắt đầu | tier={tier} | fractions={FRACTIONS}")
    log.info(f"Silver path: {silver_path}")

    for frac in FRACTIONS:
        run_fraction(spark, silver_path, tier, frac, N_RUNS)

    spark.stop()
    log.info("E1 hoàn thành. Kết quả: results/experiment_log.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E1 — Scale data experiment")
    parser.add_argument(
        "--tier",
        default="L",
        choices=["M", "L", "XL"],
        help="Tier dữ liệu (M/L/XL). Dùng L hoặc XL để thấy điểm gãy.",
    )
    parser.add_argument(
        "--fractions",
        nargs="+",
        type=float,
        default=FRACTIONS,
        help="Danh sách fraction muốn chạy (vd: 0.1 0.25 0.5 1.0)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=N_RUNS,
        help="Số lần chạy mỗi fraction (khuyến nghị 3)",
    )
    args = parser.parse_args()
    FRACTIONS = args.fractions
    N_RUNS = args.runs
    run(args.tier)
