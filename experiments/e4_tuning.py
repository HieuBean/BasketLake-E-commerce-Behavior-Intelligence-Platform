"""
experiments/e4_tuning.py
Owner: C
Reviewer: B

E4 — Tối ưu Spark. Bốn trục thí nghiệm:
  T1: shuffle.partitions — [50, 100, 200, 400, 800]
  T2: AQE bật/tắt
  T3: Broadcast join vs Sort-Merge join (fact × dim_product)
  T4: Skew join — group by product_id "hot", có/không salting

Tác vụ cố định: Silver → group by product_id → sum revenue + count.
Đây là tác vụ skew tự nhiên (vài product_id "hot" có rất nhiều sự kiện).
"""
from __future__ import annotations

import argparse
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

# T1: danh sách shuffle partitions
SHUFFLE_PARTITIONS_LIST = [50, 100, 200, 400, 800]

# T4: số salt buckets cho salting
SALT_BUCKETS = 20


# =============================================================================
# Tác vụ chính (dùng chung cho T1 / T2)
# =============================================================================

def job_group_by_product(df: DataFrame) -> int:
    """
    Tác vụ wide: group by product_id (skew tự nhiên) → sum revenue.
    Trả về số nhóm.
    """
    return (
        df.filter(F.col("event_type") == "purchase")
          .groupBy("product_id", "category_l1")
          .agg(
              F.sum("price").alias("revenue"),
              F.count("*").alias("purchase_count"),
          )
          .count()
    )


# =============================================================================
# T3: Broadcast join vs Sort-Merge join
# =============================================================================

def job_broadcast_join(silver: DataFrame, dim_product: DataFrame) -> int:
    """Join fact (purchases) × dim_product với broadcast hint."""
    purchases = silver.filter(F.col("event_type") == "purchase")
    return (
        purchases.join(F.broadcast(dim_product), on="product_id", how="left")
                 .groupBy("brand")
                 .agg(F.sum("price").alias("revenue"))
                 .count()
    )


def job_sort_merge_join(silver: DataFrame, dim_product: DataFrame, spark: SparkSession) -> int:
    """Sort-Merge join: tắt broadcast bằng cách set threshold = -1."""
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    purchases = silver.filter(F.col("event_type") == "purchase")
    result = (
        purchases.join(dim_product, on="product_id", how="left")
                 .groupBy("brand")
                 .agg(F.sum("price").alias("revenue"))
                 .count()
    )
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # restore
    return result


# =============================================================================
# T4: Salting
# =============================================================================

def job_skew_no_salt(df: DataFrame) -> int:
    """Group by product_id — skew tự nhiên, không salting."""
    return (
        df.filter(F.col("event_type") == "purchase")
          .groupBy("product_id")
          .agg(F.sum("price").alias("revenue"))
          .count()
    )


def job_skew_with_salt(df: DataFrame, buckets: int) -> int:
    """
    Salting: thêm salt = rand() % buckets vào key, group 2 lần.
    Lần 1: group (product_id, salt) → tổng cục bộ
    Lần 2: group product_id → tổng toàn cục
    """
    salted = df.filter(F.col("event_type") == "purchase") \
               .withColumn("salt", (F.rand() * buckets).cast("int"))

    partial = (
        salted.groupBy("product_id", "salt")
              .agg(F.sum("price").alias("partial_revenue"))
    )
    return (
        partial.groupBy("product_id")
               .agg(F.sum("partial_revenue").alias("revenue"))
               .count()
    )


# =============================================================================
# Helpers đo thời gian
# =============================================================================

def _time_job(exp_id: str, tier: str, fn, engine_note: str = "", n_runs: int = N_RUNS) -> None:
    times = []
    for i in range(1, n_runs + 1):
        run_id = f"{exp_id}-run{i}-{uuid.uuid4().hex[:6]}"
        start = time.perf_counter()
        try:
            row_count = fn()
            wall = round(time.perf_counter() - start, 3)
            success = True
            notes = engine_note
        except Exception as exc:
            wall = round(time.perf_counter() - start, 3)
            row_count = 0
            success = False
            notes = str(exc)
            log.error(f"{run_id} lỗi: {exc}")

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
        log.info(f"  {run_id} | wall={wall}s")

    ok = [t for t in times]
    if ok:
        log.info(
            f"{exp_id} | median={statistics.median(ok):.3f}s "
            f"| min={min(ok):.3f}s | max={max(ok):.3f}s"
        )


# =============================================================================
# Entry point
# =============================================================================

def run(tier: str = "M") -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    silver_path = paths["silver"]
    gold_path = paths["gold"]

    spark = get_spark("E4_tuning")
    df = spark.read.parquet(silver_path).cache()
    log.info(f"Silver loaded: {df.count():,} dòng")

    # ---- T1: shuffle.partitions ----
    log.info("===== T1: shuffle.partitions =====")
    for sp in SHUFFLE_PARTITIONS_LIST:
        spark.conf.set("spark.sql.shuffle.partitions", str(sp))
        spark.conf.set("spark.sql.adaptive.enabled", "false")  # tắt AQE để so sánh thuần túy
        exp_id = f"E4_T1_sp{sp}"
        _time_job(exp_id, tier, lambda: job_group_by_product(df),
                  engine_note=f"shuffle_partitions={sp},aqe=false")

    # ---- T2: AQE bật/tắt (partitions = 200) ----
    log.info("===== T2: AQE bật/tắt =====")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    for aqe in [False, True]:
        spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
        exp_id = f"E4_T2_aqe{'on' if aqe else 'off'}"
        _time_job(exp_id, tier, lambda: job_group_by_product(df),
                  engine_note=f"shuffle_partitions=200,aqe={aqe}")

    # ---- T3: Broadcast vs Sort-Merge join ----
    log.info("===== T3: Broadcast vs Sort-Merge join =====")
    import os
    dim_path = f"{gold_path}/dim_product"
    if os.path.exists(dim_path):
        dim_product = spark.read.parquet(dim_path).cache()
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        _time_job(
            "E4_T3_broadcast", tier,
            lambda: job_broadcast_join(df, dim_product),
            engine_note="join=broadcast",
        )
        _time_job(
            "E4_T3_sortmerge", tier,
            lambda: job_sort_merge_join(df, dim_product, spark),
            engine_note="join=sort_merge",
        )
        dim_product.unpersist()
    else:
        log.warning(f"dim_product không tồn tại tại {dim_path}. Bỏ qua T3 — chạy gold trước.")

    # ---- T4: Skew — có/không salting ----
    log.info("===== T4: Skew, có/không salting =====")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    _time_job(
        "E4_T4_no_salt", tier,
        lambda: job_skew_no_salt(df),
        engine_note=f"salt=none,buckets=0",
    )
    _time_job(
        f"E4_T4_salt{SALT_BUCKETS}", tier,
        lambda: job_skew_with_salt(df, SALT_BUCKETS),
        engine_note=f"salt=random,buckets={SALT_BUCKETS}",
    )

    df.unpersist()
    spark.stop()
    log.info("E4 hoàn thành. Xem Spark History UI để phân tích task distribution.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E4 — Spark tuning experiment")
    parser.add_argument("--tier", default="M", choices=["S", "M", "L"])
    parser.add_argument("--salt-buckets", type=int, default=SALT_BUCKETS)
    args = parser.parse_args()
    SALT_BUCKETS = args.salt_buckets
    run(args.tier)
