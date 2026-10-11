"""
src/analytics/run_analytics.py
Owner: D + E
Reviewer: A

Entry point chạy toàn bộ analytics từ gold tables.
Mọi kết quả ghi vào results/ (CSV) hoặc data/lake/gold/ (Parquet).

Chạy:
    spark-submit src/analytics/run_analytics.py --tier M
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from src.common.spark_session import get_spark
from src.common.config import load_config, get_paths
from src.common.logger import get_logger
from src.analytics.kpi import (
    compute_kpi_summary,
    revenue_by_day,
    revenue_by_category,
    hourly_pattern,
    funnel_by_category,
    compute_dau,
    compute_cohort_retention,
)
from src.analytics.rfm_kmeans import build_rfm, run_kmeans, cluster_profiles

log = get_logger(__name__)
RESULTS = Path("/app/results")


def _write_csv_spark(df, name: str) -> None:
    """Ghi DataFrame Spark ra CSV (single partition để dễ đọc)."""
    out = str(RESULTS / name)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)
    log.info(f"→ {out}")


def _write_kpi(summary: dict) -> None:
    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / "kpi_summary.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)
    log.info(f"→ {out}")


def run(tier: str) -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    analytics_cfg = cfg.get("analytics", {})
    gold = paths["gold"]
    silver = paths["silver"]

    spark = get_spark(f"analytics_{tier}")
    RESULTS.mkdir(exist_ok=True)

    # ---- Load gold tables ----
    fact_sessions = spark.read.parquet(f"{gold}/fact_sessions").cache()
    agg_daily     = spark.read.parquet(f"{gold}/agg_daily").cache()
    agg_hourly    = spark.read.parquet(f"{gold}/agg_hourly").cache()
    agg_funnel    = spark.read.parquet(f"{gold}/agg_funnel").cache()
    silver_df     = spark.read.parquet(silver).cache()

    log.info(f"fact_sessions: {fact_sessions.count():,} | agg_daily: {agg_daily.count():,}")

    # ---- KPI Summary ----
    log.info("=== KPI Summary ===")
    summary = compute_kpi_summary(fact_sessions, agg_daily)
    _write_kpi(summary)

    # ---- Time series ----
    log.info("=== Time series ===")
    _write_csv_spark(revenue_by_day(agg_daily),      "revenue_by_day")
    _write_csv_spark(revenue_by_category(agg_daily), "revenue_by_category")
    _write_csv_spark(hourly_pattern(agg_hourly),     "hourly_pattern")

    # ---- DAU ----
    log.info("=== DAU ===")
    _write_csv_spark(compute_dau(silver_df), "dau")

    # ---- Funnel ----
    log.info("=== Funnel ===")
    _write_csv_spark(funnel_by_category(agg_funnel), "funnel_by_category")

    # ---- Cohort retention ----
    log.info("=== Cohort retention ===")
    try:
        cohort = compute_cohort_retention(fact_sessions)
        _write_csv_spark(cohort, "cohort_retention")
    except Exception as e:
        log.warning(f"Cohort bỏ qua (cần nhiều tháng dữ liệu): {e}")

    # ---- RFM + KMeans ----
    log.info("=== RFM + KMeans ===")
    as_of = analytics_cfg.get("rfm_as_of", "2019-11-30")
    k_min, k_max = analytics_cfg.get("kmeans_k_range", [2, 10])
    k_range = range(k_min, k_max + 1)

    rfm = build_rfm(fact_sessions, as_of)
    rfm_count = rfm.count()
    log.info(f"RFM: {rfm_count:,} users có purchase")

    if rfm_count < 10:
        log.warning("Quá ít user có purchase. Bỏ qua KMeans.")
    else:
        segmented = run_kmeans(
            rfm, k_range,
            seed=analytics_cfg.get("kmeans_seed", 42),
            max_iter=analytics_cfg.get("kmeans_max_iter", 20),
        )
        segmented.write.mode("overwrite").parquet(f"{gold}/user_segments")
        profiles = cluster_profiles(segmented)
        profiles.show(truncate=False)
        _write_csv_spark(profiles, "cluster_profiles")

    # ---- Unpersist ----
    for cached in [fact_sessions, agg_daily, agg_hourly, agg_funnel, silver_df]:
        cached.unpersist()

    spark.stop()
    log.info("Analytics hoàn thành. Kết quả: results/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", default="S", choices=["S", "M", "L", "XL"])
    args = parser.parse_args()
    run(args.tier)
