"""
src/analytics/run_analytics.py
Owner: D + E
Reviewer: A

Entry point chạy toàn bộ analytics từ gold tables.
Kết quả CSV ghi vào paths.results của cấu hình; phân khúc Parquet ghi vào paths.gold.

Chạy:
    spark-submit src/analytics/run_analytics.py --tier M
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from src.analytics.kpi import (
    compute_cohort_retention,
    compute_dau,
    compute_kpi_summary,
    funnel_by_category,
    hourly_pattern,
    revenue_by_category,
    revenue_by_day,
)
from src.analytics.rfm_kmeans import (
    assign_business_labels,
    build_rfm,
    check_kmeans_stability,
    cluster_profiles,
    plot_elbow_and_silhouette,
    run_kmeans,
)
from src.common.config import get_paths, load_config
from src.common.input_manifest import verify_silver_manifest, write_kpi_manifest
from src.common.logger import get_logger
from src.common.spark_session import get_spark

log = get_logger(__name__)
def _write_csv_spark(df, name: str, results_dir: Path) -> None:
    """Ghi DataFrame Spark ra CSV (single partition để dễ đọc)."""
    out = str(results_dir / name)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)
    log.info(f"→ {out}")


def _write_kpi(summary: dict, results_dir: Path) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    out = results_dir / "kpi_summary.csv"
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
    results_dir = Path(paths["results"])
    verify_silver_manifest(silver, tier)

    spark = get_spark(f"analytics_{tier}")
    results_dir.mkdir(parents=True, exist_ok=True)

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
    _write_kpi(summary, results_dir)
    write_kpi_manifest(results_dir, tier, silver)

    # ---- Time series ----
    log.info("=== Time series ===")
    _write_csv_spark(revenue_by_day(agg_daily),      "revenue_by_day", results_dir)
    _write_csv_spark(revenue_by_category(agg_daily), "revenue_by_category", results_dir)
    _write_csv_spark(hourly_pattern(agg_hourly),     "hourly_pattern", results_dir)

    # ---- DAU ----
    log.info("=== DAU ===")
    _write_csv_spark(compute_dau(silver_df), "dau", results_dir)

    # ---- Funnel ----
    log.info("=== Funnel ===")
    _write_csv_spark(funnel_by_category(agg_funnel), "funnel_by_category", results_dir)

    # ---- Cohort retention ----
    log.info("=== Cohort retention ===")
    try:
        cohort = compute_cohort_retention(fact_sessions)
        _write_csv_spark(cohort, "cohort_retention", results_dir)
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
            results_dir=results_dir,
        )
        segmented.write.mode("overwrite").parquet(f"{gold}/user_segments")
        profiles = cluster_profiles(segmented)
        profiles = assign_business_labels(profiles)
        profiles.show(truncate=False)
        _write_csv_spark(profiles, "cluster_profiles", results_dir)

        # Cluster IDs may be permuted between seeds; store complete profiles.
        with open(results_dir / "kmeans_scores.csv", newline="", encoding="utf-8") as scores_file:
            best = max(csv.DictReader(scores_file), key=lambda row: float(row["silhouette"]))
        stability = check_kmeans_stability(rfm, k=int(best["k"]))
        with open(results_dir / "kmeans_stability.csv", "w", newline="", encoding="utf-8") as stability_file:
            writer = csv.DictWriter(stability_file, fieldnames=list(stability[0]))
            writer.writeheader()
            writer.writerows(stability)

        try:
            plot_elbow_and_silhouette(
                scores_path=results_dir / "kmeans_scores.csv",
                output_path=Path(__file__).resolve().parents[2] / "docs" / "figures"
                / f"kmeans_elbow_silhouette{'_' + tier if tier != 'S' else ''}.png",
            )
        except Exception as e:
            log.warning(f"Chưa thể sinh biểu đồ Elbow/Silhouette: {e}")

    # ---- Unpersist ----
    for cached in [fact_sessions, agg_daily, agg_hourly, agg_funnel, silver_df]:
        cached.unpersist()

    spark.stop()
    log.info(f"Analytics hoàn thành. Kết quả: {results_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", default="S", choices=["S", "M", "L", "XL"])
    args = parser.parse_args()
    run(args.tier)
