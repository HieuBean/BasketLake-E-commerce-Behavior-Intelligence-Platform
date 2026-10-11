"""
experiments/e2_scale_resource.py
Owner: A
Reviewer: C

E2 — Tăng tài nguyên, cố định dữ liệu.
Configs: local[1], local[2], local[4], local[*], cluster 1/2/3 worker.
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from src.common.spark_session import get_spark
from src.common.config import load_config, get_paths
from src.common.logger import get_logger
from experiments.harness import run_experiment

log = get_logger(__name__)

MASTERS = [
    ("local[1]",               "local_1"),
    ("local[2]",               "local_2"),
    ("local[4]",               "local_4"),
    ("local[*]",               "local_star"),
    ("spark://spark-master:7077", "cluster"),  # số worker điều chỉnh bằng docker-compose
]


def e2_job(spark, silver_path: str) -> dict:
    df = spark.read.parquet(silver_path)
    count = (
        df.filter(F.col("event_type") == "purchase")
        .groupBy("category_l1")
        .agg(F.sum("price"))
        .count()
    )
    return {"row_count": count}


def run(tier: str) -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    silver_path = paths["silver"]

    for master_url, label in MASTERS:
        spark = get_spark(f"E2_{label}", extra_conf={"spark.master": master_url})
        run_experiment(
            experiment_id=f"E2_{label}",
            fn=lambda: e2_job(spark, silver_path),
            n_runs=3,
            engine="spark",
            tier=tier,
        )
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", default="L")
    args = parser.parse_args()
    run(args.tier)
