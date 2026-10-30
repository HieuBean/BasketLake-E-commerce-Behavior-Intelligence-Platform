"""E5: compare the same aggregation on identical materialized Parquet samples."""
from __future__ import annotations

import argparse
import statistics
import tempfile
import time
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from src.common.config import get_paths, load_config
from src.common.input_manifest import verify_silver_manifest
from src.common.logger import get_logger
from src.common.metrics import RunRecord, write_record
from src.common.spark_session import get_spark

log = get_logger(__name__)
N_RUNS = 3
FRACTIONS = [0.05, 0.10, 0.25, 0.50, 1.00]


def prepare_sample(silver_path: str, fraction: float, output: Path) -> int:
    """Create one seeded sample outside the timed region for every engine."""
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be in (0, 1]")
    source = str(Path(silver_path) / "**" / "*.parquet").replace("'", "''")
    target = str(output).replace("'", "''")
    sample = "" if fraction == 1 else f"USING SAMPLE {fraction * 100:.1f} PERCENT (bernoulli, 42)"
    with duckdb.connect() as con:
        con.execute(f"""COPY (
            SELECT event_type, category_l1, price
            FROM read_parquet('{source}') {sample}
        ) TO '{target}' (FORMAT PARQUET)""")
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{target}')").fetchone()[0]


def run_duckdb(path: str) -> tuple[float, dict]:
    start = time.perf_counter()
    with duckdb.connect() as con:
        rows = con.execute("""
            SELECT category_l1, SUM(price) AS revenue
            FROM read_parquet(?) WHERE event_type = 'purchase'
            GROUP BY category_l1 ORDER BY revenue DESC
        """, [path]).fetchall()
    return time.perf_counter() - start, dict(rows)


def run_pandas_chunk(path: str) -> tuple[float, dict]:
    import pyarrow.parquet as pq

    start = time.perf_counter()
    acc: dict = {}
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=65536, columns=["event_type", "category_l1", "price"]):
        frame = batch.to_pandas()
        purchases = frame.loc[frame["event_type"] == "purchase"]
        for category, revenue in purchases.groupby("category_l1", dropna=False)["price"].sum().items():
            key = None if pd.isna(category) else category
            acc[key] = acc.get(key, 0.0) + float(revenue)
    return time.perf_counter() - start, dict(sorted(acc.items(), key=lambda item: -item[1]))


def run_spark(path: str, master: str) -> tuple[float, dict]:
    from pyspark.sql import functions as F

    spark = get_spark(f"E5_{master.replace(':', '_')}", extra_conf={"spark.master": master})
    try:
        start = time.perf_counter()
        rows = (spark.read.parquet(path)
                .filter(F.col("event_type") == "purchase")
                .groupBy("category_l1")
                .agg(F.sum("price").alias("revenue"))
                .orderBy(F.desc("revenue"))
                .collect())  # forces SUM and ORDER BY to execute
        wall = time.perf_counter() - start
        return wall, {row["category_l1"]: row["revenue"] for row in rows}
    finally:
        spark.stop()


ENGINES = {
    "duckdb": run_duckdb,
    "pandas_chunk": run_pandas_chunk,
    "spark_local": lambda path: run_spark(path, "local[*]"),
    "spark_cluster": lambda path: run_spark(path, "spark://spark-master:7077"),
}


def assert_same_result(actual: dict, expected: dict) -> None:
    if actual.keys() != expected.keys():
        raise AssertionError(f"Different categories: {actual.keys() ^ expected.keys()}")
    for category, reference in expected.items():
        value = actual[category]
        if abs(value - reference) > max(0.01, abs(reference) * 1e-8):
            raise AssertionError(f"Revenue mismatch for {category}: {value} != {reference}")


def run(tier: str = "M", engines: list[str] | None = None,
        fractions: list[float] | None = None, silver_path: str | None = None) -> None:
    paths = get_paths(load_config())
    source = silver_path or paths["silver"]
    if not Path(source).exists():
        raise FileNotFoundError(f"Silver path does not exist: {source}")
    verify_silver_manifest(source, tier)
    selected = engines or list(ENGINES)
    for name in selected:
        if name not in ENGINES:
            raise ValueError(f"Unknown engine: {name}")
    fractions = fractions or FRACTIONS
    results_dir = Path(paths["results"])
    results_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="e5_samples_", dir=results_dir) as temp:
        for fraction in fractions:
            sample_path = Path(temp) / f"sample_{int(fraction * 1000):04d}.parquet"
            input_rows = prepare_sample(source, fraction, sample_path)
            _, expected = run_duckdb(str(sample_path))
            log.info(f"E5 tier={tier} fraction={fraction} input_rows={input_rows}")
            for name in selected:
                times = []
                for _ in range(N_RUNS):
                    wall, groups, success, notes = 0.0, {}, False, ""
                    try:
                        wall, groups = ENGINES[name](str(sample_path))
                        assert_same_result(groups, expected)
                        success = True
                        times.append(wall)
                    except Exception as exc:
                        notes = str(exc)
                        log.error(f"E5 {name} fraction={fraction}: {exc}")
                    write_record(RunRecord(
                        run_id=f"E5-{uuid.uuid4().hex[:10]}",
                        experiment=f"E5_{name}_frac{int(fraction * 100):03d}",
                        engine=name, tier=tier, wall_time_s=round(wall, 3),
                        row_count=input_rows, success=success,
                        notes=f"validated_sample; output_groups={len(groups)}; {notes}",
                    ))
                if times:
                    log.info(f"E5 {name} {fraction:.0%}: median={statistics.median(times):.3f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E5 crossover experiment")
    parser.add_argument("--tier", default="M", choices=["S", "M", "L", "XL"])
    parser.add_argument("--engines", nargs="+", choices=list(ENGINES), default=list(ENGINES))
    parser.add_argument("--fractions", nargs="+", type=float, default=FRACTIONS)
    parser.add_argument("--silver-path", help="Explicit Silver Parquet directory (defaults to pipeline.yaml)")
    args = parser.parse_args()
    run(args.tier, args.engines, args.fractions, args.silver_path)
