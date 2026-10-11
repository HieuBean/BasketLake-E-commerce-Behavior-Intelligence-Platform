"""
experiments/e5_crossover.py
Owner: E
Reviewer: A

E5 — Crossover: Pandas (chunk) vs DuckDB vs Spark (local + cluster).
Câu hỏi: từ kích thước nào Spark có lợi hơn?

Tác vụ cố định (giống E1 để so sánh công bằng):
  Filter purchase → group by category_l1 → sum price → order by revenue DESC

Các fraction: 5%, 10%, 25%, 50%, 100% của tier M (hoặc L).
Mỗi engine × fraction chạy 3 lần.
"""
from __future__ import annotations

import argparse
import time
import uuid
import statistics

import duckdb
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from src.common.spark_session import get_spark
from src.common.config import load_config, get_paths
from src.common.logger import get_logger
from src.common.metrics import RunRecord, write_record

log = get_logger(__name__)

N_RUNS = 3
FRACTIONS = [0.05, 0.10, 0.25, 0.50, 1.00]


# =============================================================================
# DuckDB
# =============================================================================

def run_duckdb(silver_path: str, fraction: float) -> tuple[float, int]:
    """
    DuckDB đọc trực tiếp Parquet (không cần load vào RAM trước).
    fraction: lấy mẫu bằng USING SAMPLE.
    """
    start = time.perf_counter()
    con = duckdb.connect()
    pct = fraction * 100
    rows = con.execute(f"""
        SELECT category_l1, SUM(price) AS revenue, COUNT(*) AS cnt
        FROM read_parquet('{silver_path}/**/*.parquet')
        WHERE event_type = 'purchase'
        USING SAMPLE {pct:.1f} PERCENT (bernoulli, 42)
        GROUP BY category_l1
        ORDER BY revenue DESC
    """).fetchall()
    wall = round(time.perf_counter() - start, 3)
    con.close()
    return wall, len(rows)


# =============================================================================
# Pandas (đọc Parquet theo chunk bằng PyArrow)
# =============================================================================

def run_pandas_chunk(silver_path: str, fraction: float) -> tuple[float, int]:
    """
    Đọc Parquet theo fragment (file-level chunk), lọc và tổng hợp.
    Không load toàn bộ vào RAM cùng lúc.
    """
    start = time.perf_counter()

    dataset = pq.ParquetDataset(silver_path, use_legacy_dataset=False)
    fragments = dataset.get_fragments()

    acc: dict[str, float] = {}
    total_files = 0

    for frag in fragments:
        total_files += 1
        # Chỉ đọc cột cần thiết (column pruning thủ công)
        table = frag.to_table(columns=["event_type", "category_l1", "price"])
        df_chunk: pd.DataFrame = table.to_pandas()

        # Lấy mẫu fraction
        df_chunk = df_chunk.sample(frac=fraction, random_state=42)

        purchases = df_chunk[df_chunk["event_type"] == "purchase"]
        grouped = purchases.groupby("category_l1")["price"].sum()
        for cat, rev in grouped.items():
            acc[cat] = acc.get(cat, 0.0) + rev

    wall = round(time.perf_counter() - start, 3)
    log.info(f"Pandas: đọc {total_files} fragment(s)")
    return wall, len(acc)


# =============================================================================
# Spark
# =============================================================================

def run_spark(silver_path: str, fraction: float, master: str = "local[*]") -> tuple[float, int]:
    from pyspark.sql import functions as F

    spark = get_spark(f"E5_spark_{master.replace(':', '_')}", extra_conf={"spark.master": master})
    start = time.perf_counter()

    df = spark.read.parquet(silver_path).sample(fraction=fraction, seed=42)
    count = (
        df.filter(F.col("event_type") == "purchase")
          .groupBy("category_l1")
          .agg(F.sum("price").alias("revenue"))
          .orderBy(F.desc("revenue"))
          .count()
    )
    wall = round(time.perf_counter() - start, 3)
    spark.stop()
    return wall, count


# =============================================================================
# Runner
# =============================================================================

ENGINES = {
    "duckdb":        run_duckdb,
    "pandas_chunk":  run_pandas_chunk,
    "spark_local":   lambda path, frac: run_spark(path, frac, "local[*]"),
    "spark_cluster": lambda path, frac: run_spark(path, frac, "spark://spark-master:7077"),
}


def _record(exp_id: str, engine: str, tier: str, wall: float, row_count: int,
            success: bool = True, notes: str = "") -> None:
    write_record(RunRecord(
        run_id=f"{exp_id}-{uuid.uuid4().hex[:6]}",
        experiment=exp_id,
        engine=engine,
        tier=tier,
        wall_time_s=wall,
        row_count=row_count,
        success=success,
        notes=notes,
    ))


def run(tier: str = "M", engines: list[str] | None = None) -> None:
    cfg = load_config()
    paths = get_paths(cfg)
    silver_path = paths["silver"]

    selected = engines or list(ENGINES.keys())
    log.info(f"E5 bắt đầu | tier={tier} | engines={selected} | fractions={FRACTIONS}")

    for engine_name in selected:
        fn = ENGINES.get(engine_name)
        if fn is None:
            log.warning(f"Engine '{engine_name}' không tồn tại, bỏ qua.")
            continue

        for frac in FRACTIONS:
            exp_id = f"E5_{engine_name}_frac{int(frac * 100):03d}"
            times = []

            for _ in range(N_RUNS):
                try:
                    wall, row_count = fn(silver_path, frac)
                    success = True
                    notes = ""
                except MemoryError as exc:
                    wall, row_count, success = 0.0, 0, False
                    notes = f"MemoryError: {exc}"
                    log.error(f"{exp_id} hết RAM: {exc}")
                except Exception as exc:
                    wall, row_count, success = 0.0, 0, False
                    notes = str(exc)
                    log.error(f"{exp_id} lỗi: {exc}")

                _record(exp_id, engine_name, tier, wall, row_count, success, notes)
                times.append(wall)
                log.info(f"  {exp_id} | engine={engine_name} | frac={frac} | wall={wall}s")

            ok = [t for t in times if t > 0]
            if ok:
                log.info(
                    f"{exp_id} | median={statistics.median(ok):.3f}s "
                    f"| min={min(ok):.3f}s | max={max(ok):.3f}s"
                )

    log.info("E5 hoàn thành. Chạy experiments/plot.py để vẽ crossover chart.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E5 — Crossover experiment")
    parser.add_argument("--tier", default="M", choices=["S", "M", "L"])
    parser.add_argument(
        "--engines", nargs="+",
        default=list(ENGINES.keys()),
        choices=list(ENGINES.keys()),
        help="Engine(s) muốn chạy",
    )
    parser.add_argument(
        "--fractions", nargs="+", type=float, default=FRACTIONS,
    )
    args = parser.parse_args()
    FRACTIONS = args.fractions
    run(args.tier, args.engines)
