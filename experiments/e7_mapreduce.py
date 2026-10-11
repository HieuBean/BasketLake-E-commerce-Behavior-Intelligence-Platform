"""
experiments/e7_mapreduce.py
Owner: A (cộng điểm)

E7 — MapReduce mô phỏng bằng Python (có/không combiner).
So sánh số intermediate records và thời gian với Spark.
Gắn nội dung Chương 3.
"""
from __future__ import annotations

import time
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

from src.common.logger import get_logger
from src.common.metrics import write_record, RunRecord

log = get_logger(__name__)


# ============ Map phase ============

def mapper(record: dict) -> list[tuple]:
    """Phát (category_l1, price) cho mỗi purchase."""
    if record.get("event_type") == "purchase" and record.get("price") is not None:
        return [(record["category_l1"], float(record["price"]))]
    return []


# ============ Combiner (tùy chọn) ============

def combiner(pairs: list[tuple]) -> list[tuple]:
    """Tổng cục bộ trước khi shuffle."""
    acc = defaultdict(float)
    for k, v in pairs:
        acc[k] += v
    return list(acc.items())


# ============ Reduce phase ============

def reducer(pairs: list[tuple]) -> dict:
    acc = defaultdict(float)
    for k, v in pairs:
        acc[k] += v
    return dict(acc)


# ============ Runner ============

def run_mr(silver_path: str, use_combiner: bool) -> tuple[float, int]:
    """Trả về (wall_time, intermediate_count)."""
    log.info(f"E7 MapReduce | combiner={use_combiner}")
    start = time.perf_counter()

    all_pairs = []
    dataset = pq.ParquetDataset(silver_path)
    for fragment in dataset.fragments:
        table = fragment.to_table(columns=["event_type", "category_l1", "price"])
        for batch in table.to_batches():
            records = batch.to_pydict()
            n = len(records["event_type"])
            local_pairs = []
            for i in range(n):
                rec = {
                    "event_type":  records["event_type"][i],
                    "category_l1": records["category_l1"][i] or "unknown",
                    "price":       records["price"][i],
                }
                local_pairs.extend(mapper(rec))

            if use_combiner:
                local_pairs = combiner(local_pairs)

            all_pairs.extend(local_pairs)

    intermediate_count = len(all_pairs)
    _ = reducer(all_pairs)
    wall = round(time.perf_counter() - start, 3)
    log.info(f"Intermediate pairs: {intermediate_count:,} | wall={wall}s")
    return wall, intermediate_count


def run(silver_path: str) -> None:
    for use_c in [False, True]:
        label = "with_combiner" if use_c else "no_combiner"
        t, n = run_mr(silver_path, use_c)
        write_record(RunRecord(
            run_id=f"E7_{label}",
            experiment="E7",
            engine="python_mr",
            tier="M",
            wall_time_s=t,
            notes=f"intermediate_pairs={n}",
        ))


if __name__ == "__main__":
    from src.common.config import load_config, get_paths
    cfg = load_config()
    paths = get_paths(cfg)
    run(paths["silver"])
