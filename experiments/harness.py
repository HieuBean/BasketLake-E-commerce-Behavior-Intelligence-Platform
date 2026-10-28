"""
experiments/harness.py
Owner: A (Lead / Infra)

Framework đo thí nghiệm E1–E7.
Mỗi thí nghiệm là một hàm nhận spark + cấu hình và trả về dict kết quả.
Harness chạy mỗi cấu hình 3 lần, tính trung vị, ghi CSV.
"""
from __future__ import annotations

import statistics
import time
import uuid
from typing import Any, Callable

from src.common.logger import get_logger
from src.common.metrics import RunRecord, write_record

log = get_logger(__name__)


def run_experiment(
    experiment_id: str,
    fn: Callable[[], dict[str, Any]],
    n_runs: int = 3,
    **record_kwargs,
) -> list[RunRecord]:
    """
    Chạy fn() n_runs lần.
    fn phải trả về dict với ít nhất {"row_count": int}.
    Ghi một RunRecord cho mỗi lần chạy.
    """
    records = []
    times = []

    for i in range(1, n_runs + 1):
        run_id = f"{experiment_id}-run{i}-{uuid.uuid4().hex[:6]}"
        log.info(f"Bắt đầu {run_id}")
        start = time.perf_counter()
        try:
            result = fn()
            wall = round(time.perf_counter() - start, 3)
            rec = RunRecord(
                run_id=run_id,
                experiment=experiment_id,
                wall_time_s=wall,
                success=True,
                **{**record_kwargs, **{k: v for k, v in result.items() if hasattr(RunRecord, k)}},
            )
        except Exception as exc:
            wall = round(time.perf_counter() - start, 3)
            rec = RunRecord(
                run_id=run_id,
                experiment=experiment_id,
                wall_time_s=wall,
                success=False,
                notes=str(exc),
                **record_kwargs,
            )
            log.error(f"{run_id} thất bại: {exc}")

        write_record(rec)
        records.append(rec)
        times.append(wall)
        log.info(f"{run_id} | wall={wall}s")

    # Tóm tắt
    successful = [t for t, r in zip(times, records) if r.success]
    if successful:
        log.info(
            f"{experiment_id} | runs={n_runs} | "
            f"median={statistics.median(successful):.3f}s | "
            f"min={min(successful):.3f}s | max={max(successful):.3f}s"
        )
    return records
