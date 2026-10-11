"""
src/common/metrics.py
Ghi một dòng CSV kết quả đo vào results/experiment_log.csv.
Dùng bởi harness thí nghiệm và pipeline jobs.
"""
from __future__ import annotations

import csv
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional

import subprocess


RESULTS_DIR = Path(__file__).parent.parent.parent / "results"
LOG_FILE = RESULTS_DIR / "experiment_log.csv"

FIELDNAMES = [
    "run_id", "git_sha", "timestamp", "experiment",
    "engine", "engine_version", "tier", "row_count",
    "cores", "executors", "ram_gb",
    "shuffle_partitions", "aqe_enabled",
    "wall_time_s", "shuffle_read_bytes", "shuffle_write_bytes",
    "spill_bytes", "peak_ram_bytes", "success", "notes",
]


@dataclass
class RunRecord:
    run_id: str
    experiment: str
    engine: str
    tier: str
    wall_time_s: float
    git_sha: str = field(default_factory=lambda: _get_git_sha())
    timestamp: str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S"))
    engine_version: str = ""
    row_count: int = 0
    cores: int = 0
    executors: int = 0
    ram_gb: float = 0.0
    shuffle_partitions: int = 200
    aqe_enabled: bool = True
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    spill_bytes: int = 0
    peak_ram_bytes: int = 0
    success: bool = True
    notes: str = ""


def _get_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


def write_record(record: RunRecord) -> None:
    """Ghi RunRecord vào CSV (tạo file và header nếu chưa có)."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = LOG_FILE.exists()
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        row = asdict(record)
        # Chỉ giữ các key trong FIELDNAMES
        writer.writerow({k: row.get(k, "") for k in FIELDNAMES})


@contextmanager
def timed_run(run_id: str, experiment: str, engine: str, tier: str, **kwargs):
    """
    Context manager đo thời gian và tự ghi CSV.

    Dùng:
        with timed_run("E1-run1", "E1", "spark", "M") as rec:
            # ... chạy job ...
            rec.row_count = df.count()
    """
    record = RunRecord(
        run_id=run_id,
        experiment=experiment,
        engine=engine,
        tier=tier,
        wall_time_s=0.0,
        **kwargs,
    )
    start = time.perf_counter()
    try:
        yield record
        record.success = True
    except Exception as exc:
        record.success = False
        record.notes = str(exc)
        raise
    finally:
        record.wall_time_s = round(time.perf_counter() - start, 3)
        write_record(record)
