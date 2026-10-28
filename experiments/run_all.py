"""
experiments/run_all.py
Entry point chạy tất cả thí nghiệm E1–E7.
Dùng: spark-submit experiments/run_all.py [--experiments E1 E2 ...]
"""
from __future__ import annotations

import argparse

from src.common.logger import get_logger

log = get_logger(__name__)

EXPERIMENTS = {
    "E1": ("experiments.e1_scale_data",    "run"),
    "E2": ("experiments.e2_scale_resource","run"),
    "E3": ("experiments.e3_format",        "run"),
    "E4": ("experiments.e4_tuning",        "run"),
    "E5": ("experiments.e5_crossover",     "run"),
    "E6": ("experiments.e6_correctness",   "run"),  # không cần Spark
    "E7": ("experiments.e7_mapreduce",     "run"),  # không cần Spark
}


def main(run_list: list[str], tier: str) -> None:
    import importlib
    for exp_id in run_list:
        if exp_id not in EXPERIMENTS:
            log.warning(f"Thí nghiệm {exp_id} không tồn tại, bỏ qua.")
            continue
        module_path, fn_name = EXPERIMENTS[exp_id]
        log.info(f"===== Bắt đầu {exp_id} =====")
        try:
            mod = importlib.import_module(module_path)
            fn = getattr(mod, fn_name)
            fn(tier) if exp_id in ("E1", "E2", "E3", "E4", "E5") else fn()
        except NotImplementedError as e:
            log.warning(f"{exp_id} chưa triển khai: {e}")
        except Exception as e:
            log.error(f"{exp_id} lỗi: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiments", nargs="+", default=list(EXPERIMENTS.keys()))
    parser.add_argument("--tier", default="L")
    args = parser.parse_args()
    main(args.experiments, args.tier)
