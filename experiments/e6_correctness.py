"""
experiments/e6_correctness.py
Owner: A (bản tham chiếu oracle), E (đối chiếu)
Reviewer: D

E6 — Đối chiếu kết quả Spark với bản tham chiếu độc lập.
Dùng DuckDB (hoặc Pandas) tính 3 KPI trên tier S và M.
So sánh với kết quả từ Spark pipeline.
"""
from __future__ import annotations

import duckdb

from src.common.logger import get_logger

log = get_logger(__name__)


ORACLE_QUERIES = {
    "total_revenue": """
        SELECT ROUND(SUM(price), 2) AS total_revenue
        FROM read_parquet('{silver}/**/*.parquet')
        WHERE event_type = 'purchase'
    """,
    "total_sessions": """
        SELECT COUNT(DISTINCT user_session) AS total_sessions
        FROM read_parquet('{silver}/**/*.parquet')
    """,
    "conversion_rate": """
        SELECT
            ROUND(
                COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_session END) * 1.0
                / COUNT(DISTINCT user_session), 4
            ) AS conversion_rate
        FROM read_parquet('{silver}/**/*.parquet')
    """,
}


def run_oracle(silver_path: str) -> dict:
    con = duckdb.connect()
    results = {}
    for kpi, query in ORACLE_QUERIES.items():
        q = query.format(silver=silver_path)
        val = con.execute(q).fetchone()[0]
        results[kpi] = val
        log.info(f"Oracle {kpi}: {val}")
    return results


def compare(oracle: dict, spark_results: dict, tolerance: float = 0.001) -> bool:
    ok = True
    for kpi in oracle:
        o = oracle[kpi]
        s = spark_results.get(kpi)
        if s is None:
            log.warning(f"Spark không có kết quả cho {kpi}")
            ok = False
            continue
        diff = abs(o - s) / (abs(o) + 1e-9)
        status = "OK" if diff <= tolerance else "FAIL"
        log.info(f"{kpi}: oracle={o}, spark={s}, diff%={diff*100:.4f} [{status}]")
        if diff > tolerance:
            ok = False
    return ok


if __name__ == "__main__":
    from src.common.config import get_paths, load_config
    cfg = load_config()
    paths = get_paths(cfg)
    oracle = run_oracle(paths["silver"])
    # TODO: đọc kết quả Spark từ results/kpi_summary.csv và so sánh
