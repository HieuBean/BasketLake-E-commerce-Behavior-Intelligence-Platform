"""Validate saved RFM values against purchase events in Silver with DuckDB."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src.common.config import get_paths, load_config
from src.common.input_manifest import verify_silver_manifest


def validate(tier: str) -> dict:
    cfg = load_config()
    paths = get_paths(cfg)
    verify_silver_manifest(paths["silver"], tier)
    silver = str(Path(paths["silver"]) / "**" / "*.parquet").replace("'", "''")
    segments = str(Path(paths["gold"]) / "user_segments" / "*.parquet").replace("'", "''")
    as_of = cfg["analytics"]["rfm_as_of"]
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET temp_directory='/tmp/duckdb_rfm_check'")
    row = con.execute(
        f"""
        WITH oracle AS (
            SELECT user_id,
                   datediff('day', CAST(max(event_ts) AS DATE), DATE '{as_of}') AS recency,
                   count(DISTINCT user_session) AS frequency,
                   sum(price) AS monetary
            FROM read_parquet('{silver}')
            WHERE event_type = 'purchase'
            GROUP BY user_id
        ), saved AS (
            SELECT user_id, recency, frequency, monetary
            FROM read_parquet('{segments}')
        )
        SELECT count(*) AS compared,
               count(*) FILTER (WHERE o.user_id IS NULL OR s.user_id IS NULL) AS missing,
               count(*) FILTER (WHERE o.recency IS DISTINCT FROM s.recency) AS recency_diff,
               count(*) FILTER (WHERE o.frequency IS DISTINCT FROM s.frequency) AS frequency_diff,
               count(*) FILTER (WHERE abs(o.monetary-s.monetary) > 0.01) AS monetary_diff,
               round(max(abs(o.monetary-s.monetary)), 8) AS max_monetary_diff
        FROM oracle o FULL OUTER JOIN saved s USING (user_id)
        """
    ).fetchone()
    con.close()
    keys = ["compared", "missing", "recency_diff", "frequency_diff", "monetary_diff", "max_monetary_diff"]
    return dict(zip(keys, row))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", choices=["S", "M", "L", "XL"], required=True)
    args = parser.parse_args()
    report = validate(args.tier)
    print(report)
    if any(report[key] for key in ("missing", "recency_diff", "frequency_diff", "monetary_diff")):
        raise SystemExit(1)
