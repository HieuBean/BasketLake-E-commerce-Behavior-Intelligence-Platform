"""Analyze one official immutable batch; stdlib only, no shared CSV access."""

import argparse
import csv
import json
import math
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / "results/m3/e1/task06b/official-1"


def main():
    owner = json.loads((RUN / "runner.json").read_text())
    assert owner["status"].startswith("PASS") and owner["exit_code"] == 0
    folders = list((RUN / "measurements").iterdir())
    assert len(folders) == 1
    output = folders[0]
    manifest = json.loads((output / "manifest.json").read_text())
    records = [json.loads(p.read_text()) for p in output.glob("E1-*.json")]
    assert len(records) == 18 and all(r["success"] for r in records)
    assert manifest["base_rows"] == 109820004 and manifest["source_metadata_unchanged"]
    ready = json.loads(
        (ROOT / "data/lake/benchmark/e1/tier_l/tier-l-ready.manifest.json").read_text()
    )
    assert manifest["dataset_metadata_fingerprint"] == ready["silver_metadata_fingerprint"]
    validation = None
    if (RUN / "validation/validation.json").exists():
        validation = json.loads((RUN / "validation/validation.json").read_text())
        assert validation["status"] == "PASS"
        for key in [
            "spark.executor.memory",
            "spark.executor.cores",
            "spark.driver.memory",
            "spark.cores.max",
            "spark.sql.shuffle.partitions",
            "spark.sql.parquet.enableVectorizedReader",
            "spark.sql.parquet.columnarReaderBatchSize",
            "spark.sql.inMemoryColumnarStorage.batchSize",
        ]:
            assert validation["resources"][key] == manifest["resources"][key], key
    rows = []
    for f in [0.01, 0.05, 0.10, 0.25, 0.50, 1.0]:
        point = [r for r in records if r["fraction"] == f]
        assert len(point) == 3
        assert len({r["actual_input_rows"] for r in point}) == 1
        assert all(r["spark_config"] == manifest["resources"] for r in point)
        values = [r["wall_time_s"] for r in point]
        assert all(t > 0 and math.isfinite(t) for t in values)
        oracle = json.loads((output / f"sample-{f}.json").read_text())
        if validation and f in [0.25, 1.0]:
            reference = next(p["reference"] for p in validation["points"] if p["fraction"] == f)
            assert oracle["rows"] == reference["rows"]
        for r in point:
            a = {v["category_l1"]: v for v in r["values"]}
            b = {v["category_l1"]: v for v in oracle["oracle_values"]}
            assert a.keys() == b.keys()
            assert all(
                a[k]["purchase_count"] == b[k]["purchase_count"]
                and math.isclose(
                    a[k]["revenue"] or 0, b[k]["revenue"] or 0, rel_tol=1e-9, abs_tol=1e-6
                )
                for k in a
            )
        row = {
            "fraction": f,
            "input_rows": point[0]["actual_input_rows"],
            "successful_runs": 3,
            "purchase_count": point[0]["purchase_count"],
            "revenue": point[0]["revenue"],
            "median_s": statistics.median(values),
            "mean_s": statistics.mean(values),
            "std_s": statistics.stdev(values),
            "min_s": min(values),
            "max_s": max(values),
            "median_throughput_rows_s": statistics.median(r["throughput_rows_s"] for r in point),
            "time_range_ratio": max(values) / min(values),
            "variation_flag": max(values) / min(values) > 1.25,
            "all_runs_retained": True,
            "preparation_and_warmup_s": oracle["preparation_and_warmup_s"],
        }
        for key in [
            "shuffle_read_bytes",
            "shuffle_write_bytes",
            "spill_bytes",
            "memory_bytes_spilled",
            "jvm_gc_time_ms",
            "failed_tasks",
        ]:
            data = [r["metrics"].get(key) for r in point]
            row["median_" + key] = (
                statistics.median(data) if all(v is not None for v in data) else None
            )
        rows.append(row)
    full = rows[-1]
    small = rows[0]
    assert full["input_rows"] == ready["rows"]["silver"]
    assert full["purchase_count"] == ready["monetary"]["silver"]["purchases"]
    assert math.isclose(
        full["revenue"], ready["monetary"]["silver"]["revenue"], rel_tol=0, abs_tol=0.01
    )
    for row in rows:
        row["input_factor_vs_1pct"] = row["input_rows"] / small["input_rows"]
        row["time_factor_vs_1pct"] = row["median_s"] / small["median_s"]
        row["normalized_throughput_vs_1pct"] = (
            row["input_factor_vs_1pct"] / row["time_factor_vs_1pct"]
        )
        row["time_ratio_vs_full"] = row["median_s"] / full["median_s"]
        row["relative_time_speedup_vs_full"] = full["median_s"] / row["median_s"]
    result = {
        "status": "PASS",
        "manifest": str(output / "manifest.json"),
        "rows": rows,
        "application_id": manifest["resources"]["application_id"],
        "definitions": {
            "std_s": "sample standard deviation, n=3; no runs removed",
            "speedup": "T(100%)/T(f); different data sizes, NOT parallel speedup",
            "normalized_throughput": "(N(f)/N(1%))/(T(f)/T(1%)); fixed-resource data scaling, NOT strong-scaling efficiency",
            "variation_flag": "max/min >1.25 descriptive only; no exclusion",
            "memory_bytes_spilled": "cumulative deserialized spill estimate; not peak RAM",
        },
        "records": records,
        "input_manifest": manifest,
    }
    with (RUN / "analysis.json").open("x") as stream:
        json.dump(result, stream, indent=2)
    with (RUN / "summary.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({"status": "PASS", "rows": rows, "application_id": result["application_id"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", default=str(RUN))
    args = parser.parse_args()
    RUN = Path(args.run_dir).resolve()
    assert (ROOT / "results/m3/e1/task06b").resolve() in RUN.parents
    main()
