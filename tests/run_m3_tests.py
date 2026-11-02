"""Dependency-free M3 runner for Gold, E1 and read-only handoff audits."""

import argparse
import hashlib
import importlib
import inspect
import json
import math
import statistics
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / "results/m3/e1/task06b/recovery-2"


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for part in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()


def check():
    ready = read(ROOT / "data/lake/benchmark/e1/tier_l/tier-l-ready.manifest.json")
    final = read(RUN / "finalization.json")
    analysis = read(RUN / "analysis.json")
    validation = read(RUN / "validation/validation.json")
    assert ready["status"] == "READY FOR E1 BENCHMARK"
    assert final["status"] == "COMPLETE" and final["successful_measured_runs"] == 18
    assert len(ready["full_DQ_checks"]) == 26 and all(
        x["pass"] for x in ready["full_DQ_checks"].values()
    )
    assert ready["rows"] == {"bronze": 109950743, "silver": 109820004, "quarantine": 130739}
    assert ready["rows"]["bronze"] == ready["rows"]["silver"] + ready["rows"]["quarantine"]
    assert analysis["status"] == validation["status"] == "PASS"
    manifest = analysis["input_manifest"]
    records = analysis["records"]
    assert (
        manifest["source_metadata_unchanged"]
        and manifest["dataset_metadata_fingerprint"] == ready["silver_metadata_fingerprint"]
    )
    assert manifest["schema"] == ready["schemas"]["silver"]
    assert (
        manifest["code_sha256"]
        == digest(ROOT / "experiments/e1_scale_data.py")
        == digest(RUN / "e1_scale_data.executed.py")
    )
    assert len(records) == 18 and len({r["run_id"] for r in records}) == 18
    seen = set()
    for row in analysis["rows"]:
        group = [r for r in records if r["fraction"] == row["fraction"]]
        assert len(group) == 3 and all(r["success"] for r in group)
        assert len({r["actual_input_rows"] for r in group}) == 1
        times = [r["wall_time_s"] for r in group]
        for name, value in [
            ("mean_s", statistics.mean(times)),
            ("median_s", statistics.median(times)),
            ("std_s", statistics.stdev(times)),
        ]:
            assert math.isclose(value, row[name], rel_tol=0, abs_tol=1e-9), (row["fraction"], name)
        for r in group:
            assert r["wall_time_s"] > 0 and math.isclose(
                r["throughput_rows_s"], r["actual_input_rows"] / r["wall_time_s"]
            )
            assert (
                r["spark_config"] == manifest["resources"]
                and r["application_id"] == final["official_application"]
            )
            assert r["output_rows"] == len(r["values"])
            metrics = r["metrics"]
            assert metrics["status"] == "measured" and metrics["failed_tasks"] == 0
            stage = {tuple(s) for s in metrics["stage_attempts"]}
            assert all(s[0] > metrics["pre_run_stage_watermark"] for s in stage)
            assert not stage.intersection(seen)
            seen.update(stage)
    for name, record in final["artifacts"].items():
        p = ROOT / name
        assert p.stat().st_size == record["bytes"] and digest(p) == record["sha256"], name
    gold = read(ROOT / "results/m3/delivery/gold-summary.json")
    assert gold["historical_checks"] == 45 and gold["all_pass"]
    assert "F.first(" not in (ROOT / "src/processing/silver_to_gold.py").read_text(encoding="utf-8")
    result = {
        "status": "PASS",
        "utc": datetime.now(timezone.utc).isoformat(),
        "official_runs": 18,
        "fractions": 6,
        "artifacts_verified": len(final["artifacts"]),
        "historical_gold_integration_checks": 45,
        "historical_TierL_DQ_checks": 26,
        "stats_recomputed": True,
        "stage_attribution_disjoint": True,
        "executed_code_sha256": manifest["code_sha256"],
        "dataset_write": False,
        "Spark_job_submitted": False,
        "backup_checksum": "NOT REVALIDATED",
    }
    return result


def check_delivery():
    package = ROOT / "results/m3/delivery"
    for name, expected in read(package / "checksums.json").items():
        assert digest(ROOT / name) == expected, name
    data = read(package / "e1-results.json")
    records = data["records"]
    assert len(records) == 18
    assert len({r["run_id"] for r in records}) == 18
    for point in data["summary"]:
        group = [r for r in records if r["fraction"] == point["fraction"]]
        assert len(group) == 3 and all(r["success"] for r in group)
        times = [r["wall_time_s"] for r in group]
        for name, value in [
            ("mean_s", statistics.mean(times)),
            ("median_s", statistics.median(times)),
            ("std_s", statistics.stdev(times)),
        ]:
            assert math.isclose(point[name], value, rel_tol=0, abs_tol=1e-9)
        assert all(
            math.isclose(r["throughput_rows_s"], r["actual_input_rows"] / r["wall_time_s"])
            for r in group
        )
    assert read(package / "tier-l-summary.json")["rows"]["silver"] == data["base_rows"] == 109820004
    assert read(package / "gold-summary.json")["all_pass"]
    return {
        "status": "PASS",
        "mode": "portable delivery, no Spark or lake access",
        "official_runs": 18,
        "fractions": 6,
        "statistics_recomputed": True,
    }


def run_spark(module_name):
    from pyspark.sql import SparkSession

    module = importlib.import_module("tests." + module_name)
    tests = sorted((name, fn) for name, fn in vars(module).items() if name.startswith("test_"))
    if not tests:
        raise RuntimeError(f"No tests discovered in {module_name}")
    for name, fn in tests:
        if (
            not inspect.isfunction(fn)
            or list(inspect.signature(fn).parameters) != ["spark"]
            or getattr(fn, "pytestmark", None)
            or hasattr(fn, "__wrapped__")
        ):
            raise RuntimeError(f"Unsupported pytest semantics: {module_name}.{name}")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("m3-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.eventLog.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    passed = failed = 0
    started = time.monotonic()
    try:
        for name, fn in tests:
            try:
                fn(spark)
                passed += 1
                print(f"PASS {module_name}.{name}", flush=True)
            except Exception:
                failed += 1
                print(f"FAIL {module_name}.{name}", flush=True)
                traceback.print_exc()
    finally:
        spark.stop()
    print(
        f"{module_name}: {passed} passed, {failed} failed, elapsed_s={time.monotonic()-started:.3f}"
    )
    return failed == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("suite", choices=["gold", "e1", "audit", "audit-delivery", "all"])
    args = parser.parse_args()
    ok = True
    if args.suite in ("gold", "all"):
        ok = run_spark("test_gold") and ok
    if args.suite in ("e1", "all"):
        ok = run_spark("test_e1") and ok
    if args.suite in ("audit-delivery", "all"):
        print(json.dumps(check_delivery()))
    if args.suite in ("audit", "all"):
        print(json.dumps(check()))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
