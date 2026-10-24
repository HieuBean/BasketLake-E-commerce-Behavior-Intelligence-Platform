"""Identify the input and tier used to produce an analytics result."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


def silver_fingerprint(path: str | Path) -> str:
    root = Path(path)
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Silver Parquet files in {root}")
    digest = hashlib.sha256()
    for file in files:
        stat = file.stat()
        record = [str(file.relative_to(root)), stat.st_size, stat.st_mtime_ns]
        digest.update(json.dumps(record, ensure_ascii=False).encode("utf-8"))
    return digest.hexdigest()


def silver_manifest_path(silver_path: str | Path) -> Path:
    root = Path(silver_path)
    return root.with_name(root.name + ".manifest.json")


def write_silver_manifest(silver_path: str, tier: str) -> None:
    manifest = {"tier": tier, "silver_fingerprint": silver_fingerprint(silver_path)}
    silver_manifest_path(silver_path).write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


def verify_silver_manifest(silver_path: str, tier: str) -> None:
    path = silver_manifest_path(silver_path)
    if not path.exists():
        raise ValueError(f"Missing {path}; rerun Silver for tier {tier}")
    actual = json.loads(path.read_text(encoding="utf-8"))
    expected = {"tier": tier, "silver_fingerprint": silver_fingerprint(silver_path)}
    if actual != expected:
        raise ValueError(f"Silver files or tier differ from manifest: {path}")


def write_kpi_manifest(results_dir: Path, tier: str, silver_path: str) -> None:
    manifest = {"tier": tier, "silver_fingerprint": silver_fingerprint(silver_path)}
    (results_dir / "kpi_summary.manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


def verify_kpi_manifest(results_dir: Path, tier: str, silver_path: str) -> None:
    path = results_dir / "kpi_summary.manifest.json"
    if not path.exists():
        raise ValueError(f"Missing {path}; rerun analytics for tier {tier}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    expected = {"tier": tier, "silver_fingerprint": silver_fingerprint(silver_path)}
    if manifest != expected:
        raise ValueError("KPI output tier or Silver input differs; rerun analytics before E6")
