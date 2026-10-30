"""The benchmark must reject a Silver dataset with the wrong tier or changed files."""
import pytest

from src.common.input_manifest import verify_silver_manifest, write_silver_manifest


def test_silver_manifest_rejects_wrong_tier_and_changed_input(tmp_path):
    silver = tmp_path / "silver" / "events"
    silver.mkdir(parents=True)
    sample = silver / "part-000.parquet"
    sample.write_bytes(b"first version")

    write_silver_manifest(str(silver), "S")
    verify_silver_manifest(str(silver), "S")

    with pytest.raises(ValueError, match="tier"):
        verify_silver_manifest(str(silver), "M")

    sample.write_bytes(b"second version with another length")
    with pytest.raises(ValueError, match="differ"):
        verify_silver_manifest(str(silver), "S")
