"""E6 must report conversion-rate rounding error instead of hiding it."""

from experiments.e6_correctness import compare


def test_conversion_rate_report_preserves_oracle_precision():
    exact = 18361 / 886657
    passed, report = compare({"conversion_rate": exact}, {"conversion_rate": 0.0207})
    row = report[0]

    assert passed
    assert row["oracle_value"] == round(exact, 8)
    assert row["spark_value"] == 0.0207
    assert row["diff_abs"] > 0
    assert row["diff_pct"] == 0.0392
