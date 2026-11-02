"""Tiny deterministic E1 tests; no production lake and no third-party test runner."""
import csv
import tempfile
from pathlib import Path

from experiments import e1_scale_data as e1
from src.common.input_manifest import verify_silver_manifest, write_silver_manifest


def fixture(spark):
    return spark.createDataFrame([
        (1, 'purchase', 'a', 10.0), (2, 'purchase', 'a', 0.0),
        (3, 'purchase', 'a', None), (4, 'view', 'a', 90.0),
        (5, 'purchase', 'b', 5.0), (6, 'remove_from_cart', 'b', 9.0)],
        'id long, event_type string, category_l1 string, price double')


def test_fractions(spark):
    for value in [0, -0.1, 1.01, float('nan'), float('inf')]:
        try:
            e1.validate_fraction(value)
        except ValueError:
            pass
        else:
            raise AssertionError(value)
    for value in e1.FRACTIONS:
        e1.validate_fraction(value)


def test_fixed_seed_sampling_repartition(spark):
    source = spark.range(1000)
    a = e1.sample_input(source, 0.25, 42)
    b = e1.sample_input(source.repartition(3), 0.25, 42)
    assert a.exceptAll(b).count() == 0 and b.exceptAll(a).count() == 0
    assert a.count() > 0
    assert e1.sample_input(source, 1.0, 42).count() == 1000
    assert a.exceptAll(e1.sample_input(source, 0.50, 42)).count() == 0
    assert a.exceptAll(e1.sample_input(source, 0.25, 43)).count() > 0


def test_projection_preserves_full_schema_sampling(spark):
    from pyspark.sql import functions as F
    source = fixture(spark).crossJoin(spark.range(20).select(F.col('id').alias('unused_identity')))
    for fraction in [.25, 1.0]:
        expected = e1.sample_input(source, fraction, 42).select('event_type','category_l1','price')
        actual = e1.prepare_workload_input(source.repartition(3), fraction, 42)
        assert actual.columns == ['event_type','category_l1','price']
        assert actual.exceptAll(expected).count()==0 and expected.exceptAll(actual).count()==0
        assert e1.equivalent(e1.representative_job(expected),e1.representative_job(actual))


def test_tier_and_changed_dataset_rejected(spark):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory) / 'events'
        root.mkdir()
        data = root / 'part.parquet'
        data.write_bytes(b'fixture-metadata-only')
        write_silver_manifest(str(root), 'S')
        verify_silver_manifest(str(root), 'S')
        for tier, mutate in [('L', False), ('S', True)]:
            if mutate:
                data.write_bytes(b'changed-dataset')
            try:
                verify_silver_manifest(str(root), tier)
            except ValueError:
                pass
            else:
                raise AssertionError('mislabeled or changed dataset accepted')


def test_success_only_summary(spark):
    summary = e1.successful_summary([
        {'success': True, 'wall_time_s': n} for n in [1.0, 3.0, 8.0]] +
        [{'success': False, 'wall_time_s': 1000.0}])
    assert summary == {'successful_runs': 3, 'failed_runs': 1, 'median_s': 3.0, 'min_s': 1.0, 'max_s': 8.0}
    assert e1.successful_summary([{'success': False, 'wall_time_s': 1}])['median_s'] is None


def test_materialized_workload_values(spark):
    actual = e1.representative_job(fixture(spark))
    assert actual == [{'category_l1': 'a', 'revenue': 10.0, 'purchase_count': 3},
                      {'category_l1': 'b', 'revenue': 5.0, 'purchase_count': 1}]
    assert not e1.equivalent(actual, [{'category_l1': 'a', 'revenue': 0.0, 'purchase_count': 3}])


def test_empty_and_unavailable_metrics(spark):
    assert e1.representative_job(fixture(spark).limit(0)) == []
    metrics = e1._spark_metrics(spark)
    assert metrics['status'] == 'unavailable'
    assert all(metrics[c] is None for c in ['spill_bytes','shuffle_read_bytes','shuffle_write_bytes','peak_ram_bytes'])


def test_append_csv_preserves_previous_rows(spark):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'runs.csv'
        e1.append_record(path, {'run_id': 'prior', 'metrics': {'spill_bytes': None}})
        before = path.read_bytes()
        e1.append_record(path, {'run_id': 'next', 'error': 'comma, newline\nmessage'})
        assert path.read_bytes().startswith(before)
        with path.open(newline='', encoding='utf-8') as stream:
            assert [r['run_id'] for r in csv.DictReader(stream)] == ['prior', 'next']


def test_cache_cleanup_success_and_failure(spark):
    source = fixture(spark)
    before = spark.sparkContext._jsc.getPersistentRDDs().size()
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory)
        records = e1.run_fraction(spark, source, 1.0, 42, 1, output, {})
        assert records[0]['success'] and records[0]['actual_input_rows'] == 6
        assert records[0]['metrics']['status'] == 'measured', records[0]['metrics']
        assert records[0]['metrics']['job_group'] == records[0]['run_id']
        assert records[0]['metrics']['shuffle_write_bytes'] is not None
        assert all(s[0] > records[0]['metrics']['pre_run_stage_watermark'] for s in records[0]['metrics']['stage_attempts'])
        calls = []
        def fail_measured(df):
            calls.append(1)
            if len(calls) > 1:
                raise RuntimeError('injected measured failure')
            return e1.representative_job(df)
        records = e1.run_fraction(spark, source, 1.0, 42, 3, output, {}, fail_measured)
        assert len(records) == 1 and len(calls) == 2, 'must stop after first measured failure'
        assert not records[0]['success'] and records[0]['throughput_rows_s'] is None
        assert 'injected' in records[0]['error']
        assert e1.successful_summary(records)['successful_runs'] == 0
        def fail_warmup(df):
            raise RuntimeError('injected warmup failure')
        try:
            e1.run_fraction(spark, source, 1.0, 42, 1, output, {}, fail_warmup)
        except RuntimeError:
            pass
        else:
            raise AssertionError('warmup error swallowed')
    assert spark.sparkContext._jsc.getPersistentRDDs().size() == before
