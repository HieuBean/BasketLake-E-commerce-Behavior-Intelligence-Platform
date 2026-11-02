"""E1 compute-only Method B: fixed hash sample, materialized input, one warmup.
Purchase filter -> category sum(price)/count -> revenue sort -> actual values.
No join. Tier must match Silver manifest. Dedicated results only, no shared log.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen
from pyspark import StorageLevel
from pyspark.sql import functions as F
from src.common.config import get_paths, load_config
from src.common.input_manifest import silver_fingerprint, verify_silver_manifest
from src.common.spark_session import get_spark

FRACTIONS = [0.01, 0.05, 0.10, 0.25, 0.50, 1.0]
FIELDS = ['run_id', 'fraction', 'seed', 'actual_input_rows', 'success', 'wall_time_s',
          'throughput_rows_s', 'cache_status', 'checksum', 'error', 'metrics', 'spark_config']


def validate_fraction(fraction):
    if not math.isfinite(fraction) or not 0 < fraction <= 1:
        raise ValueError('fraction must be finite and in (0, 1]')


def sample_input(df, fraction, seed):
    """Partition-invariant hash threshold, nested fractions; identical rows co-sample."""
    validate_fraction(fraction)
    return df.filter(F.pmod(F.xxhash64(F.lit(seed), *[F.col(c) for c in sorted(df.columns)]),
                            F.lit(1000000)) < F.lit(fraction * 1000000))


def prepare_workload_input(source, fraction, seed):
    # Hash MUST see the original full schema. Projection is deliberately AFTER it.
    # Raw scan still needs hash columns; only cache/decode of later queries is narrow.
    return sample_input(source, fraction, seed).select('event_type', 'category_l1', 'price')


def representative_job(df):
    query = (df.filter(F.col('event_type') == 'purchase').groupBy('category_l1')
             .agg(F.sum('price').alias('revenue'), F.count('*').alias('purchase_count'))
             .orderBy(F.desc('revenue'), F.asc('category_l1')))
    result = []
    for row in query.toLocalIterator():
        result.append(row.asDict())
        if len(result) > 10000:
            raise ValueError('workload output exceeds 10000 categories')
    return result


def equivalent(left, right):
    a = {r['category_l1']: r for r in left}
    b = {r['category_l1']: r for r in right}
    if a.keys() != b.keys():
        return False
    for k in a:
        if a[k]['purchase_count'] != b[k]['purchase_count']:
            return False
        x, y = a[k]['revenue'], b[k]['revenue']
        if x is None or y is None:
            if x != y:
                return False
        elif not math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-6):
            return False
    return True


def checksum(result):
    rows = sorted(result, key=lambda r: (r['category_l1'] is not None, r['category_l1'] or ''))
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


def _spark_metrics(spark):
    return {'status': 'unavailable', 'source': None, 'shuffle_read_bytes': None,
            'shuffle_write_bytes': None, 'spill_bytes': None, 'peak_ram_bytes': None}


def stage_watermark(spark):
    try:
        address = spark.sparkContext.uiWebUrl + '/api/v1/applications/' + spark.sparkContext.applicationId + '/stages'
        with urlopen(address, timeout=15) as stream:
            return max((a['stageId'] for a in json.load(stream)), default=-1)
    except Exception:
        return None


def run_metrics(spark, group, watermark):
    """Only stages belonging to measured job group; called outside timed region."""
    try:
        if watermark is None:
            raise ValueError('pre-run stage watermark unavailable')
        jobs = list(spark.sparkContext.statusTracker().getJobIdsForGroup(group))
        stages = set()
        for job in jobs:
            info = spark.sparkContext.statusTracker().getJobInfo(job)
            if info is None:
                raise ValueError('job metadata evicted')
            stages.update(int(s) for s in info.stageIds if int(s) > watermark)
        values = []
        for stage in sorted(stages):
            address = spark.sparkContext.uiWebUrl + '/api/v1/applications/' + spark.sparkContext.applicationId + '/stages/' + str(stage)
            with urlopen(address, timeout=15) as stream:
                attempts = json.load(stream)
            values.extend(a for a in attempts if a['status'] == 'COMPLETE')
        if not jobs or not values:
            raise ValueError('no completed measured stages')
        keys = {'shuffle_read_bytes': 'shuffleReadBytes', 'shuffle_write_bytes': 'shuffleWriteBytes',
                'spill_bytes': 'diskBytesSpilled', 'memory_bytes_spilled': 'memoryBytesSpilled',
                'executor_run_time_ms': 'executorRunTime', 'jvm_gc_time_ms': 'jvmGcTime',
                'failed_tasks': 'numFailedTasks'}
        result = {k: sum(a[v] for a in values) if all(v in a for a in values) else None for k, v in keys.items()}
        result.update(status='measured', source='Spark REST; completed stages in measured job group',
                      job_group=group, job_ids=jobs, stage_attempts=[[a['stageId'], a['attemptId']] for a in values],
                      peak_ram_bytes=None, pre_run_stage_watermark=watermark)
        return result
    except Exception as exc:
        return dict(_spark_metrics(spark), reason=repr(exc), job_group=group)


def successful_summary(records):
    ok = [r['wall_time_s'] for r in records if r['success'] and r['wall_time_s'] > 0]
    return {'successful_runs': len(ok), 'failed_runs': sum(not r['success'] for r in records),
            'median_s': statistics.median(ok) if ok else None,
            'min_s': min(ok) if ok else None, 'max_s': max(ok) if ok else None}


def append_record(path, record):
    path = Path(path)
    exists = path.exists() and path.stat().st_size > 0
    if exists:
        with path.open(newline='', encoding='utf-8') as stream:
            if next(csv.reader(stream)) != FIELDS:
                raise ValueError('incompatible E1 CSV header')
    with path.open('a', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (list, dict)) else v
                         for k, v in record.items() if k in FIELDS})


def run_fraction(spark, source, fraction, seed, n_runs, output, config, job=representative_job, validation_only=False):
    if n_runs < 1 and not (validation_only and n_runs == 0):
        raise ValueError('runs must be positive')
    sample = prepare_workload_input(source, fraction, seed).persist(StorageLevel.MEMORY_AND_DISK)
    records = []
    try:
        preparation_start = time.perf_counter()
        (output / ('plan-' + str(fraction) + '.txt')).write_text(
            sample.groupBy().count()._jdf.queryExecution().toString())
        rows = sample.count()
        oracle = [r.asDict() for r in sample.groupBy('category_l1').agg(
            F.sum(F.when(F.col('event_type') == 'purchase', F.col('price'))).alias('revenue'),
            F.sum(F.when(F.col('event_type') == 'purchase', 1).otherwise(0)).alias('purchase_count')
        ).filter(F.col('purchase_count') > 0).collect()]
        warmup = job(sample)
        if not equivalent(warmup, oracle):
            raise ValueError('independent conditional aggregate oracle mismatch')
        (output / ('sample-' + str(fraction) + '.json')).write_text(json.dumps(
            {'fraction': fraction, 'seed': seed, 'rows': rows, 'warmup_values': warmup,
             'oracle_values': oracle, 'preparation_and_warmup_s': time.perf_counter() - preparation_start,
             'cached_columns': sample.columns, 'validation_only': validation_only,
             'timing': 'Method B: sample count and one warmup excluded'}, indent=2))
        for attempt in range(n_runs):
            record = {'run_id': 'E1-' + uuid.uuid4().hex, 'fraction': fraction, 'seed': seed,
                      'actual_input_rows': rows, 'success': False, 'throughput_rows_s': None,
                      'cache_status': 'MEMORY_AND_DISK materialized; warmup complete; OS cache uncontrolled',
                      'checksum': None, 'error': '', 'metrics': _spark_metrics(spark), 'spark_config': config}
            record['started_utc'] = datetime.utcnow().isoformat() + 'Z'
            group = record['run_id']
            watermark = stage_watermark(spark)  # exclude already completed/reused preparation stages
            spark.sparkContext.setJobGroup(group, 'E1 measured fraction=' + str(fraction), interruptOnCancel=True)
            start = time.perf_counter()
            try:
                result = job(sample)
                record['wall_time_s'] = time.perf_counter() - start
                if not equivalent(result, warmup):
                    raise ValueError('workload differs from fixed sample warmup')
                record['checksum'] = checksum(result)
                record['values'] = result
                record['output_rows'] = len(result)
                record['purchase_count'] = sum(r['purchase_count'] for r in result)
                record['revenue'] = sum(r['revenue'] or 0.0 for r in result)
                record['success'] = True
                if record['wall_time_s'] > 0:
                    record['throughput_rows_s'] = rows / record['wall_time_s']
            except Exception as exc:
                record['wall_time_s'] = time.perf_counter() - start
                record['error'] = repr(exc)
            finally:
                spark.sparkContext.setLocalProperty('spark.jobGroup.id', None)
                spark.sparkContext.setLocalProperty('spark.job.description', None)
            record['metrics'] = run_metrics(spark, group, watermark)
            record['application_id'] = spark.sparkContext.applicationId
            record['finished_utc'] = datetime.utcnow().isoformat() + 'Z'
            with (output / (record['run_id'] + '.json')).open('x') as stream:
                json.dump(record, stream, indent=2)
            append_record(output / 'runs.csv', record)
            records.append(record)
            print('E1_RUN', fraction, attempt + 1, record['success'], record['wall_time_s'], flush=True)
            if not record['success']:
                break  # preserve failure, never continue/retry an unhealthy point
        return records
    except Exception as exc:
        if not records:
            append_record(output / 'runs.csv', {
                'run_id': 'E1-preparation-' + uuid.uuid4().hex, 'fraction': fraction,
                'seed': seed, 'actual_input_rows': None, 'success': False,
                'wall_time_s': 0.0, 'throughput_rows_s': None, 'checksum': None,
                'cache_status': 'preparation failed; not a measured workload run',
                'error': repr(exc), 'metrics': _spark_metrics(spark), 'spark_config': config})
        raise
    finally:
        pending_error = sys.exc_info()[0] is not None
        try:
            sample.unpersist(blocking=True)
        except Exception as cleanup:
            if not pending_error:
                raise
            print('E1_CACHE_CLEANUP_ERROR', repr(cleanup), file=sys.stderr, flush=True)


def resources(spark, status_url):
    keys = ['spark.master', 'spark.executor.cores', 'spark.executor.memory', 'spark.driver.memory',
            'spark.cores.max', 'spark.sql.shuffle.partitions', 'spark.sql.adaptive.enabled',
            'spark.sql.adaptive.coalescePartitions.enabled', 'spark.dynamicAllocation.enabled',
            'spark.sql.session.timeZone', 'spark.sql.files.maxPartitionBytes',
            'spark.sql.parquet.enableVectorizedReader', 'spark.sql.parquet.columnarReaderBatchSize',
            'spark.sql.inMemoryColumnarStorage.batchSize',
            'spark.sql.autoBroadcastJoinThreshold', 'spark.eventLog.enabled', 'spark.eventLog.dir']
    conf = {}
    for key in keys:
        try:
            conf[key] = spark.conf.get(key)
        except Exception:
            conf[key] = None  # unset/unknown; do not invent an effective measured value
    conf['spark_version'] = spark.version
    conf['application_id'] = spark.sparkContext.applicationId
    conf['driver_max_heap_bytes'] = spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
    try:
        with urlopen(status_url, timeout=5) as response:
            master = json.load(response)
        conf['workers'] = [{k: w.get(k) for k in ['id','state','cores','memory','coresused','memoryused']}
                           for w in master['workers']]
        conf['alive_workers'] = sum(w['state'] == 'ALIVE' for w in master['workers'])
    except Exception as exc:
        conf['workers_status'] = {'status': 'unavailable', 'reason': str(exc)}
    return conf


def run(args):
    for f in args.fractions:
        validate_fraction(f)
    if args.runs < 1 or (not args.smoke and args.runs < 3):
        raise ValueError('formal benchmark requires >=3 attempts and >=3 successful runs per point')
    if args.smoke and (len(args.fractions) != 1 or args.runs != 1):
        raise ValueError('smoke permits one fraction, one measured run')
    paths = get_paths(load_config())
    source_path = Path(args.silver_path or paths['silver']).resolve()
    verify_silver_manifest(str(source_path), args.tier)
    output_root = Path(args.output_root).resolve()
    allowed = Path('/app/results/m3/e1').resolve()
    if allowed != output_root and allowed not in output_root.parents:
        raise ValueError('E1 output must be under /app/results/m3/e1')
    output = output_root / (datetime.utcnow().strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8])
    output.mkdir(parents=True, exist_ok=False)
    spark = get_spark('E1_readiness' if args.smoke else 'E1_compute_only')
    manifest = {'experiment_id': output.name, 'git_sha': args.git_sha,
                'requested_tier': args.tier, 'actual_tier': args.tier,
                'tier_evidence': 'existing Silver manifest tier + current metadata fingerprint',
                'source_path': str(source_path), 'timestamp_utc': datetime.utcnow().isoformat(),
                'input_validation_status': 'pending', 'fractions': args.fractions, 'seed': args.seed,
                'smoke_only': args.smoke, 'workload': 'E1 purchase/category revenue-count v1; no join',
                'method': 'B compute-only, persisted sample + 1 warmup excluded',
                'code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    before = silver_fingerprint(source_path)
    try:
        source = spark.read.parquet(str(source_path))
        if not {'event_type','category_l1','price','source_month'} <= set(source.columns):
            raise ValueError('Silver schema missing workload/provenance columns')
        manifest.update({'base_rows': source.count(), 'schema_fingerprint': hashlib.sha256(source.schema.json().encode()).hexdigest(),
                         'schema': source.schema.jsonValue(), 'dataset_metadata_fingerprint': before,
                         'parquet_bytes': sum(p.stat().st_size for p in source_path.rglob('*.parquet')),
                         'source_months': [r[0] for r in source.select('source_month').distinct().collect()],
                         'spark_version': spark.version, 'resources': resources(spark, args.master_status_url),
                         'input_validation_status': 'verified_manifest_and_readable_parquet'})
        manifest['inventory'] = {name: {'path': str(path), 'exists': path.exists()} for name, path in {
            'official_dim_product': Path(paths['gold']) / 'dim_product',
            'raw_october': Path(paths['raw']) / '2019-Oct.csv',
            'raw_november': Path(paths['raw']) / '2019-Nov.csv'}.items()}
        records = []
        for fraction in args.fractions:
            records.extend(run_fraction(spark, source, fraction, args.seed, args.runs, output, manifest['resources']))
            if not all(r['success'] for r in records):
                raise RuntimeError('Measured failure: batch stopped without retry')
        summary = {str(f): successful_summary([r for r in records if r['fraction'] == f]) for f in args.fractions}
        (output / 'summary.json').write_text(json.dumps(summary, indent=2))
        manifest['source_metadata_unchanged'] = silver_fingerprint(source_path) == before
        print('E1_OUTPUT', str(output), flush=True)
        print('E1_SUMMARY', json.dumps(summary), flush=True)
        assert manifest['source_metadata_unchanged']
        assert all(r['success'] for r in records), 'failed measured runs; see runs.csv'
        assert all(summary[str(f)]['successful_runs'] >= args.runs for f in args.fractions)
    except Exception as exc:
        manifest['error'] = repr(exc)
        raise
    finally:
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2, default=str))
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tier', required=True, choices=['S','M','L','XL'])
    parser.add_argument('--git-sha', required=True, help='Exact HEAD supplied by submitting shell')
    parser.add_argument('--silver-path')
    parser.add_argument('--fractions', nargs='+', type=float, default=FRACTIONS)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--output-root', default='/app/results/m3/e1/formal')
    parser.add_argument('--master-status-url', default='http://spark-master:8080/json/')
    parser.add_argument('--smoke', action='store_true')
    run(parser.parse_args())
