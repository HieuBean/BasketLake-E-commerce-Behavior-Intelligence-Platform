"""M3 contract assertions; tiny fixtures only, no lake I/O.

The session identity assertions cover the reviewed Task 02 identity contract.
Functions also run through run_m3_tests.py when pytest is unavailable.
"""
from collections import defaultdict
from datetime import datetime, timedelta
from math import isclose

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.processing import silver_to_gold as gold
from src.processing.bronze_to_silver import apply_r9_session

SCHEMA = ('user_id long, user_session string, session_id string, event_ts string, '
          'event_type string, product_id long, price double, category_id long, '
          'category_l1 string, category_l2 string, category_l3 string, brand string')
BUILDERS = [gold.build_fact_sessions, gold.build_dim_product, gold.build_dim_user,
            gold.build_agg_daily, gold.build_agg_hourly, gold.build_agg_funnel]


def frame(spark, rows):
    return (spark.createDataFrame(rows, SCHEMA)
            .withColumn('event_ts', F.to_timestamp('event_ts'))
            .withColumn('event_date', F.to_date('event_ts'))
            .withColumn('event_hour', F.hour('event_ts'))
            .withColumn('category_code', F.col('category_l1'))
            .withColumn('category_missing', F.col('category_l1') == 'unknown')
            .withColumn('brand_missing', F.col('brand') == 'unknown')
            .withColumn('price_valid', F.coalesce(F.col('price') > 0, F.lit(False)))
            .withColumn('session_generated', F.col('user_session').isNull())
            .withColumn('out_of_month', F.lit(False))
            .withColumn('source_file', F.lit('2019-Oct.csv'))
            .withColumn('source_month', F.lit('2019-10'))
            .withColumn('ingest_ts', F.to_timestamp(F.lit('2019-10-03 00:00:00'))))


def event(user=1, session='s', effective=None, ts='2019-10-01 23:59:59',
          kind='view', product=1, price=10.0, category='a', brand='brand'):
    return (user, session, effective or session, ts, kind, product, price,
            100, category, None, None, brand)


def fixture(spark):
    return frame(spark, [
        event(), event(kind='cart'),
        event(ts='2019-10-02 00:00:01', kind='purchase', price=20.0, product=2, category='b'),
        event(ts='2019-10-02 00:00:01', kind='purchase', price=0.0),
        event(ts='2019-10-02 00:00:02', kind='purchase', price=None),
        event(ts='2019-10-02 00:00:03', kind='remove_from_cart'),
        event(session='second'), event(user=2),
        event(user=3, session=None, effective='gen_3_1'),
        event(user=3, session=None, effective='gen_3_2', kind='purchase', price=5.0,
              ts='2019-10-02 00:30:00'),
    ])


def keyed(df, keys):
    rows = df.collect()
    result = {tuple(r[k] for k in keys): r.asDict() for r in rows}
    assert len(result) == len(rows), f'duplicate grain: {keys}'
    return result


def test_fact_values_and_conservation(spark):
    df = fixture(spark)
    source = defaultdict(list)
    for r in df.collect():
        source[r.user_id, r.user_session or r.session_id].append(r)
    actual = keyed(gold.build_fact_sessions(df), ['user_id', 'user_session'])
    assert set(actual) == set(source)
    for key, events in source.items():
        r = actual[key]
        times = [e.event_ts for e in events]
        buys = [e for e in events if e.event_type == 'purchase']
        assert r['session_start'] == min(times) <= r['session_end'] == max(times)
        assert r['session_duration_s'] == int((max(times) - min(times)).total_seconds()) >= 0
        assert r['event_count'] == len(events)
        for kind in ['view', 'cart', 'purchase']:
            assert r[kind + '_count'] == sum(e.event_type == kind for e in events)
        assert r['converted'] == bool(buys)
        assert r['last_purchase_ts'] == (max(e.event_ts for e in buys) if buys else None)
        assert isclose(r['revenue'], sum(e.price or 0 for e in buys), abs_tol=1e-9)
        assert r['distinct_products'] == len({e.product_id for e in events})
    assert sum(r['event_count'] for r in actual.values()) == 10


def test_dimensions_values(spark):
    df = fixture(spark)
    products = keyed(gold.build_dim_product(df), ['product_id'])
    assert set(products) == {(1,), (2,)}
    p = products[(1,)]
    assert p['price_min'] == 0.0 and p['price_max'] == 10.0
    assert isclose(p['price_avg'], 65 / 8)
    assert p['price_min'] <= p['price_avg'] <= p['price_max']
    users = keyed(gold.build_dim_user(df), ['user_id'])
    assert set(users) == {(1,), (2,), (3,)}
    assert users[(1,)]['session_count'] == 2
    assert all(r['first_seen'] <= r['last_seen'] for r in users.values())


def test_daily_and_hourly_values(spark):
    df = fixture(spark)
    # Collect UTC strings: PySpark converts TimestampType to host-local datetimes.
    source = df.withColumn('event_ts', F.date_format('event_ts', 'yyyy-MM-dd HH:mm:ss')).collect()
    for builder, keys in [(gold.build_agg_daily, ['event_date', 'category_l1']),
                          (gold.build_agg_hourly, ['day_of_week', 'event_hour'])]:
        actual = keyed(builder(df), keys)
        groups = defaultdict(list)
        for e in source:
            utc_ts = datetime.strptime(e.event_ts, '%Y-%m-%d %H:%M:%S')
            key = ((e.event_date, e.category_l1) if len(keys) == 2 and keys[0] == 'event_date'
                   else ((utc_ts.isoweekday() % 7) + 1, utc_ts.hour))
            groups[key].append(e)
        assert set(actual) == set(groups)
        for key, events in groups.items():
            r = actual[key]
            assert isclose(r['revenue'], sum(e.price or 0 for e in events if e.event_type == 'purchase'))
            if 'event_count' in r:
                assert r['event_count'] == len(events)
            else:
                for kind, col in [('view', 'views'), ('cart', 'carts'), ('purchase', 'purchases')]:
                    assert r[col] == sum(e.event_type == kind for e in events)
                assert r['unique_users'] == len({e.user_id for e in events})
        if 'event_count' in next(iter(actual.values())):
            assert sum(r['event_count'] for r in actual.values()) == 10
    assert spark.conf.get('spark.sql.session.timeZone') == 'UTC'


def test_dim_user_generated_identity(spark):
    r = keyed(gold.build_dim_user(fixture(spark)), ['user_id'])[(3,)]
    assert r['session_count'] == 2, f"generated sessions: expected 2, got {r['session_count']}"


def test_daily_composite_identity(spark):
    df = frame(spark, [event(), event(user=2)])
    r = gold.build_agg_daily(df).first()
    assert r.sessions == 2, f'expected 2 user/session pairs, got {r.sessions}'


def test_daily_generated_identity(spark):
    df = frame(spark, [event(session=None, effective='gen_1_1')])
    actual = gold.build_agg_daily(df).first().sessions
    assert actual == 1, f'generated session: expected 1, got {actual}'


def test_funnel_composite_identity(spark):
    r = gold.build_agg_funnel(frame(spark, [event(), event(user=2)])).first()
    assert r.sessions_with_view == 2, f'expected 2, got {r.sessions_with_view}'


def test_funnel_generated_identity(spark):
    df = frame(spark, [event(session=None, effective='gen_1_1'),
                       event(session=None, effective='gen_1_2', ts='2019-10-02 00:30:00')])
    r = gold.build_agg_funnel(df).first()
    assert r.sessions_with_view == 2, f'expected 2, got {r.sessions_with_view}'


def test_funnel_membership_without_order(spark):
    df = frame(spark, [event(kind='purchase'), event(kind='purchase'),
                       event(kind='cart'), event(ts='2019-10-02 00:00:00'),
                       event(category='b')])
    rows = keyed(gold.build_agg_funnel(df), ['category_l1'])
    assert tuple(rows[('a',)][k] for k in ['sessions_with_view', 'sessions_with_cart',
                                          'sessions_with_purchase']) == (1, 1, 1)
    assert rows[('b',)]['sessions_with_view'] == 1


def test_daily_sessions_are_local_to_group(spark):
    df = frame(spark, [event(), event(category='b'), event(ts='2019-10-02 00:00:00')])
    rows = keyed(gold.build_agg_daily(df), ['event_date', 'category_l1'])
    assert len(rows) == 3 and all(r['sessions'] == 1 for r in rows.values())
    assert gold.build_fact_sessions(df).count() == 1


def test_r9_gap_boundaries_and_ties(spark):
    start = datetime(2019, 10, 1)
    for gap in [1799, 1800, 1801]:
        df = frame(spark, [event(session=None, ts=str(start)),
                           event(session=None, ts=str(start)),
                           event(session=None, ts=str(start + timedelta(seconds=gap))),
                           event(user=2, session=None, ts=str(start))]).drop('session_id')
        out, _ = apply_r9_session(df, gap_minutes=30)
        rows = out.collect()
        ids = {r.session_id for r in rows if r.user_id == 1}
        assert len(ids) == (2 if gap > 1800 else 1)
        assert all(r.session_generated and r.session_id for r in rows)
        facts = gold.build_fact_sessions(out).collect()
        assert sum(r.event_count for r in facts) == 4
        assert len(facts) == len(ids) + 1


def test_approved_representatives_independent_of_input_order(spark):
    # ADR-004 approved by user in Task 04B: lexical tie-break.
    rows = [event(category='a', brand='alpha'), event(category='b', brand='beta')]
    outputs = []
    for ordered in [rows, rows[::-1]]:
        df = frame(spark, ordered).coalesce(1)
        p = gold.build_dim_product(df).first()
        f = gold.build_fact_sessions(df).first()
        outputs.append((p.brand, p.category_l1, f.category_l1))
    assert outputs == [('alpha', 'a', 'a'), ('alpha', 'a', 'a')]


def test_null_only_purchase_characterization(spark):
    df = frame(spark, [event(kind='purchase', price=None)])
    assert gold.build_fact_sessions(df).first().revenue is None
    assert gold.build_agg_daily(df).first().revenue is None
    assert gold.build_agg_hourly(df).first().revenue is None


def test_empty_builders(spark):
    for builder in BUILDERS:
        assert builder(frame(spark, [])).collect() == []


def test_session_id_priority_and_blank_original(spark):
    df = frame(spark, [event(session='old', effective='canonical'),
                       event(session=' ', effective='canonical')])
    fact = gold.build_fact_sessions(df).collect()
    assert len(fact) == 1 and fact[0].user_session == 'canonical'
    assert fact[0].event_count == 2
    assert gold.build_dim_user(df).first().session_count == 1
    assert gold.build_agg_daily(df).first().sessions == 1
    assert gold.build_agg_funnel(df).first().sessions_with_view == 1


def test_original_session_fallback(spark):
    for df in [frame(spark, [event()]).drop('session_id'),
               frame(spark, [event()]).withColumn('session_id', F.lit(' ')),
               frame(spark, [event()]).withColumn('session_id', F.lit(None).cast('string'))]:
        assert gold.build_fact_sessions(df).first().user_session == 's'
        assert gold.build_dim_user(df).first().session_count == 1
        assert gold.build_agg_daily(df).first().sessions == 1
        assert gold.build_agg_funnel(df).first().sessions_with_view == 1


def test_missing_both_session_ids_rejected(spark):
    df = frame(spark, [event(session=None)])
    for builder in [gold.build_fact_sessions, gold.build_dim_user,
                    gold.build_agg_daily, gold.build_agg_funnel]:
        try:
            builder(df).collect()
        except Exception as exc:
            assert 'Gold session identity requires' in str(exc)
        else:
            raise AssertionError(f'{builder.__name__} invented a missing session')


def test_public_schemas_and_kpi_consumer(spark):
    from src.analytics.kpi import compute_kpi_summary, funnel_by_category

    df = fixture(spark)
    expected = [
        'user_id:bigint user_session:string session_start:timestamp session_end:timestamp '
        'last_purchase_ts:timestamp event_count:bigint view_count:bigint cart_count:bigint '
        'purchase_count:bigint revenue:double distinct_products:bigint category_l1:string '
        'session_duration_s:bigint converted:boolean',
        'product_id:bigint category_id:bigint category_l1:string category_l2:string '
        'category_l3:string brand:string price_min:double price_max:double price_avg:double',
        'user_id:bigint first_seen:timestamp last_seen:timestamp session_count:bigint',
        'event_date:date category_l1:string views:bigint carts:bigint purchases:bigint '
        'revenue:double unique_users:bigint sessions:bigint',
        'day_of_week:int event_hour:int event_count:bigint revenue:double',
        'category_l1:string sessions_with_view:bigint sessions_with_cart:bigint '
        'sessions_with_purchase:bigint',
    ]
    for builder, schema in zip(BUILDERS, expected):
        assert builder(df).dtypes == [tuple(field.split(':')) for field in schema.split()]
    summary = compute_kpi_summary(gold.build_fact_sessions(df), gold.build_agg_daily(df))
    assert summary['total_sessions'] == 5
    assert summary['total_revenue'] == 25.0
    assert summary['converted_sessions'] == 2
    assert summary['conversion_rate'] == 0.5
    rows = keyed(funnel_by_category(gold.build_agg_funnel(df)), ['category_l1'])
    assert rows[('a',)]['sessions_with_view'] == 4
    assert rows[('a',)]['overall_conversion'] == 0.5


def _proposed_mode(df, keys, value):
    """TEST-ONLY candidate policy: ignore null; unknown is an ordinary value.

    Not approved for production. All-null groups remain null via a left join.
    """
    from pyspark.sql import Window

    counts = df.filter(F.col(value).isNotNull()).groupBy(*keys, value).count()
    ranked = counts.withColumn('_rank', F.row_number().over(
        Window.partitionBy(*keys).orderBy(F.desc('count'), F.asc(value))))
    selected = ranked.filter('_rank = 1').select(*keys, value)
    return df.select(*keys).distinct().join(selected, keys, 'left')


def test_proposed_modes_reverse_and_repartition(spark):
    # Candidate expected values are explicit, independent of production first().
    rows = [event(category='b', brand='beta'), event(category='a', brand='alpha'),
            event(category='a', brand=None, kind='purchase', ts='2019-10-02 00:01:00'),
            event(category='b', brand='beta', ts='2019-10-02 00:02:00'),
            event(user=2, session='single', product=2, category='unknown', brand=None)]
    for ordered, partitions in [(rows, 1), (rows[::-1], 1), (rows, 3), (rows[::-1], 4)]:
        df = frame(spark, ordered).repartition(partitions)
        sessions = keyed(_proposed_mode(df, ['user_id', 'session_id'], 'category_l1'),
                         ['user_id', 'session_id'])
        assert sessions[(1, 's')]['category_l1'] == 'a'  # tied a/b -> lexical a
        assert sessions[(2, 'single')]['category_l1'] == 'unknown'
        products = keyed(_proposed_mode(df, ['product_id'], 'brand'), ['product_id'])
        assert products[(1,)]['brand'] == 'beta'
        assert products[(2,)]['brand'] is None


def test_production_metrics_reverse_and_repartition(spark):
    df = fixture(spark)
    variants = [df, frame(spark, list(reversed([
        tuple(r[c] for c in ['user_id', 'user_session', 'session_id', 'event_ts',
                            'event_type', 'product_id', 'price', 'category_id',
                            'category_l1', 'category_l2', 'category_l3', 'brand'])
        for r in df.withColumn('event_ts', F.date_format('event_ts', 'yyyy-MM-dd HH:mm:ss')).collect()
    ]))).repartition(3)]
    for builder in BUILDERS:
        outputs = [builder(v) for v in variants]
        columns = [c for c in outputs[0].columns if c not in
                   ({'category_l1'} if builder == gold.build_fact_sessions else
                    {'category_id', 'category_l1', 'category_l2', 'category_l3', 'brand'}
                    if builder == gold.build_dim_product else set())]
        left, right = [d.select(*columns) for d in outputs]
        assert left.exceptAll(right).count() == 0
        assert right.exceptAll(left).count() == 0

CATEGORY = ['category_id', 'category_l1', 'category_l2', 'category_l3']


def known(column):
    return F.coalesce((F.length(F.trim(F.col(column))) > 0)
                      & (F.lower(F.trim(F.col(column))) != 'unknown'), F.lit(False))


def choose(df, keys, values, valid):
    # Rank whole observed values/tuples. No independent per-category-column mode.
    counts = df.groupBy(*(keys + values)).count().withColumn('_valid', valid.cast('int'))
    window = Window.partitionBy(*keys).orderBy(
        F.desc('_valid'), F.desc('count'), *[F.col(c).asc_nulls_last() for c in values])
    return counts.withColumn('_rank', F.row_number().over(window)).filter('_rank = 1').select(*(keys + values))


def candidate_fact(df):
    effective = df.withColumn('user_session', gold._effective_session_id(df))
    category = choose(effective, ['user_id', 'user_session'], ['category_l1'], known('category_l1'))
    category = category.withColumn('category_l1', F.when(known('category_l1'), F.col('category_l1')).otherwise('unknown'))
    original = gold.build_fact_sessions(df)
    return original.drop('category_l1').join(category, ['user_id', 'user_session']).select(*original.columns)


def candidate_product(df):
    # PROPOSED validity: category_id nonnull AND known l1; l2/l3 may be null.
    # If no valid tuple, choose a real observed tuple by frequency/stable ordering.
    category = choose(df, ['product_id'], CATEGORY,
                      F.col('category_id').isNotNull() & known('category_l1'))
    brand = choose(df, ['product_id'], ['brand'], known('brand'))
    brand = brand.withColumn('brand', F.when(known('brand'), F.col('brand')).otherwise('unknown'))
    original = gold.build_dim_product(df)
    return (original.select('product_id', 'price_min', 'price_max', 'price_avg')
            .join(category, 'product_id').join(brand, 'product_id').select(*original.columns))


def test_p1_known_priority_tie_fallback_and_identity(spark):
    rows = [event(category='unknown')] * 3 + [event(category=None), event(category=' '),
            event(category='b'), event(category='a', kind='purchase', ts='2019-10-02 00:00:01'),
            event(user=2, category='z'),
            event(user=3, session=None, effective='gen_3_1', category=' '),
            event(user=3, session=None, effective='gen_3_2', category='q')]
    for ordered, partitions in [(rows, 1), (rows[::-1], 1), (rows, 3), (rows[::-1], 4)]:
        df = frame(spark, ordered).repartition(partitions)
        result = keyed(gold.build_fact_sessions(df), ['user_id', 'user_session'])
        assert {k: r['category_l1'] for k, r in result.items()} == {
            (1, 's'): 'a', (2, 's'): 'z', (3, 'gen_3_1'): 'unknown', (3, 'gen_3_2'): 'q'}
        assert result[(1, 's')]['event_count'] == 7
        assert result[(1, 's')]['purchase_count'] == 1
        assert result[(1, 's')]['revenue'] == 10.0
        assert result[(1, 's')]['converted'] is True
        assert result[(1, 's')]['session_duration_s'] == 2


def test_p2_observed_tuple_and_p3_brand(spark):
    def row(product, category_id, l1, l2, l3, brand):
        values = list(event(product=product, category=l1, brand=brand))
        values[7:11] = [category_id, l1, l2, l3]
        return tuple(values)
    rows = [row(1, 2, 'b', 'x', None, 'beta'), row(1, 1, 'a', None, 'y', 'alpha'),
            row(1, 1, 'a', None, 'y', None), row(1, None, 'unknown', None, None, 'unknown')]
    rows += [row(1, None, 'unknown', None, None, ' ')] * 3
    rows += [row(2, None, None, None, None, None), row(3, 3, 'c', None, None, 'gamma')]
    rows += [row(4, 9, 'z', None, None, 'beta'), row(4, 8, 'y', None, None, 'alpha')]
    for ordered, partitions in [(rows, 1), (rows[::-1], 1), (rows, 3), (rows[::-1], 4)]:
        df = frame(spark, ordered).repartition(partitions)
        result = keyed(gold.build_dim_product(df), ['product_id'])
        assert tuple(result[(1,)][c] for c in CATEGORY) == (1, 'a', None, 'y')
        assert result[(1,)]['brand'] == 'alpha'
        assert tuple(result[(2,)][c] for c in CATEGORY) == (None, None, None, None)
        assert result[(2,)]['brand'] == 'unknown'
        assert result[(3,)]['brand'] == 'gamma'
        assert result[(4,)]['category_id'] == 8 and result[(4,)]['brand'] == 'alpha'
        observed = {(r[5], r[7], r[8], r[9], r[10]) for r in rows}
        assert all((key[0], *(r[c] for c in CATEGORY)) in observed for key, r in result.items())


def test_candidate_schema_metrics_and_consumers(spark):
    from src.analytics.kpi import compute_cohort_retention, compute_kpi_summary, funnel_by_category

    df = fixture(spark)
    old_fact, new_fact = gold.build_fact_sessions(df), candidate_fact(df)
    old_product, new_product = gold.build_dim_product(df), candidate_product(df)
    for old, new, representatives in [(old_fact, new_fact, ['category_l1']),
                                      (old_product, new_product, CATEGORY + ['brand'])]:
        assert old.dtypes == new.dtypes
        columns = [c for c in old.columns if c not in representatives]
        assert old.select(*columns).exceptAll(new.select(*columns)).count() == 0
        assert new.select(*columns).exceptAll(old.select(*columns)).count() == 0
    daily = gold.build_agg_daily(df)
    assert compute_kpi_summary(old_fact, daily) == compute_kpi_summary(new_fact, daily)
    assert funnel_by_category(gold.build_agg_funnel(df)).count() == 2
    old_cohort, new_cohort = compute_cohort_retention(old_fact), compute_cohort_retention(new_fact)
    assert old_cohort.exceptAll(new_cohort).count() == 0
    assert new_cohort.exceptAll(old_cohort).count() == 0


def test_production_unique_modes_fallback_ties_and_independent_brand(spark):
    rows = [event(category='b', brand='alpha'), event(category='b', brand='alpha'),
            event(category='a', brand='beta'), event(category='a', brand='beta'),
            event(category='a', brand='beta'), event(category='unknown', brand='alpha'),
            event(category='unknown', brand='alpha')]
    # Force distinct observed category tuples; a tuple wins while brand alpha wins
    # on other events, demonstrating approved independent selection explicitly.
    rows = [tuple(list(r[:7]) + [2 if r[8] == 'a' else 1] + list(r[8:])) for r in rows]
    rows += [event(product=2, category='unknown', brand=None),
             event(product=2, category=' ', brand='unknown')]
    for ordered, partitions in [(rows, 1), (rows[::-1], 3)]:
        # Brand alpha=4 wins on b/unknown events, while category a wins with 3.
        df = frame(spark, ordered).repartition(partitions)
        assert gold.build_fact_sessions(df).first().category_l1 == 'a'
        product = keyed(gold.build_dim_product(df), ['product_id'])
        assert product[(1,)]['category_id'] == 2
        assert product[(1,)]['brand'] == 'alpha'
        assert product[(2,)]['category_l1'] == ' '  # all-invalid tuple tie, lexical ASC
        assert product[(2,)]['brand'] == 'unknown'


