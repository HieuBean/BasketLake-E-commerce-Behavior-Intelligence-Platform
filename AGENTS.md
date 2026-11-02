# AGENTS.md — BasketLake / M3 (Tea)

> Updated 2026-10-30. Scope: `HieuBean/BasketLake-E-commerce-Behavior-Intelligence-Platform`, active `develop` branch. Read current local checkout and approved team contracts before changes. This file guides coding agents; it does not authorize Git operations or changes outside M3.

## Role and boundaries

M3 owns Silver → Gold modeling/sessionization, six Gold outputs, experiment E1 (data scaling), experiment E4 (Spark tuning), validation and report evidence for chapters 5–6.

Primary files: `src/processing/silver_to_gold.py`, `experiments/e1_scale_data.py`, `experiments/e4_tuning.py`, focused M3 tests (`tests/test_gold.py`, `tests/test_sessionization.py` if added), M3 notes in `docs/log/`.

Treat as read-only by default: M1 Docker/Bronze/shared infrastructure, M2 `src/processing/bronze_to_silver.py`, data contracts/configuration, and M4/M5 analytics consumers. Propose cross-owner changes before implementing. Do not silently change schemas, KPI meanings, analytics interfaces or group-wide Spark configuration.

## Mandatory source inspection

Before coding, inspect `README.md`, `docs/phan-cong-nhom.md` (M3), `docs/data-dictionary.md`, `configs/pipeline.yaml`, `src/processing/bronze_to_silver.py`, `src/processing/silver_to_gold.py`, related analytics consumers and relevant tests. For E1/E4 inspect experiment scripts and `src/common/metrics.py`. Check `git status`, local branch and diff. Document what is **verified**, **inferred** and **not tested**.

## Verified local execution context (user-reported, 2026-10-30)

- Spark standalone `apache/spark:3.5.4`, one master and two alive workers, Windows PowerShell + WSL2/Docker Compose.
- Compose bind mounts repository `../:/app`, including `data/`; avoid unnecessary `docker cp`, and never delete `/app/data/raw` without recognizing the host data impact.
- `configs/pipeline.yaml`: Tier S October 2019 CSV `2019-Oct.csv`, sample fraction `0.025`, seed `42`; M October; L October + November; `cleaning.session_gap_minutes=30`.
- An old compressed file was mislabeled `.csv` and created invalid Bronze; source was extracted and Bronze/Silver rerun successfully. Validate headers/input format, not just `_SUCCESS`.
- Driver submissions confirmed working with `docker compose -f docker/docker-compose.yml exec -e PYTHONPATH=/app spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/processing/silver_to_gold.py --tier S`.
- `python3` direct is not equivalent to `spark-submit`: ad hoc PySpark checks required `PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip` in this environment.
- Observed Tier S: Bronze **1,060,438**; Silver **1,060,392**; Quarantine **46** (R7); invariant OK. Gold row counts: `fact_sessions` **921,388**, `dim_product` **85,347**, `dim_user` **629,200**, `agg_daily` **434**, `agg_hourly` **168**, `agg_funnel` **14**.
- Verified checks: zero duplicate grain keys in all six tables; zero negative duration and null session keys in `fact_sessions`; `sum(fact_sessions.event_count)=1,060,392`; Gold vs Silver purchase events **18,470** and revenue approximately **5,813,952.29**; `sum(agg_hourly.event_count)=1,060,392` and `sum(agg_daily.views+carts+purchases)=1,060,392` in this particular sample.
- `session_generated=false` for all **1,060,392** Silver events in this sample. This does **not** test R9's missing-session/gap branch. Observed counts represent a 2.5% **event sample**, not intact behavioral sessions; don't interpret short session durations/counts as population-level behavior.

These are **observations of a local run**, not a claim that all Gold logic or tests pass. Do not reuse these counts as expected values for other inputs, tiers, commits or seeds.

## Gold contracts and correctness rules

- Silver: `/app/data/lake/silver/events`; Gold: `/app/data/lake/gold/{fact_sessions,dim_product,dim_user,agg_daily,agg_hourly,agg_funnel}`.
- Existing `fact_sessions` uses grain `(user_id, user_session)` after `coalesce(user_session, session_id)` within its own function. Preserve public output columns until team-approved changes. Never combine null original sessions into one group; test fallback explicitly.
- In Silver R9, missing original `user_session` is assigned `session_id` using a 30-minute **strictly greater-than** inactivity gap on missing-session events. This branch exists but has not been exercised in real Tier S input. A **separate** gap-based comparison across all user events must be specified/tested before implementing or altering Gold semantics.
- Review cross-table consistency: `dim_user`, `agg_daily`, `agg_funnel` currently use original `user_session`; funnel groups by `(user_session, category_l1)` without `user_id`. A happy-path Tier S sample may conceal null-session or key collision issues.
- Current Gold uses `first()` for representative `category_l1`/product properties; selection can be nondeterministic on conflicting values. First define deterministic, documented policy and add fixture tests.
- Revenue is a **purchase-event price proxy**, not actual order revenue (no order/quantity). Validate purchase count and monetary totals with numeric tolerance; do not change positive/zero/null handling without contract review.
- Validate grains: sessions `(user_id,user_session)`; product `product_id`; user `user_id`; daily `(event_date,category_l1)`; hourly `(day_of_week,event_hour)`; funnel `category_l1`. Check uniqueness, nulls, nonnegative duration/counts, reconciled event sums/revenue and consumer compatibility.
- Use PySpark DataFrame/Window API; avoid production-scale `.collect()`, Python UDFs and nondeterministic sampling/ordering. Test tiny deterministic fixtures including equal timestamps, exactly 30m vs >30m, different users sharing session ID, missing originals, cross-day, zero/null prices and categories.

## Benchmark integrity — E1/E4

E1: configured fractions `[.01,.05,.10,.25,.50,1.0]`, constant cluster resources and reproducible sampling; ideally >=3 **successful** runs/point. Current code labels `--tier` but reads configured shared Silver path; never claim `--tier L` changes data unless physically rebuilt. Current docstring refers to a join not implemented; reconcile workload before benchmarks. `_spark_metrics()` returns `{}`; missing shuffle/spill data are unknown, not measured zeros. Exclude failures from medians and disclose warm/cold cache/seed policy.

E4: compare shuffle partitions `[50,100,200,400,800]`, AQE off/on, broadcast vs sort-merge join, and 20-bucket salting under controlled inputs. Verify physical plans and **materialize actual aggregate values**: a trailing `.count()` can prune revenue computation. Verify numerical equivalence, restore conf in `finally`, and exclude failed runs from statistics. No unsupported claims of speedup, physical worker independence or measured memory/shuffle metrics.

Log exact command, local git SHA, tier, input row count/path, Spark version, worker resources, seed, config, repeated timings, success/failure and source of UI/event-log metrics. Outputs go to approved `results/`, `docs/figures/`, `docs/log/` locations; never commit large raw/lake data, credentials or personal paths.

## Workflow and definition of done

1. State scope, actual current behavior, target contract, affected files, approval dependencies and focused validation plan.
2. For **first M3 coding task**, prioritize *non-destructive regression tests and contract audit* for existing Gold; no production modifications until tests isolate a flaw and semantics are agreed.
3. Run focused tests, then available project tests/lint, and Tier S pipeline only when justified. Say `NOT RUN` for unavailable checks. Preserve verified raw/Silver/Gold data and avoid overwriting outputs merely for a test.
4. Work on an isolated branch from current `develop` after checking a clean tree; do not commit, push, open PR or merge unless explicitly asked.
5. Gold is fully done only when edge-case behavior, deterministic representative fields, cross-table session identity, consumer contracts and relevant fixtures are validated. E1/E4 require separately measured reproducible evidence before completion can be claimed.

See `PLANS.md` and `SKILL.md` in this M3 working-document set; if installed at `docs/m3/`, adjust relative links and confirm the agent reads them explicitly.
