---
name: m3-gold
description: BasketLake M3 Silver-to-Gold correctness, sessionization regression tests, E1 data scaling and E4 Spark tuning (read AGENTS.md and PLANS.md first).
---

# SKILL.md — M3 Gold operational workflow

> Updated 2026-10-30 for `develop` and real Tier S execution. Local checkout/approved data contract takes precedence.

## Task routing

**Default first task:** inspect Gold, build deterministic PySpark tests and document contract gaps, without changing implementation. Other modes: approved Gold fix; separate gap-session comparison; E1 controlled benchmark; E4 controlled tuning; report/PR notes. Do not expand into another member's module without approval.

## Preflight

Read `AGENTS.md`, `PLANS.md`, `docs/data-dictionary.md`, `configs/pipeline.yaml`, Gold/Silver code, affected consumers and existing tests. Execute `git status`, `git branch --show-current`, `git rev-parse --short HEAD` before editing. Confirm cluster and input paths if executing against actual data.

Use PowerShell from project root:

```powershell
docker compose -f docker/docker-compose.yml ps
docker compose -f docker/docker-compose.yml exec spark-master ls -lh /app/data/raw/
docker compose -f docker/docker-compose.yml exec spark-master ls -lh /app/data/lake/silver/events/
```

Spark submit with driver import path:

```powershell
docker compose -f docker/docker-compose.yml exec -e PYTHONPATH=/app spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/processing/silver_to_gold.py --tier S
```

For an **ad hoc** direct Python/PySpark command in this image (not generally needed for `spark-submit`):

```powershell
docker compose -f docker/docker-compose.yml exec -e PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip spark-master python3 -c "import pyspark; print(pyspark.__version__)"
```

Do not copy compressed content into a `.csv` filename. Current Compose bind-mounts the host repo into `/app`, so deletion inside `/app/data` may delete host data. No `docker cp` is needed for files already in repo `data/raw`.

## First job: Gold fixture-based regression tests

1. Inspect functions `build_fact_sessions`, `build_dim_product`, `build_dim_user`, `build_agg_daily`, `build_agg_hourly`, `build_agg_funnel` in `src/processing/silver_to_gold.py` and Silver `apply_r9_session`. Map public grain/output columns against dictionary and analytics consumers.
2. Add a compact test fixture with multiple users, same session token across users, valid and missing originals, event gap 29m59s / 30m / 30m01s, tied timestamps, midnight boundary, null/zero price, conflicting product fields/category, and `remove_from_cart`. Test current behavior separately from proposed behavior.
3. Assert grains, no session loss, `sum(event_count)`, purchase count/revenue reconciliation (floating tolerance), nonnegative durations, `converted == purchase_count > 0`, daily/hourly conservation, funnel session identity. For empty inputs and invalid business rules, describe contractual uncertainty rather than inventing expectations.
4. Run focused tests using environment already present (discover repo test commands first); if unavailable, mark `NOT RUN`. Never claim local tests run solely because the user's production Tier S counts passed.
5. Record observed failures, exact reproduction and proposed fixes. Do not change production Gold or upstream modules on the first pass without approval.

## Tier S observed baseline, not a permanent assertion

User-reported successful execution 2026-10-30: Bronze 1,060,438; Silver 1,060,392; Quarantine 46 R7; Gold `fact_sessions=921,388`, `dim_product=85,347`, `dim_user=629,200`, `agg_daily=434`, `agg_hourly=168`, `agg_funnel=14`. Zero duplicate grain keys; fact negative duration/null keys zero; fact event count sums to Silver; purchase count 18,470 and revenue ~5,813,952.29 match Silver. All 1,060,392 events showed `session_generated=false`; R9 generated-case still untested.

A random 2.5% **event sample** is not intact session sampling, so session shape and duration should not be generalized to full data.

## Approved Gold-change flow (only after first audit)

Propose a small change, record contract and consumer impact, write a failing fixture, implement with DataFrame/Window API, rerun focused tests, compare Gold results. Existing fact uses `(user_id, coalesce(user_session,session_id))` internally; dim user/daily/funnel still use original session; funnel groups without user ID. Don't silently change public columns or table names. `first()` representative fields need deterministic rules before modification. A separate 30-minute gap analysis is not identical to Silver R9 fallback and must be distinctly labeled.

## E1 workflow (only when selected)

Actual fractions `.01,.05,.10,.25,.50,1`; use fixed resource allocation, seed and workload, 3 successful measurements each when feasible. `--tier` is a label in current script and does not materialize a different dataset. Inspect representative-job docstring/code mismatch and `_spark_metrics()` TODO first. Record real rows, elapsed time, dataset SHA/config and unavailable metrics explicitly. Do not publish failed timings as successes.

## E4 workflow (only when selected)

T1 shuffle partitions 50/100/200/400/800; T2 AQE off/on; T3 broadcast vs sort-merge; T4 no salting vs 20-bucket salting. Fix input/settings/cache/warm-up policy; verify physical plan, and ensure action computes actual sums rather than optimizer-pruned `.count()` after aggregation. Check equivalent results per group with tolerance; restore all conf; summarize only valid repetitions. Save real evidence/figures under approved paths.

## Reporting format

Provide: **Objective**, **Files touched**, **Observed current behavior**, **Contract and decisions**, **Commands/tests (PASS/FAIL/NOT RUN)**, **Findings with reproducible fixtures**, **Risks and next task**. Do not commit, push, open PR or alter datasets without explicit request.
