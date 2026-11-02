# PLANS.md — M3 (Tea): Gold correctness, E1 and E4

**Updated:** 2026-10-30  
**Repo / branch basis:** `HieuBean/BasketLake-E-commerce-Behavior-Intelligence-Platform`, `develop` (verify local HEAD before editing)  
**Owner:** M3 (Tea)  
**Evidence level:** Tier S numbers below are from user-provided execution logs; no unreported experiment or test is claimed.

## Objective and ownership

Own Gold/sessionization/dimensions/aggregations, E1 scale-data and E4 Spark-tuning experiments, plus chapters 5–6 evidence. M1 owns Docker/Bronze, M2 owns Silver/R1–R9, M4/M5 consume Gold. Coordinate any upstream or public-contract change before editing other modules.

## Current task status — closure review 2026-10-30

Documentation-only review of existing evidence; no tests or pipelines rerun.

| Task | Current status | Execution evidence (kept under its original task) |
|---|---|---|
| [Task 01](docs/M3_SUMMARY.md) | COMPLETE | Audit and reproductions: Gold 9 PASS / 5 FAIL |
| [Task 02](docs/M3_SUMMARY.md) | COMPLETE | Gold 18/18; Silver 25/25 PASS |
| [Task 03](docs/M3_SUMMARY.md) | COMPLETE — Closed by Task 04B | Historical Task03: Gold 20/20, integration 28/28 PASS |
| [Task 04](docs/M3_SUMMARY.md) | PARTIAL — Superseded by Task 04B | Historical preparation: prototype 3/3, Gold 20/20 PASS |
| [Task 04B](docs/M3_SUMMARY.md) | COMPLETE | Gold 20/20, policy 4/4, integration 45/45, Silver 25/25 PASS |
| [Task 05](docs/M3_SUMMARY.md) | COMPLETE — readiness/smoke only | E1 fixtures 8/8 PASS; one S 1% smoke PASS; formal L input not ready |
| [Task 06A](docs/M3_SUMMARY.md) | PARTIAL — BLOCKED: NOVEMBER INPUT MISSING | Isolated config/preflight; 8 safety checks PASS; no L build |

Task03 directly completed policy analysis/prototype, safe separate Tier S output,
Silver reconciliation, baseline comparison, KPI/funnel execution and RFM schema/
dependency review. Task04B supplied user approval under ADR-004, production
deterministic selection, reverse/repartition policy assertions, schema/grain and
metric preservation, separate Tier S validation and KPI/funnel/cohort compatibility.
The acceptance-criteria closure matrix is in the Task03 report. Task04B results
are supplemental evidence, not replacement Task03 measurements or a Task03 rerun.

- [x] Close Task03 acceptance criteria using direct + Task04B evidence.
- [x] Preserve Task03/Task04 historical results and original logs/JSON.
- [x] Record user approval only; no M2/team approval asserted.
- [ ] RFM/KMeans runtime: **NOT RUN — missing numpy** (tracked follow-up, not PASS).
- [ ] Full-stream gap comparison, broader session-collision decisions and benchmark
  preparation remain separate work; closing Task03 does not close all M3 phases.

## Phase 0 — Environment and pipeline baseline: OBSERVED COMPLETE (2026-10-30)

- [x] Spark 3.5.4 standalone cluster ran with 1 master, 2 workers in Docker/WSL2.
- [x] Replaced incorrectly named compressed input with real extracted `2019-Oct.csv` in repo-mounted `data/raw`.
- [x] Bronze Tier S (fraction .025, seed 42) completed: **1,060,438** rows.
- [x] Silver Tier S completed: **1,060,392** rows; Quarantine **46** all R7; **1,060,438 = 1,060,392 + 46**.
- [x] Gold Tier S created and read all six tables.
- [ ] Record exact local git SHA and system/worker CPU/RAM settings before benchmarks; not evidenced yet.
- [x] Verify `git status`, work on M3 task branch; feature/m3-gold-layer at d07282a, evidenced Tasks01–04B.

## Tier S Gold baseline — VERIFIED BASIC CHECKS

| Output | Rows | Basic check |
| --- | ---: | --- |
| `fact_sessions` | 921,388 | key duplicate=0; null session keys=0; duration < 0 =0 |
| `dim_product` | 85,347 | duplicate `product_id`=0 |
| `dim_user` | 629,200 | duplicate `user_id`=0 |
| `agg_daily` | 434 | duplicate `(event_date,category_l1)`=0 |
| `agg_hourly` | 168 | duplicate `(day_of_week,event_hour)`=0 |
| `agg_funnel` | 14 | duplicate `category_l1`=0 |

- [x] `sum(fact_sessions.event_count)` = **1,060,392** = Silver rows.
- [x] Silver purchase events = Gold purchase events = **18,470**.
- [x] Silver revenue vs fact-session revenue ~= **5,813,952.29** (floating-point tolerance).
- [x] `sum(agg_hourly.event_count)` = **1,060,392**.
- [x] `sum(agg_daily.views+carts+purchases)` = **1,060,392** for this input (not a universal invariant when `remove_from_cart` exists).
- [x] All Silver input events in this sample have `session_generated=false`; **0** exercise R9 generated sessions.
- [ ] Verify field-level values for dimensions, daily/hourly revenue per group, funnel membership and edge-case/session semantics.

**Interpretation limit:** 2.5% random **event** sampling creates artificially sparse sessions; `921,388` sessions and averages from this sample should not be described as population session behavior. A successful write/read and aggregate sum is not proof of all business logic.

## Phase 1 — FIRST CODING TASK: Gold contract audit and deterministic regression tests (priority 1)

Primary paths: `src/processing/silver_to_gold.py`, a focused test file under `tests/` (choose actual repo naming convention), `docs/log/` for evidence. Initially **do not modify production code**.

- [x] Inspect current local HEAD, Gold implementation, `docs/data-dictionary.md`, Silver R9 and consumers; produce actual-vs-required grain/semantics matrix.
- [x] Build minimal deterministic PySpark fixtures testing all six builders, particularly `build_fact_sessions`, `build_agg_funnel`, null original `user_session`, two users with same session token, exactly 30m/>30m, ties, cross-midnight, null/zero prices, `remove_from_cart`, conflicting category/brand.
- [x] Verify existing behavior without assuming desired business rules: uniqueness; event and purchase reconciliation; revenue numeric tolerance; valid duration; conversion semantics; daily/hourly aggregates; funnel counts; stable outputs where applicable.
- [x] Separate **current behavior**, **confirmed bug**, **ambiguous contract**, **proposed change**. Record expected failing tests without hiding failures; do not change public schemas before approval.
- [x] Run focused tests in existing execution environment; report commands, PASS/FAIL/NOT RUN and any fixtures that reveal bugs.

**Exit:** reproducible Gold test suite + concise contract/risk report. Submit proposed fixes for discussion; do not state Gold fully complete until edge cases are handled.

## Phase 2 — Resolve Gold correctness + sessionization semantics (priority 1)

- [x] Agree canonical session identity across fact, dim user, daily, funnel: user-approved Task02 `(user_id, effective_session_id)`; null-session fallback documented.
- [x] M2 R9 already builds `session_id` for missing originals using gap >30m among missing-session events; tested with synthetic missing-session data (not exercised by Tier S).
- [ ] Specify separate **full event-stream gap comparison**, including per-user event ordering, deterministic tie-breaks and gap exactly 30m vs >30m. Do not silently replace original sessions or add public table without agreement.
- [x] Define deterministic representative `category_l1`/brand policy when events conflict, replacing unspecified `first()` with user-approved ADR-004 rules in Task04B.
- [ ] Confirm revenue/converted behavior for invalid, missing and zero purchase prices; document that values represent event-price proxies.
- [ ] Validate field-level Gold outputs and M4/M5 downstream contracts with fixtures and Tier S where appropriate.

**Exit:** approved semantics + passing regression tests + documented differences/reconciliation.

## Phase 3 — E1 data scaling (after Gold correctness; priority 2)

After Task05, `experiments/e1_scale_data.py` verifies the existing Silver tier/file
manifest before execution; --tier never builds data. Six fractions unchanged,
fixed hash seed42 and Method B compute-only; dedicated M3 logs, no shared CSV.
Current verified input is S. Formal Tier L requires a separately approved build.

- [ ] Confirm whether experiment requires physically built L or can be a labeled Tier S smoke-test; do not overwrite today's S baseline by accident.
- [x] Reconcile intended join workload in docstring with actual implementation (purchase → category revenue/count, no join), Task05.
- [x] Fix deterministic sampling and cache/timing boundary; record true input rows,
  SHA/code hash and resource snapshot; Task05 smoke verified, formal resource gate remains.
- [ ] Only use measured shuffle/spill metrics; `_spark_metrics()` is currently TODO. Unknown is not zero.
- [ ] >=3 successful runs/fraction when feasible; summarize median/min/max of successes only; log individual failures.
- [ ] Capture evidence in `results/experiment_log.csv`, planned `docs/figures/e1_scale_data.png`, reproducible notes and limitations.

**Exit:** controlled data-size/time results, documented workload, valid chart and transparent instrumentation.

## Phase 4 — E4 Spark tuning (after sound baseline; priority 2)

- [ ] T1 shuffle partitions `[50,100,200,400,800]` with fixed AQE.
- [ ] T2 AQE off/on with remaining settings fixed.
- [ ] T3 broadcast vs sort-merge: verify physical plan, same join and identical result measures.
- [ ] T4 no salting vs 20-bucket salting for actual skew; verify per-product result equivalence.
- [ ] Avoid `.count()`-only benchmark after aggregation (Catalyst can skip revenue expressions); force real revenue calculations.
- [ ] Restore configs, manage cache/warm-up fairly, exclude failed runs, record 3 success timings where feasible.
- [ ] Preserve actual Spark UI/event-log evidence; don't invent shuffle, task skew or memory metrics.

**Exit:** repeatable T1–T4 metrics, correctness comparisons and workload-specific interpretation.

## Phase 5 — Report and delivery

- [ ] Document Gold grains/sessionization, edge-case and reconciliation tests in chapter 5.
- [ ] Document E1/E4 methodology, hardware caveats, repeated timings, figures and Spark UI evidence in chapter 6.
- [x] Consolidate M3 notes under `docs/log/m3-*.md`; review git diff for data/secrets and scoped ownership.
- [ ] Request teammate review; create commit/PR/push/merge only when specifically authorized.

## Decisions needed before changing semantics

1. Canonical session key and role of generated `session_id` across all six outputs.
2. Whether gap-session comparison becomes a separate table, test-only output or experimental view.
3. Deterministic representative category/brand selection policy.
4. Price validity rules and exact funnel interpretation (step membership vs ordered conversion).
5. Actual E1 representative workload and benchmark tiers/resources.

## First action to execute

Use the companion agent prompt supplied with these files: **audit and add small deterministic Gold tests only; do not alter Silver/Bronze, production Gold or benchmark results on the first pass**. Collect focused test results and propose ranked follow-up fixes.

## Task 04 evidence — 2026-10-30

Current status: **PARTIAL — Superseded by Task 04B**. The checklist below is the
historical state at Task04 handoff; its pending items were fulfilled by Task04B.

- [x] Searched local policy evidence; P1/P2/P3 remain PENDING, not approved.
- [x] Prepared test-only known-priority/observed-tuple prototype and decision proposal
  `docs/adr/004-gold-representatives-proposed.md`; prototype 3 tests PASS.
- [ ] Approve representative policy and P2 validity/fallback before production edits.
- [ ] Implement approved policy and run Tier S into a separate task04 validation output.

See `docs/M3_SUMMARY.md` for the consolidated result.

## Task 04B evidence — 2026-10-30 (supersedes Task04 pending state)

- [x] User explicitly approved P1/P2/P3 and ADR-004 details in current Task04B prompt.
  No M2/team approval is asserted. ADR path retained and approval recorded.
- [x] Production deterministic representatives implemented with observed category
  tuples, known-priority mode and stable tie-breaks; public column names/types/order retained.
- [x] Gold 20/20, production policy suite 4/4, Silver 25/25 PASS, all exit 0.
- [x] Tier S separate task04 output: 45/45 checks PASS, no protected metadata changes;
  official/Task03 differences explained by representatives/numeric tolerance.
- [x] KPI/funnel/cohort validated in available scope.
- [ ] RFM/KMeans runtime: NOT RUN (numpy missing); do not install without dependency plan.
- [ ] Prepare E1 workload/input/resource/seed/cache protocol; no E1/E4 runs in Task04B.

See `docs/M3_SUMMARY.md` for the consolidated result.

## Task 05 evidence — 2026-10-30

- [x] Audit E1/metrics/plot/manifest; no shared modules/config/Gold edits.
- [x] Verified Silver S 1,060,392 rows, 101,506,218 Parquet bytes, October only;
  reject requested L on S; raw November/Silver L not available at inspected paths.
- [x] Workload v1 no-join, Method B, fixed hash sample/seed, finally cleanup,
  success-only summaries and dedicated append-safe CSV implemented.
- [x] E1 fixture tests 8/8 PASS (two runs), one cluster S 1% smoke PASS; actual
  sample 10,639 rows, one measured run; no scalability conclusion.
- [x] Input/resource manifest, actual aggregate values, checksum and unavailable
  shuffle/spill markers recorded; shared experiment CSV/Silver metadata unchanged.
- [x] Task06 plan documented in task05-e1-benchmark-readiness.md.
- [ ] Build/verify separate Tier L with M1/M2 approval; no automatic pipeline run.
- [ ] Formal >=3 successful runs at six points, stage-scoped telemetry collection
  and dedicated manifest-aware plot; not run/implemented as measurements in Task05.

## Task 06A evidence — 2026-10-30

- [x] Read-only raw/disk/cluster/bind mount audit; October header/sample valid,
  November missing at canonical/alias raw paths. Full October scan NOT RUN.
- [x] Isolated config prepared via existing CONFIG_PATH; semantics preserved,
  output roots outside official lake. 8 safety checks PASS, exit 0.
- [x] Before/after Tier S file lists/sizes/content SHA256 + shared experiment CSV
  recorded in task06a-preflight-evidence.json; protected content unchanged=true.
- [ ] Supply/verify raw November and storage/backing VHD headroom before writes.
- [ ] Review Silver shared metrics side effect with M1/M2; CONFIG_PATH isolates
  data/DQ paths but Silver CLI still appends shared results/experiment_log.csv.
- [ ] Build isolated Bronze/Silver L, validate DQ/invariant/R7/keys/months and
  generate truthful L producer/build manifests; not run while gates blocked.
- [ ] Hand off validated L to Task06B; planned path is not a ready dataset.

## Task 06A-R evidence — 2026-10-30

Status: **PARTIAL — ON HOLD for Task06A-INFRA storage planning**. Historical
Task06A November-missing status above is retained; resume has newer input evidence.

- [x] November verified with October; full raw checksums in task06ar-preflight.json.
- [x] Metrics wrapper isolates build/DQ output without modifying M1/M2/shared metrics.
- [x] Isolated Bronze L built/read back:109,950,743 rows, both source months,
  111 Parquet files/4,006,673,564 bytes; no official lake overwrite.
- [x] Standalone Silver OOM diagnosed and failed attempt retained; controlled local
  recovery stopped when user requested infrastructure audit. No active build now.
- [ ] Silver/Quarantine L, DQ/invariants, producer manifest and Task06B READY handoff.

See [Tier L summary](docs/M3_SUMMARY.md).

## Task 06A-INFRA evidence — 2026-10-30

Status: **AUDIT/PLAN COMPLETE — migration/configuration/cleanup NOT EXECUTED**.

- [x] Located Docker WSL2 data VHD on C,43.13 GB file length; measured C/E capacity,
  Docker/WSL inventory, volumes/mounts, Spark scratch/events and bounded cache/copy scan.
- [x] Protected official lake/previous validation/shared experiment files full SHA256
  unchanged from Task06AR preflight; Bronze L hashes recorded;2 workers ALIVE/apps0.
- [x] Documented Docker Desktop UI migration plan, concrete unimplemented per-worker
  Spark temp/work bind diff, backup/postchecks and separately approved cleanup candidates.
- [ ] User approval and supported Desktop UI migration; no manual VHD moves/deletions.
- [ ] Shared infrastructure approval for Compose diff, directory creation and restart.
- [ ] Postmigration Spark reads/protected hashes and resource plan before Silver L resume.

See [Tier L summary](docs/M3_SUMMARY.md). No deletion,
data migration, Compose change, Tier L build or benchmark performed in this infra task.

## Task 06A-INFRA-B evidence — 2026-10-30

Status: **COMPLETE — E Docker storage verified; offline backup and scratch isolation PASS**.
Historical audit pending state above is retained; approval and execution follow-up here.

- [x] User approved backup, supported migration, minimal Compose scratch changes and restart.
- [x] New preflight found Docker already relocated to E before agent action; live E VHD
  confirmed after start, no simulated UI/migration claim and no internal settings edits.
- [x] Official Desktop stop; WSL stopped; exclusive-handle offline43,110,105,088-byte
  backup on E, source/target SHA256 identical. Backup retained outside repository.
- [x] Compose config/allowed-diff check PASS; preserve old worker logs, recreate only
  master/workers with distinct E local/work binds, start History, keep all volumes/images.
- [x] Standalone shuffle fixture20,000 rows/37 keys ran on both workers; effective
  /spark-local, write permission and application temp cleanup PASS.
- [x] Fresh read-only Silver S1,060,392 rows and6 official Gold counts/schemas PASS;
  protected files SHA256/list/size before/after PASS542 files, shared CSV unchanged.
- [x] Cluster2 workers ALIVE, unchanged2cores/2GiB each,0 active apps after smoke.
- [ ] Task06A-R resource/peak scratch budget and memory execution plan before Silver L
  resume; E~69.62GB free after retained backup. No Silver L/E1 run in infra task.

See [Tier L summary](docs/M3_SUMMARY.md). No cleanup,
old VHD deletion, prune, unregister, factory reset, commit/push/PR.

## Task 06A-R2 evidence — 2026-10-30

Status: **PARTIAL — ON HOLD FOR USER DECISION / READ-ONLY SCOPE**; Task06B NOT READY.
E disk renewed gate PASS13:35Z; prelaunch then stopped on attached backup protection.
Original disk BLOCKED inspection below is retained as historical evidence.

- [x] Fresh Spark read verifies Bronze L109,950,743 rows, both months, unchanged
  schema/layout111 files/4,006,673,564 bytes;12 missing original sessions before cleaning.
- [x] Historical executor Java heap OOM distinguished from manually cancelled local
  recovery. History full-input shuffle measured13.15GB; not inferred from4GB Bronze.
- [x] Job-specific proposal:1core/1536MiB executor,total2cores,driver2g,shuffle1024,
  AQE coalesce off and no R7 persist; cluster/defaults/M2 rules unchanged.
- [x] Explicit planning envelope84.45GB vs inspected E74.25GB (~10.20GB shortfall).
  No full-run peak guarantee; stopped before Silver write gate, no retry.
- [x] M3 wrapper adds --no-persist-dedupe using existing M2 parameter; real tiny
  R1-R9/metrics isolation fixture PASS. Budget unit tests4/4 PASS, all exit0.
- [x] Main protected content542 files SHA256 PASS; extended old-results/Task01–05
  snapshot prepared, backup retained (fresh43GB rehash NOT RUN while write gate blocked).
- [ ] Satisfy/recheck E capacity (target>=90GB free), watcher and fresh write preflight.
- [ ] Silver/Quarantine L build, independent DQ, producer/content manifest and READY handoff.

Latest R2: E free126.24GB measured host/container, disk envelope passed; no agent
cleanup. Pre-write542-file SHA preservation PASS; watcher prepared/self-test PASS,
exact-driver identity tests2/2 PASS. Controlled prelaunch exit1 before Silver submit:
backup VHD is Windows-attached read-write PhysicalDrive3, read-lock unavailable.
Await user approval to dismount only backup then verify full SHA; no detach/restore
performed. Disk shortage is historical, current blocker is backup mount/protection.

User subsequently approved backup-only dismount conditionally: exact PhysicalDrive3
mapping AND no process using the file. Mapping confirmed13:49Z, but Windows Restart
Manager reports System PID4; stopped without dismount or fresh SHA check. Approval
is received; current blocker is file use, not missing approval. Final read-only
preservation542 files PASS, workers2 ALIVE/apps0; no Silver L or VHD mutation.

Latest read-only investigation: VHDMP attach13:09:19Z by historical PID27488,Windows
disk3 RAW/0 partitions,no Windows drive letter; initiator executable unknown. Backup
fresh-hash/unattached lock gate was agent-added, not Spark/Silver dependency. Historical
backup SHA remains valid at INFRA-B checkpoint; current content not revalidated after
RW attach. User decision needed on backup assurance vs isolated Silver policy, not
automatic permission to detach. Watcher/code unchanged; no Silver/VHD mutation.
See [Tier L summary](docs/M3_SUMMARY.md).

## Task 06A-R3 — 2026-10-30

Status: **PARTIAL — STOPPED BY WATCHER C-DELTA GUARD; NOT READY**. User approved decoupled backup gate; no VHD content access
or mount-state changes. Backup checksum NOT REVALIDATED, attached is independent warning.

- [x] Controller policy regression17 assertions PASS; mandatory/C-path/resource guards retained.
- [x] Fresh no-cache M2 wrapper fixture PASS;542 protected files content PASS,115 prior artifacts snapshot.
- [x] Fresh host E-backed paths/2workers/resources/output isolation PASS; Spark Bronze
  count109,950,743/both months/schema PASS,app-20261030141808-0002.
- [ ] Controlled full Silver/Quarantine L build and independent R1–R9/E1 DQ.
- [ ] Host preservation, full output fingerprints, READY manifest/Task06B handoff.

Actual run app-20261030142119-0003 exit1 at15:14Z after53min: watcher relative C
drop>5GB stopped exact own driver, not OOM/ENOSPC or wrong route. C still124GB,
Windows pagefile allocation grew3082→6198MiB (explains part of delta, not all).
As-run controller/evidence retained; poststop fixed relative C drop to warning,
absolute C/E reserves/OOM/health/route protection remain;19 policy assertions PASS.
No retry. Silver/Quarantine dataset absent after abort, no READY/full DQ.

- [x] Poststop preservation542 protected files/115 prior artifacts SHA PASS,cluster
  healthy2 workers/apps0,scratch files0;backup checksum NOT REVALIDATED.
- [ ] Fresh reviewed controlled attempt and full L validation; Task06B NOT READY.

See [Tier L summary](docs/M3_SUMMARY.md). No E1 benchmark.

See [Tier L summary](docs/M3_SUMMARY.md). No Silver L,
E1/E4 benchmark, cleanup, config/Compose/upstream changes, commit/push/PR.

## Task 06A-R4 — 2026-10-31

Status: **PARTIAL — SWAP MIGRATION PASS; SILVER RETRY NOT RUN**.
User explicitly approved swap-only global maintenance in this session.

- [x] Read-only pagefile/WSL/Docker/cache audit; C growth only partly explained.
- [x] WSL swap4GiB moved through supported .wslconfig + controlled WSL/Desktop restart.
- [x] New swap path E verified by VHDMP; two workers ALIVE/apps0; active Docker remains E.
- [x] Windows pagefile unchanged; no direct Docker VHD operations or cleanup.
- [x] Protected542 files and115 prior artifacts SHA256 PASS after maintenance.
- [x] Project terminal temp/cache launcher, route preflight and E monitoring added.
- [x] Storage-script12 assertions and existing policy19 assertions PASS.
- [ ] Container job tmp/Java overrides and fixture; integrated controlled-run monitoring.
- [ ] Full fresh Silver gates, one controlled retry, full DQ and READY manifest.

Backup checksum NOT REVALIDATED. R3 E free-space anomaly remains partly UNKNOWN;
protected content unchanged. Task06B NOT READY. No Silver/E1 during approved swap step.
See [Tier L summary](docs/M3_SUMMARY.md).
Historical R2/R3 evidence/status remains intact. No commit/push/PR.

## Task 06A-R4B — 2026-10-31

Status: **PARTIAL — PRODUCER INCOMPLETE; TERMINAL STATUS UNKNOWN. Task06B NOT READY.**
Historical controlled Silver attempt: app-20261030180919-0002; no second submit.
Container E-backed storage audit/distributed scratch fixture/mandatory gates PASS.
Full DQ and READY handoff pending. Preservation postchecks PASS. No second retry.
See docs/M3_SUMMARY.md.

Read-only session recovery2026-10-31: no live driver/controller or Master active app;
History incomplete, last persisted stage19 303/1460 tasks,0 failed. Silver committed
61 Parquet files/9,835,577,566 bytes; Quarantine absent. No producer exit code or
build report; do not label SUCCEEDED or infer OOM/kill. Host reboot05:20:44UTC and
container restart05:23UTC observed; exact producer termination cause remains unknown.

- [x] One-shot reattachment/History/process/output/storage evidence saved; no new watcher.
- [x] Fresh542 protected files and117 prior-artifact content checks PASS.
- [ ] Full Tier L DQ/accounting — BLOCKED: producer unverified, Quarantine absent.
- [ ] Separately reviewed/approved minimal Quarantine recovery; no retry in this task.
- [ ] READY manifest and Task06B handoff after full validation/preservation.

Backup checksum NOT REVALIDATED; no VHD/global config change, output deletion or E1.

## Task 06A-R4C — 2026-10-31

Status: **COMPLETE — FEASIBLE (investigation/plan only); recovery NOT EXECUTED.**

- [x] Independent Silver write commit verified: stage16 complete, stdout marker,
  `_SUCCESS`20:12:25.503UTC30Oct,61 Parquet/one write UUID, no temporary files.
- [x] Read-only application app-20261031055126-0000 exit0:21 checks +9 fixture checks PASS.
- [x] Fresh Silver109,820,004 rows; October42,418,544/November67,401,460;
  generated12, checked null/normalization/session conformance defects0.
- [x] Full Bronze source-file/month FD +exact12-column schema proves public Q
  tie-equivalence; cache-off/reverse/repartition fixtures agree. R5 raw-vs-normalized
  flag ties and Data Dictionary raw-value mismatch explicitly documented.
- [x] Quarantine-only isolated plan/resource envelope/stop conditions prepared;
  no producer recovery submitted. B−S130,739 remains unverified Q gap.
- [x] Protected542/prior117/Silver124-file content/source hashes unchanged.
- [ ] Separate approval +implementation/review for Q-only recovery entrypoint.
- [ ] Full post-recovery DQ including exact R7 duplicates/B=S+Q/revenue, then READY.

Task06B **NOT READY**; R4B producer remains incomplete. R4C does not rewrite that
history or certify the full dataset. See
[Tier L summary](docs/M3_SUMMARY.md).

## Task06A-FINAL — 2026-10-31

Status: **COMPLETE — Tier L READY; Task06B may begin.**
User approved A–E end-to-end; historical R4B interrupted producer status is retained,
not rewritten. Its dataset-level pending items are closed by FINAL below.

- [x] Source/Silver SHA preflight; no system, M1/M2, Gold/default config/Compose edits.
- [x] New M3 Quarantine-only writer;7 fixture checks +native no-overwrite test PASS.
  Initial private-temp fixture failure fixed/tested; failed evidence retained.
- [x] One Quarantine attempt app-20261031063308-0003 exit0; no Silver branch action.
- [x] Full DQ app-20261031065148-0004 exit0,26/26 PASS, exact payload/R7/global money.
- [x] Counts B109,950,743=S109,820,004+Q130,739; Q all R7, actual readback not assumed.
- [x] Owned candidate renamed into empty Tier L official Q via same-E no-overwrite
  native move; post-move content unchanged,1 Parquet/5,012,706 bytes.
- [x] Protected542/prior117/Silver124-file/source hash preservation PASS before/after/final.
- [x] READY manifest and E1-compatible producer fingerprint published/verified;
 2 workers ALIVE,0 active apps. Backup checksum NOT REVALIDATED, untouched.
- [ ] Task06B formal E1 matrix — READY TO START, NOT EXECUTED here; own experiment preflight.

READY `/app/data/lake/benchmark/e1/tier_l/tier-l-ready.manifest.json`.
Silver `/app/data/lake/benchmark/e1/tier_l/silver/events`; official Tier L Quarantine
`/app/data/lake/benchmark/e1/tier_l/quarantine/events`. No Tier S overwrite.
See [Tier L summary](docs/M3_SUMMARY.md).

## Task06B — Official E1 (2026-11-01)

Current status: **COMPLETE — recovery-2:18/18 official measured runs,
validation/analysis/charts/tests/preservation PASS.**

Historical official-1: **BLOCKED — executor heap OOM trong preparation25%;0/18 measured runs.**

Continuation approved by user: **COMPLETE — recovery-2 validation25%/100% and
official18; original BLOCKED attempt retained below.** Fixed resource profile;
post-hash cache projection3columns, non-vectorized reader/batch settings fixed.
9/9 E1 tests PASS; versioned770-file SHA preflight PASS, including explicit proof
of the authorized historical Task05 documentation append (no data exceptions).

- [x] READY Tier L + SHA preflight770 watched files; routes/cluster/reserves PASS.
- [x] E1 fixture8/8 PASS; measured job-group metrics và stop-on-failure verified.
- [x] One official matrix controller independent of Codex; frozen Method B/hash42,
  fixed2 cores/1536MiB executors/2GiB driver/shuffle1024,6fractions ×3 repetitions.
- [x] app-20261031172341-0000 stopped by OOM guard; no application retry;
  stage8 failed75/158tasks complete,4failed; evidence retained; cluster2ALIVE/idle.
- [x] Producer exit0 +18 successful measured runs — closed by recovery-2;
  original sample.count heap OOM retained, never included in successful timing.
- [x] Summary/5final PNG figures/correctness/full-data100% reference reconciliation.
- [x] Fresh M3 regressions:Gold20/policy4/Silver25 PASS; preservation770files PASS after.
- [x] Failed-attempt summary/status CSV and handoff checklist; no synthetic charts.
- [x] User approved preparation/projection/reader mitigation with same resource profile;
  fresh recovery-2 owns validation then official execution, no subtask chain.
- [x] Full-data preparation25%/100% PASS, validation app-20261031175204-0001 exit0;
  actual27,460,414/109,820,004rows and source/reference money reconcile.
- [x] Official6×3 measured runs, app-20261031180518-0002 exit0 under same profile.
- [x] Recovery2 E1 tests9/Gold20/policy4/Silver25 PASS;770-file SHA postcheck PASS.
- [x] `recovery-2/finalization.json` COMPLETE; source/input/evidence SHA and handoff.
  Postprocessor INFO-stderr handling failure retained; final tests/checks recovered
  independently without benchmark resubmission. No production or lake changes.

See [E1 summary](docs/M3_SUMMARY.md).
Historical Task01–06A evidence retained; E4 remains outside this task.

## M3 FINAL HANDOFF — 2026-11-01

Status: **M3 READY FOR HANDOFF — Gold + Tier L + E1; E4 NOT RUN.**

- [x] Current branch/HEAD/dirty diff attributed; pre-existing shared outputs retained.
- [x] Independent audit recomputed18run statistics and verified57artifact hashes.
- [x] Fresh final fixtures:E1 9/Gold20/policy4 PASS; historical Silver25 reused explicitly.
- [x] Existing inventory SHA preservation770files PASS; no protected dataset writes.
- [x] README E1 legacy-target caveat, safe guide and Chương5–6 technical summary.
- [x] Small portable aggregate evidence +5final PNGs; offline audit without Spark/lake.
- [x] Exact file-level commit recommendations, holds/ownership reviews and PR draft.
- [ ] User approval/human diff review before any staging/commit/push/PR.

See [final handoff](docs/M3_SUMMARY.md),
[guide](docs/M3_HANDOFF.md).
No benchmark rerun, E4, infrastructure action or new technical task in this audit.

