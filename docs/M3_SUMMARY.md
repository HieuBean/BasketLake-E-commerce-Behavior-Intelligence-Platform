# M3 — Gold, Tier L và E1

## Trạng thái

- **READY FOR HANDOFF:** Gold Layer, Tier L dataset và E1 Benchmark hoàn tất.
- **E4 NOT RUN** — không thuộc phần đã nghiệm thu.
- Không commit/push/PR; không thay đổi dữ liệu lake chính thức.

## Việc đã làm

- Gold: thống nhất khóa `(user_id, effective_session_id)`, ưu tiên Silver `session_id`;
  deterministic representatives theo ADR-004; giữ public schema/grain của sáu bảng.
- Tier L October + November: Bronze **109.950.743**, Silver **109.820.004**,
  Quarantine **130.739** rows; full DQ **26/26 PASS**.
- E1: **18/18 successful measured runs**, sáu fractions × ba repetitions;
  fixed hash seed42, purchase → category aggregate → sort → materialize, không join.
- Profile: Spark3.5.4,2execution cores, executor1536MiB/1core, driver2GiB,
  shuffle1024; non-vectorized reader, cache batch1000; projection sau full-schema hash.
- MethodB compute-only: materialization/oracle/one warmup ngoài timer.
- Fixtures đã nghiệm thu: Gold20, policy4, E1 9, Silver25 PASS.
  Gold TierS integration45/45PASS; không coi các kết quả lịch sử là vừa chạy lại.

## Kết quả chính

|Fraction|Actual rows|Median(s)|Mean(s)|Sample std(s)|
|---|---:|---:|---:|---:|
|1%|1.099.456|17,205|17,732|1,288|
|5%|5.492.953|17,334|17,553|0,707|
|10%|10.983.787|19,484|19,911|1,222|
|25%|27.460.414|19,888|19,657|0,403|
|50%|54.916.699|20,789|21,027|0,460|
|100%|109.820.004|26,212|26,540|1,700|

- Official app `app-20261031180518-0002`, exit0; không OOM/failed tasks, measured spill0.
- Full100%:1.659.703purchase events, revenue proxy505.126.293,44USD, reconciliationPASS.
- Input tăng99,886lần, median compute tăng1,524lần; không phải parallel speedup
  hay thời gian ingest/cleaning. Preparation riêng182,550–249,692s/point.

## Đường dẫn cần giữ

- Code: `src/processing/silver_to_gold.py`, `experiments/e1_scale_data.py`.
- Dataset: `data/lake/benchmark/e1/tier_l/{bronze,silver,quarantine}/events`.
- READY: `data/lake/benchmark/e1/tier_l/tier-l-ready.manifest.json`.
- E1 gốc: `results/m3/e1/task06b/recovery-2/` — finalization, measurements, analysis,
  CSV/JSON, validation và provenance. Các failed attempts vẫn là evidence, không trộn statistics.
- Gói nhỏ: `results/m3/delivery/`;5PNG: `docs/figures/m3/e1-task06b-final/`.
- Policy chính thức: [ADR-004](adr/004-gold-representatives-proposed.md).
- Hướng dẫn: [M3_HANDOFF](M3_HANDOFF.md).
- Kiểm tra offline: `python -m tests.run_m3_tests audit-delivery`.
- Tests M3 giữ lại: `test_gold.py`, `test_e1.py` và runner chung `run_m3_tests.py`.

## Lưu ý

- Revenue là purchase-price proxy; funnel là step membership, không ordered funnel.
- Official Gold baseline được bảo toàn; code mới đã kiểm tra ở output validation riêng.
- RFM/KMeans runtime của M3: **NOT RUN — missing numpy**; không thay thế báo cáo M5.
- Protected datasets/artifacts, manifests, metrics và event logs không được dọn xóa.
- Các báo cáo protected và trang chuyển hướng bắt buộc được giữ ngoài hai bản tổng kết này.
