# Trạng thái tier L

> **Cập nhật cuối**: 2026-11-02 · **Trạng thái**: ✅ **HOÀN THÀNH TOÀN BỘ**
>
> Lịch sử: lần đầu dừng ở bước `gold` vì hết dung lượng ổ đĩa C:. Sau đó chuyển dự án sang
> `D:\` (C: từ 10,1 → 38,3 GB), chạy tiếp và hoàn tất gold + analytics + plots.
> Các mục §2, §5, §6 dưới đây là ghi chú lịch sử, **không cần làm nữa**.

---

## 1. Đã xong những gì

| Bước | Trạng thái | Kết quả đo được |
|---|---|---|
| Tải dataset | ✅ | `2019-Oct.csv` 5,28 GB + `2019-Nov.csv` 8,39 GB (đã xoá zip 4,29 GB sau khi verify) |
| `bronze` TIER=L | ✅ | **109.950.743 dòng**, 4,03 GB Parquet |
| `silver` TIER=L | ✅ | **109.820.004 dòng**, 8,97 GB Parquet |
| `quarantine` TIER=L | ✅ | **130.739 dòng** (toàn bộ do R7 dedupe) |
| DQ report tier L | ✅ | `results/dq_report_silver.csv` + `.md` |
| `gold` TIER=L | ✅ | **6/6 bảng** hoàn tất (xem dưới) |
| `analytics` TIER=L | ✅ | `results/tier_l/` — KPI, funnel, DAU, cohort, RFM/KMeans |
| `plots` TIER=L | ✅ | 8 PNG trong `docs/figures/tier_l/` |

### Bất biến đã kiểm chứng

```
109.950.743 (bronze) = 109.820.004 (silver) + 130.739 (quarantine) → OK
```

### Bảng Gold tier L

| Bảng | Kết quả | Kiểm tra |
|---|---|---|
| `fact_sessions` | **23.017.602 phiên**, 1,39 GB | Grain `(user_id,user_session)` **0 trùng lặp** ✅ |
| `agg_daily` | 854 dòng (61 ngày × 14 danh mục) | Revenue khớp `fact_sessions` ✅ |
| `agg_hourly` | 168 dòng (7×24) | `day_of_week` đủ 1–7 ✅ |
| `agg_funnel` | 14 dòng | Theo `category_l1` ✅ |
| `dim_user` | 5.316.649 | ✅ |
| `dim_product` | 206.876 | ✅ |

**Khoảng thời gian phủ**: `2019-10-01` → `2019-11-30` (đủ 2 tháng, điều kiện để cohort có ý nghĩa).

### Kết quả analytics tier L

| Chỉ số | Giá trị |
|---|---|
| Revenue | 505.126.293,44 |
| Sessions | 23.017.602 |
| Conversion rate | 0,0610 (6,10%) |
| Cart abandonment | 0,5941 (59,41%) |
| AOV proxy | 360,10 |
| Cohort | **9 cohort, quan sát tới T+8** (tier M chỉ 4 tuần) |
| KMeans | k=2 được chọn; `stability_spread` = 0,54% (< 5%) |

> ⚠️ **Phát hiện dữ liệu quan trọng**: tháng 11/2019 **không có purchase nào vào ngày 15/11**
> trong dữ liệu gốc, rồi 16/11 (68.247) và 17/11 (185.195) vọt gấp 3× và 8× bình thường.
> Đã kiểm chứng ở tầng **bronze** (chưa qua lọc) → **lỗi ở nguồn REES46, không phải pipeline**.
> 3 ngày này chiếm **16,1% doanh thu** và **15,3% purchase** của tier L.
> Chi tiết + đánh giá tác động: `docs/report/m4-chuong7-demo-ket-qua.md` §7.6.2.

---

## 1b. ⚠️ CẢNH BÁO: lake hiện chứa dữ liệu TIER L, không còn là tier S/M

`configs/pipeline.yaml` dùng **chung một đường dẫn lake** cho mọi tier:

```
bronze → /app/data/lake/bronze/events
silver → /app/data/lake/silver/events
gold   → /app/data/lake/gold
```

Khi chạy tier L, bước bronze/silver/gold đã **ghi đè** lake cũ. Kiểm chứng hiện tại:

| Kiểm tra | Giá trị |
|---|---|
| `silver` ngày nhỏ nhất → lớn nhất | `2019-10-01` → `2019-11-30` |
| Số ngày trong silver | **61** (2 tháng) |

**Hệ quả cần biết:**

- `results/kpi_summary.csv` (tier S) và `results/tier_m/` **vẫn còn đúng** — chúng được tính
  lúc lake còn đúng dữ liệu tương ứng, và đã lưu ra file riêng.
- **NHƯNG nếu ai chạy lại `make analytics TIER=S` bây giờ, kết quả sẽ SAI** — vì nó đọc lake
  2 tháng nhưng ghi vào `results/`, đè lên số tier S đúng.

**Cách xử lý nếu cần chạy lại tier S hoặc M:**

```powershell
# Phải dựng lại lake theo tier tương ứng TRƯỚC khi chạy analytics
make bronze TIER=S && make silver TIER=S && make gold TIER=S
make analytics TIER=S
# Lưu ý: lệnh này sẽ ghi đè lake tier L đang có — muốn giữ tier L thì
# copy data/lake ra chỗ khác trước, hoặc tạo config riêng cho từng tier
# (xem mẫu configs/pipeline-tier-l.yaml và pipeline-m5-tier-m.yaml).
```

> **Đề xuất cho M1 (hạ tầng)**: tách lake theo tier (`bronze_s`, `bronze_m`, `bronze_l`)
> hoặc dùng subdirectory, để các tier không ghi đè nhau. Hiện tại `pipeline-m5-tier-m.yaml`
> đã làm đúng (dùng `bronze_m`, `silver_m`, `gold_m`) — có thể áp dụng cùng mẫu cho tier L.

---

## 2. Vì sao dừng

Ổ C: chỉ còn **6,3 GB** khi gold đang ghi. Nguy cơ:
- Ghi dở → Parquet hỏng, mất luôn phần đã xong.
- Windows hết chỗ → Docker Desktop crash như đã xảy ra 1 lần ở bước silver.

**Đã giải phóng**: xoá zip 4,29 GB → còn **10,1 GB**.

**Ước tính cần thêm**: gold đầy đủ ~2,5 GB (agg_* nhỏ, chủ yếu là fact_sessions 1,39 GB đã có).
Analytics tier L đọc gold + ghi `results/` ~vài trăm MB. **10,1 GB là đủ để chạy tiếp gold + analytics.**

Nếu vẫn thiếu, giải phóng thêm bằng một trong các cách ở §5.

---

## 3. Máy đang ở trạng thái nào

| Thành phần | Trạng thái |
|---|---|
| Docker containers | **ĐÃ DỪNG** (`docker compose stop`) — cần `up -d` lại |
| `data/lake/bronze` | 4,03 GB — còn nguyên |
| `data/lake/silver` | 8,97 GB — còn nguyên |
| `data/lake/quarantine` | ~0 GB — còn nguyên |
| `data/lake/gold` | 1,51 GB — 3/6 bảng, **giữ nguyên, chạy lại sẽ ghi đè** |
| `data/raw/*.csv` | 13,67 GB — còn nguyên (cần cho lần chạy lại nếu muốn) |
| `docker/.env` | `SPARK_WORKER_MEMORY=4G` (đã chỉnh, xem §4) |

---

## 4. Hai lỗi đã gặp và cách khắc phục (quan trọng khi chạy lại)

### Lỗi 1 — Executor bị OOM-kill (`exit code 143`)

Lần chạy silver đầu tiên chết với `Executors Lost: Command exited with code 52`,
sau đó `KILLED exitStatus 143`.

**Nguyên nhân**: `docker/.env` để `SPARK_WORKER_MEMORY=2G`, không đủ cho 110 triệu dòng.
Tôi đã tăng lên `6G` → **tệ hơn**, vì Docker VM chỉ được cấp **16,6 GB**
(`docker info --format '{{.MemTotal}}'`); 2 worker × 6G + driver 3G vượt hạn mức → OOM lại.

**Cách đúng đã dùng được**: `SPARK_WORKER_MEMORY=4G` + tham số submit:
```
--conf spark.driver.memory=2g
--conf spark.executor.memory=3g
--conf spark.executor.cores=2
--conf spark.sql.shuffle.partitions=120
```
→ silver chạy xong trong **1185 giây (~20 phút)**.

### Lỗi 2 — Cổng 8080 bị chiếm

`docker compose up` báo `bind: Only one usage of each socket address` vì tiến trình
`gateway.exe` (nexus-runtime) giữ cổng 8080.

**Đã sửa**: `docker-compose.yml` đổi sang `"8089:8080"`.
→ Spark Master UI giờ ở **http://localhost:8089** (không còn 8080).
✅ Đã cập nhật toàn bộ tài liệu ghi `localhost:8080` → `8089`
(Makefile, run.ps1, README, phan-cong-nhom.md, báo cáo M1, nhật ký hieu-w1).

---

## 5. Lệnh chạy tiếp (làm đúng thứ tự)

```powershell
# 1. Bật cluster
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps      # phải thấy 4 container Up

# 2. Chạy gold (ghi đè, hoàn tất 3 bảng còn thiếu)
docker compose -f docker/docker-compose.yml exec -T -e CONFIG_PATH=/app/configs/pipeline.yaml spark-master `
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 `
  --conf spark.app.name=gold_L --conf spark.driver.memory=2g `
  --conf spark.executor.memory=3g --conf spark.sql.shuffle.partitions=120 `
  /app/src/processing/silver_to_gold.py --tier L

# 3. Chạy analytics tier L  ← đây là mục W3 mà M4 cần
docker compose -f docker/docker-compose.yml exec -T -e CONFIG_PATH=/app/configs/pipeline.yaml spark-master `
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 `
  --conf spark.app.name=analytics_L --conf spark.driver.memory=2g `
  --conf spark.executor.memory=3g `
  /app/src/analytics/run_analytics.py --tier L

# 4. Vẽ biểu đồ tier L
docker compose -f docker/docker-compose.yml exec -T spark-master bash -c "cd /app && python3 src/analytics/plots.py --tier L"
```

> **Lưu ý về `make`**: `make gold TIER=L` dùng `$(SPARK_SUBMIT)` không truyền
> `--conf spark.executor.memory`, nên sẽ dùng mặc định của cluster (4G/worker).
> Nếu vẫn OOM, dùng trực tiếp lệnh `spark-submit` ở trên có tham số rõ ràng.

---

## 6. Nếu vẫn thiếu dung lượng

Xếp theo mức độ an toàn (an toàn nhất trước):

| Cách | Giải phóng | Rủi ro |
|---|---|---|
| Xoá `data/raw/2019-Oct.csv` (5,28 GB) | 5,28 GB | ⚠️ Cần lại nếu muốn chạy lại bronze — nhưng bronze đã có, chỉ mất khi `make clean-all` |
| Xoá `data/raw/2019-Nov.csv` (8,39 GB) | 8,39 GB | ⚠️ **Không xoá** nếu còn định chạy lại silver tier L |
| Xoá `data/lake/bronze` (4,03 GB) | 4,03 GB | ⚠️ Mất khả năng chạy lại silver mà không đọc lại CSV |
| `docker system prune -a` | ~2,7 GB (build cache) | ✅ An toàn, chỉ mất cache build |
| Di chuyển `data/` sang ổ khác rồi symlink | — | ✅ An toàn nhưng cần ổ còn trống |

**Khuyến nghị**: chạy `docker system prune -a --volumes` trước (giải phóng ~2,7 GB cache),
rồi thử gold. Nếu vẫn thiếu mới xoá `2019-Oct.csv` (bronze đã có nên không cần).

---

## 7. Việc còn lại của M4 sau khi tier L chạy xong

| # | Việc | Ghi chú |
|---|---|---|
| 1 | Chạy `run_analytics.py --tier L` | Sinh `results/kpi_summary.csv`, `cohort_retention/`, ... cho tier L |
| 2 | **Kiểm tra cohort đủ tuần** | Đây là mục đích chính của W3: có tháng 10 + 11 → cohort ≥8 tuần thay vì 4 |
| 3 | Chạy `plots.py --tier L` | Sinh `docs/figures/tier_l/` |
| 4 | Cập nhật Chương 7 §7.6 | Thay phần "hạn chế 1 tháng" bằng số cohort thật |
| 5 | Cập nhật README bảng KPI | Nếu nhóm muốn dẫn số tier L |
| 6 | Chạy lại `make analytics` tier S/M | Để sinh `funnel_totals.csv` + cột `selected` trong `kmeans_scores.csv` |

> ⚠️ **Chưa có `results/tier_l/`** — `configs/pipeline.yaml` có tier L nhưng
> `paths.results` mặc định là `/app/results`, **ghi đè kết quả tier S**.
> **Phải tạo `configs/pipeline-tier-l.yaml`** với `results: /app/results/tier_l`
> trước khi chạy analytics, nếu không sẽ mất kết quả tier S đang có.
>
> ```yaml
> # configs/pipeline-tier-l.yaml
> paths:
>   raw:         /app/data/raw
>   bronze:      /app/data/lake/bronze/events
>   silver:      /app/data/lake/silver/events
>   quarantine:  /app/data/lake/quarantine/events
>   gold:        /app/data/lake/gold
>   results:     /app/results/tier_l
> tiers:
>   L:
>     months: ["2019-Oct", "2019-Nov"]
> analytics:
>   rfm_as_of: "2019-12-01"    # ngày kế tiếp ngày cuối dữ liệu tier L
>   kmeans_k_range: [2, 10]
>   kmeans_seed: 42
>   kmeans_max_iter: 20
> ```
> File này **chưa được tạo** — cần làm ở bước 3.

---

## 8. Tóm tắt một dòng

> Bronze + Silver + DQ report tier L **đã xong và kiểm chứng bất biến**;
> Gold còn 3 bảng `agg_*`; dung lượng đã giải phóng về 10,1 GB đủ để chạy tiếp.
> Việc còn lại: chạy gold → tạo `pipeline-tier-l.yaml` → chạy analytics + plots tier L.
