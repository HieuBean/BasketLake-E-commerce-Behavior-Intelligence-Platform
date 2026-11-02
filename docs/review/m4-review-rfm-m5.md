# Review RFM / KMeans (M5) — bởi M4

> **Người review**: M4 (Analytics — KPI/Funnel/Cohort)
> **Người sở hữu module**: M5 (`src/analytics/rfm_kmeans.py`)
> **Phạm vi**: `build_rfm`, `run_kmeans`, `assign_business_labels`, `check_kmeans_stability`
> **Số liệu dùng để review**: `results/kmeans_scores.csv`, `results/kmeans_stability.csv`,
> `results/kmeans_stability_k4.csv`, `results/cluster_profiles/`, `results/tier_m/*` — tất cả đã có sẵn trong repo, không cần chạy lại.
>
> **Cách đọc**: mỗi phát hiện ghi rõ mức độ · bằng chứng · cách sửa đề xuất.
>
> **TRẠNG THÁI: PHẦN LỚN ĐÃ SỬA** (M4 thực hiện theo yêu cầu của nhóm) — xem
> §"Đã sửa gì trong đợt này" ngay dưới đây. Phần phân tích gốc giữ nguyên để truy vết.

---

## Đã sửa gì trong đợt này

| # | Phát hiện | Cách sửa đã áp dụng | Test |
|---|---|---|---|
| 1 | Chọn `k` theo `argmax(silhouette)` | Thêm **cổng ổn định**: chỉ chọn `k` trong các `k` có `stability_spread ≤ 5%`. Tách logic thuần sang `src/analytics/kmeans_selection.py` để test được mà không cần Spark | 11 test `tests/test_kmeans_stability.py` |
| 2 | `rfm_as_of` lệch +30 ngày | ⏸ **CHƯA SỬA** — đổi sẽ làm thay đổi toàn bộ kết quả đã công bố; cần nhóm chốt trước | — |
| 3 | `check_kmeans_stability` hardcode `maxIter=20` | Thêm tham số `max_iter`; `run_analytics.py` truyền từ `kmeans_max_iter` | phủ bởi test import/signature |
| 4 | Nhánh nhãn 4 cụm là dead code | Tự hết khi Phát hiện 1 được sửa — cổng ổn định giờ chọn `k=4` ở tier S nên nhánh 4 nhãn sẽ chạy | — |
| 5 | Số seed thử bị giảm âm thầm | Log rõ `Số seed thử cho mỗi k: N → [...]` | — |

**Kiểm chứng**: `make test` trong container → **59/59 PASSED**;
`ruff check src/ experiments/ tests/` → sạch.

**Việc còn lại**: Phát hiện 2 (`rfm_as_of`). Đổi giá trị này bắt buộc phải chạy lại
`make analytics` cả tier S và M, rồi cập nhật `docs/kiem-toan-m5.md` + báo cáo Chương 6.
Cần nhóm chốt trước khi làm vì nó làm đổi mọi con số RFM đã công bố.

---

## Kết luận tổng quan (phân tích gốc, trước khi sửa)

Code chạy được, cấu trúc tốt, đã có log-transform + StandardScaler + kiểm tra đa seed — vượt yêu cầu cơ bản. **Nhưng có một lỗi phương pháp nghiêm trọng**: quy tắc chọn `k` và bộ máy kiểm tra độ ổn định **mâu thuẫn nhau**, dẫn tới việc công bố đúng nghiệm **kém tái lập nhất**. Lỗi này M5 đã tự ghi nhận trong `docs/kiem-toan-m5.md` nhưng **code chưa được sửa** và **README vẫn đang công bố số cụm 2**.

| # | Phát hiện | Mức độ |
|---|---|---|
| 1 | Chọn `k` theo `argmax(silhouette)` → chọn đúng nghiệm bất ổn nhất | 🔴 Nghiêm trọng |
| 2 | `rfm_as_of = 2019-11-30` lệch +30 ngày so với ngày cuối của dữ liệu | 🟡 Trung bình |
| 3 | `check_kmeans_stability` hardcode `maxIter=20`, bỏ qua cấu hình | 🟡 Trung bình |
| 4 | Nhánh gán nhãn 4 cụm là dead code (không bao giờ chạy) | 🟡 Trung bình |
| 5 | `dict.fromkeys([seed, 0, 123])` âm thầm giảm số seed thử | 🟢 Thấp |

---

## 🔴 Phát hiện 1 — Quy tắc chọn `k` chọn đúng nghiệm kém tái lập nhất

### Cách code đang làm

`run_kmeans` chọn `k` theo silhouette lớn nhất:

```python
if silhouette > best_score:
    best_score = silhouette
    best_k = k
    best_model = model
```

Trong mỗi `k`, nó chọn seed có **inertia thấp nhất**:

```python
model, selected_seed = min(candidates, key=lambda pair: pair[0].summary.trainingCost)
```

### Bằng chứng

**Tier S — `results/kmeans_scores.csv`**: silhouette cao nhất ở `k=2` (0,835945), vượt xa mọi `k` khác (k=3: 0,5254; k=4: 0,5498; còn lại ~0,51). Vậy `k=2` luôn thắng.

**Tier S — độ ổn định**, ghép cụm theo **thứ hạng monetary** (bất biến duy nhất, vì cluster ID bị hoán vị giữa các seed — chính comment trong code thừa nhận):

| k | Silhouette | Kích thước cụm qua 3 seed | Spread lớn nhất | Kết luận |
|---|---|---|---|---|
| **2** | **0,8359** ← được chọn | `[904, 7411, 7447]` / `[16342, 9835, 9799]` | **124,5%** | ❌ Không tái lập |
| **4** | 0,5498 ← bị loại | `[904, 904, 904]` / `[6939, 6967, 6859]` / `[5348, 5332, 5371]` / `[4055, 4043, 4112]` | **1,7%** | ✅ Tái lập tốt |

Cụm chi tiêu cao ở `k=2` có **904 người** với seed 0, nhưng **7.411** với seed 42 và **7.447** với seed 123 — chênh **8,2 lần**. Đây không phải phân khúc khách hàng, đây là sản phẩm phụ của khởi tạo ngẫu nhiên.

### Hệ quả cụ thể

`results/cluster_profiles/` đang công bố:

```
cluster,user_count,median_recency,median_frequency,median_monetary,segment_name
1,904,41,2,504.42,VIP / Champions
0,16342,45,1,175.04,Other buyers (needs further segmentation)
```

Đây chính là nghiệm `seed=0` — nghiệm **lệch nhất** trong ba seed. Nếu chạy lại với seed 42, "VIP / Champions" sẽ có 7.411 người thay vì 904 (**gấp 8,2 lần**). Con số trên README (`KMeans clusters | 2`) và trong báo cáo Chương 6 vì thế không tái lập được.

### Vì sao silhouette cao bất thường lại là dấu hiệu xấu

Silhouette 0,8359 cho `k=2` là **quá cao** so với dữ liệu hành vi thật (thường 0,3–0,6). Nguyên nhân: một cụm rất nhỏ (904 người, ~5%) nằm tách hẳn về một phía, kéo silhouette trung bình lên. Đây là kiểu "degenerate split" — silhouette thưởng cho việc tách một nhóm nhỏ ra xa, không thưởng cho cấu trúc nghiệp vụ có ích.

### Cách sửa đề xuất

Thêm **cổng ổn định** vào quy tắc chọn `k`: chỉ chấp nhận `k` nếu phân khúc tái lập được qua các seed.

```python
# Trong run_kmeans, sau khi có score_rows cho tất cả k:
STABILITY_TOLERANCE = 0.05   # 5% biến động kích thước cụm tối đa

def _cluster_size_spread(rfm_scaled, k, seeds=(42, 0, 123)) -> float:
    """Độ lệch kích thước cụm (theo thứ hạng monetary) giữa các seed."""
    sizes_by_rank = []
    for s in seeds:
        preds = KMeans(k=k, seed=s, maxIter=max_iter, featuresCol="features").fit(rfm_scaled)
        prof = (preds.select(F.col("prediction").alias("cluster"), "monetary")
                     .groupBy("cluster").agg(F.count("*").alias("n"),
                                             F.percentile_approx("monetary", 0.5).alias("m"))
                     .orderBy(F.desc("m")).collect())
        sizes_by_rank.append([r["n"] for r in prof])
    worst = 0.0
    for rank in range(k):
        vals = [row[rank] for row in sizes_by_rank]
        worst = max(worst, (max(vals) - min(vals)) / (sum(vals) / len(vals)))
    return worst

# Chọn k: silhouette cao NHẤT trong các k ĐẠT cổng ổn định
stable_ks = [k for k in k_range if _cluster_size_spread(rfm_scaled, k) <= STABILITY_TOLERANCE]
pool = [r for r in score_rows if r["k"] in stable_ks] or score_rows
best = max(pool, key=lambda r: r["silhouette"])
```

Với dữ liệu hiện có, cổng này sẽ loại `k=2` ở tier S (spread 124,5%) và chọn `k=4` (spread 1,7%, silhouette 0,5498) — khớp đúng với khuyến nghị M5 đã viết trong `docs/kiem-toan-m5.md`.

**Lưu ý quan trọng — tier M không bị lỗi này**:

| Tier | k được chọn | Spread giữa 3 seed | Đánh giá |
|---|---|---|---|
| S | 2 (silhouette 0,8359) | **124,5%** | ❌ |
| M | 2 (silhouette 0,5801) | **1,1%** | ✅ Ổn định |

Ở tier M, `k=2` tái lập tốt (`79753 / 79216 / 80060` và `267365 / 267902 / 267058`). Vậy **quy tắc argmax-silhouette chỉ sai ở tier S**. Đề xuất: hoặc thêm cổng ổn định (an toàn cho mọi tier), hoặc tối thiểu **bỏ con số "2 clusters" khỏi phần KPI chính của README** và dẫn số tier M.

---

## 🟡 Phát hiện 2 — `rfm_as_of` lệch +30 ngày so với dữ liệu

### Bằng chứng

| Nguồn | Giá trị |
|---|---|
| `configs/pipeline.yaml → analytics.rfm_as_of` | `"2019-11-30"` |
| `event_date` nhỏ nhất / lớn nhất (tier M) | `2019-10-01` … **`2019-10-31`** |

`build_rfm` tính:

```python
as_of = F.to_date(F.lit(as_of_date))
F.datediff(as_of, F.max("last_purchase_ts")).alias("recency")
```

Vì `as_of` = 30/11 nhưng dữ liệu kết thúc 31/10, **mọi user bị cộng thêm đúng 30 ngày recency**. Đo được trên bản công bố: `median_recency` = 36 và 45 ngày, trong khi tính đúng theo ngày cuối dữ liệu phải là **6 và 15 ngày**.

### Vì sao có hại

`run_kmeans` áp `log1p(recency)` **trước khi** StandardScaler. Một phép dịch hằng số làm thay đổi **hình dạng** phân phối sau log, không chỉ vị trí:

| Recency gốc | `log1p` khi as_of=30/11 | `log1p` khi as_of=01/11 |
|---|---|---|
| 6 | log1p(36) = 3,61 | log1p(6) = 1,95 |
| 15 | log1p(45) = 3,83 | log1p(15) = 2,77 |
| **Khoảng cách giữa 2 cụm** | **0,22** | **0,82** |

Chiều Recency bị **nén ~3,7 lần** so với thiết kế đúng. Vì StandardScaler chuẩn hoá theo phương sai, việc nén này làm Recency mất trọng số tương đối so với Frequency và Monetary trong không gian đặc trưng — đúng lúc Recency là chiều phân biệt "khách mới" với "khách ngủ đông".

### Cách sửa đề xuất

```yaml
# configs/pipeline.yaml
analytics:
  rfm_as_of: "2019-11-01"   # ngày kế tiếp ngày cuối của dữ liệu (2019-10-31)
```

> ⚠️ **Đây là thay đổi có hệ quả rộng**: đổi `rfm_as_of` sẽ thay đổi toàn bộ
> `cluster_profiles`, `kmeans_scores`, `kmeans_stability`, `user_segments`, và mọi con số
> RFM trong báo cáo Chương 6 / `docs/kiem-toan-m5.md`. Cần **M5 chủ trì** và chạy lại trên
> cả tier S và M trước khi merge. Nếu nhóm quyết định giữ 30/11 để khỏi chạy lại, thì phải
> **ghi rõ trong báo cáo**: "Recency tính tới 30/11/2019, tức mọi user được cộng thêm 30 ngày;
> điều này không đổi thứ hạng Recency nhưng làm giảm trọng số của chiều Recency sau log1p".

---

## 🟡 Phát hiện 3 — `check_kmeans_stability` bỏ qua cấu hình `max_iter`

```python
def check_kmeans_stability(rfm, k, seeds=None):
    ...
    for seed in seeds:
        km = KMeans(k=k, seed=seed, maxIter=20, featuresCol="features")   # ← hardcode
```

Trong khi `run_kmeans` nhận `max_iter` từ `pipeline.yaml`:

```python
KMeans(k=k, seed=initial_seed, maxIter=max_iter, featuresCol="features")
```

Hiện tại cả hai đều bằng 20 nên chưa sai số. Nhưng nếu nhóm tinh chỉnh `kmeans_max_iter` (ví dụ 50 để hội tụ tốt hơn), thì **kết quả kiểm tra ổn định sẽ được đo ở cấu hình khác với mô hình chính** — so sánh không còn công bằng.

**Sửa**: thêm tham số `max_iter` và truyền từ `run_analytics.py`.

```python
def check_kmeans_stability(rfm, k, seeds=None, max_iter=20):
    ...
    km = KMeans(k=k, seed=seed, maxIter=max_iter, featuresCol="features")
```

---

## 🟡 Phát hiện 4 — Nhánh gán nhãn 4 cụm là dead code

```python
if len(pdf) < 4:
    lbl = "VIP / Champions" if m >= med_m else "Other buyers (needs further segmentation)"
elif m >= med_m and r <= med_r:
    lbl = "VIP / Champions"
...
```

Vì `k=2` luôn thắng theo argmax-silhouette, `len(pdf)` luôn = 2 `< 4`, nên **toàn bộ logic 4 nhãn
(VIP / At Risk / New / Hibernating) không bao giờ chạy**. Kết quả công bố chỉ có 2 nhãn, trong đó
một nhãn là `"Other buyers (needs further segmentation)"` — tức chính code tự thừa nhận chưa phân
khúc được.

Đây là **hệ quả trực tiếp của Phát hiện 1**, không phải lỗi riêng. Khi sửa Phát hiện 1 (chọn `k=4`
ở tier S), nhánh này sẽ hoạt động và cho ra 4 nhãn nghiệp vụ có ích.

**Kiểm chứng bổ sung**: bảng `k=4` cho thấy 4 cụm có profile phân biệt rõ và ổn định:

| Hạng monetary | Kích thước (3 seed) | Monetary | Recency | Nhãn nghiệp vụ phù hợp |
|---|---|---|---|---|
| 1 | 904 / 904 / 904 | 504,42 | 41 | VIP / Champions (rất ổn định) |
| 2 | 6.967 / 6.939 / 6.859 | ~254 | 51 | Chi tiêu khá, lâu chưa mua → At Risk |
| 3 | 5.332 / 5.348 / 5.371 | ~228 | 36 | Mới mua, chi tiêu khá → Loyal / Promising |
| 4 | 4.043 / 4.055 / 4.112 | ~42 | 46 | Chi tiêu thấp → Hibernating |

Đây là phân khúc **có ý nghĩa nghiệp vụ và tái lập được** — tốt hơn hẳn 2 cụm hiện tại.

---

## 🟢 Phát hiện 5 — Số seed thử có thể bị giảm âm thầm

```python
for initial_seed in dict.fromkeys([seed, 0, 123])
```

`dict.fromkeys` khử trùng lặp. Nếu ai đó đặt `kmeans_seed: 0` hoặc `123` trong `pipeline.yaml`,
danh sách chỉ còn **2 seed** thay vì 3, và bảng `kmeans_stability` vẫn ghi như thể đủ 3. Không sai
số hiện tại (`seed = 42`), nhưng nên tách rõ:

```python
candidate_seeds = list(dict.fromkeys([seed, 0, 123]))
log.info(f"Số seed thử cho mỗi k: {len(candidate_seeds)} → {candidate_seeds}")
```

---

## Những điểm đã kiểm tra và **ĐẠT** (không cần sửa)

| Hạng mục | Kết quả |
|---|---|
| Log-transform `log1p` trước KMeans | ✅ Đúng — RFM lệch phải, `log1p` tránh `log(0)` |
| `StandardScaler(withStd=True, withMean=True)` | ✅ Đúng — 3 chiều khác đơn vị (ngày / lần / USD) |
| Chỉ lấy user `converted = True` cho RFM | ✅ Đúng — user chỉ view không có R/F/M |
| `ClusteringEvaluator` dùng `distanceMeasure` mặc định `squaredEuclidean` | ✅ **Không phải lỗi** — Spark 3.5 chỉ hỗ trợ `squaredEuclidean` và `cosine`; `euclidean` không tồn tại. Code dùng mặc định là hợp lệ |
| `percentile_approx` cho median | ✅ Phù hợp ở quy mô dữ liệu lớn |
| Chọn seed theo inertia thấp nhất trong từng `k` | ✅ Hợp lý — KMeans phụ thuộc khởi tạo, lấy nghiệm tốt nhất là chuẩn |
| Ghi `kmeans_scores.csv` có cột `selected_seed` | ✅ Tốt — minh bạch, cho phép review này tái lập |
| Comment cảnh báo cluster ID bị hoán vị | ✅ Tốt — chính là cơ sở để tôi ghép cụm theo thứ hạng |
| `docs/kiem-toan-m5.md` ghi nhận bất ổn seed | ✅ Rất tốt — M5 đã tự phát hiện và ghi trung thực |

---

## Việc cần quyết định (cập nhật sau khi sửa)

| # | Việc | Trạng thái |
|---|---|---|
| 1 | Thêm cổng ổn định vào quy tắc chọn `k` | ✅ **Đã sửa** — `kmeans_selection.select_k`, chọn `k=4` ở tier S |
| 2 | Sửa README, bỏ "2 clusters" khỏi KPI chính | ⏸ Chờ chạy lại `make analytics` để có `cluster_profiles` mới rồi mới cập nhật đúng số |
| 3 | Quyết định `rfm_as_of` = `2019-11-01` hay giữ `2019-11-30` | 🟡 **Cần nhóm chốt** — đổi giá trị làm đổi mọi số RFM đã công bố |
| 4 | Truyền `max_iter` vào `check_kmeans_stability` | ✅ **Đã sửa** |
| 5 | Log số seed thực dùng mỗi `k` | ✅ **Đã sửa** |

### Cách chạy lại khi nhóm chốt `rfm_as_of`

```bash
# 1. Sửa configs/pipeline.yaml → analytics.rfm_as_of: "2019-11-01"
#    (và configs/pipeline-m5-tier-m.yaml nếu chạy tier M)

# 2. Chạy lại analytics (cần gold tier S/M đã có)
make analytics TIER=S
make analytics TIER=M PIPELINE_CONFIG=/app/configs/pipeline-m5-tier-m.yaml

# 3. Vẽ lại biểu đồ
make plots TIER=S
make plots TIER=M PIPELINE_CONFIG=/app/configs/pipeline-m5-tier-m.yaml
```

Sau bước 2, các file thay đổi: `results/kmeans_scores.csv` (thêm cột
`stability_spread`, `selected`), `results/kmeans_stability.csv`, `results/cluster_profiles/`,
và tương đương trong `results/tier_m/`. Khi đó mới cập nhật được README và
`docs/kiem-toan-m5.md` cho khớp.
