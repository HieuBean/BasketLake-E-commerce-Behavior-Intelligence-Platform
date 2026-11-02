"""
src/analytics/kmeans_selection.py
Owner: M5 (Analytics 2) — tách logic thuần bởi M4 trong đợt review.

Logic THUẦN (không phụ thuộc Spark) để chọn k cho KMeans:

  1. Đo độ ổn định của một k qua nhiều seed: kích thước cụm thay đổi bao nhiêu
     khi đổi seed khởi tạo (ghép cụm theo thứ hạng Monetary).
  2. Chọn k = silhouette cao nhất **trong các k đạt cổng ổn định**.

Vì sao tách riêng file này?
---------------------------
`rfm_kmeans.py` import `pyspark.ml`, nên mọi hàm trong đó chỉ test được khi có
Spark + JVM. Phần quyết định chọn k là toán học thuần (so sánh số, lấy max),
không cần Spark — tách ra để test bằng pytest thường, chạy được cả trên máy
không có Spark và trong CI không dựng cluster.

Bối cảnh lỗi đã sửa
-------------------
`run_kmeans` trước đây chọn `argmax(silhouette)`. Trên tier S cách đó chọn k=2
(silhouette 0,8359) nhưng cụm chi tiêu cao chứa 904 / 7.411 / 7.447 người tùy
seed — lệch 124,5%, không tái lập được. k=4 có silhouette 0,5498 nhưng lệch
tối đa 1,7%. Cổng ổn định loại k=2 và chọn k=4.
"""
from __future__ import annotations

#: Ngưỡng mặc định cho biến động kích thước cụm giữa các seed.
#: 0.05 = 5% — chặt hơn mức lệch thật của các k ổn định (1,1–1,7% ở tier S/M)
#: nhưng lỏng hơn nhiều so với mức lệch của nghiệm degenerate (124,5% ở tier S).
DEFAULT_STABILITY_TOLERANCE = 0.05


def stability_spread(sizes_by_seed: list[list[int]]) -> float:
    """
    Độ lệch kích thước cụm lớn nhất giữa các seed: ``(max - min) / mean``.

    Tham số
    -------
    sizes_by_seed : list[list[int]]
        Với mỗi seed, danh sách kích thước cụm **đã sắp theo thứ hạng
        median_monetary giảm dần**. Bắt buộc sắp theo thứ hạng vì cluster ID
        bị hoán vị giữa các seed (KMeans không đảm bảo thứ tự nhãn).

    Trả về
    ------
    float
        ``0.0`` nếu mọi seed cho cùng kích thước cụm; càng lớn càng bất ổn.
        Trả ``0.0`` khi đầu vào rỗng hoặc chỉ có một seed (không có gì so sánh).
    """
    if not sizes_by_seed:
        return 0.0
    n_clusters = min(len(s) for s in sizes_by_seed)
    worst = 0.0
    for rank in range(n_clusters):
        vals = [s[rank] for s in sizes_by_seed]
        mean = sum(vals) / len(vals)
        if mean > 0:
            worst = max(worst, (max(vals) - min(vals)) / mean)
    return worst


def select_k(
    score_rows: list[dict],
    tolerance: float = DEFAULT_STABILITY_TOLERANCE,
) -> tuple[dict, list[int]]:
    """
    Chọn k tốt nhất theo silhouette trong các k **đạt cổng ổn định**.

    Tham số
    -------
    score_rows : list[dict]
        Mỗi phần tử cần khoá ``k``, ``silhouette``, ``stability_spread``.
    tolerance : float
        Ngưỡng ``stability_spread`` tối đa được chấp nhận.

    Trả về
    ------
    (row, excluded_ks)
        ``row`` là dòng được chọn; ``excluded_ks`` là các k bị cổng loại.
        Nếu **không** k nào đạt ngưỡng, quay về ``argmax(silhouette)`` trên
        toàn bộ ``score_rows`` và ``excluded_ks`` sẽ là toàn bộ các k — người
        gọi phải log cảnh báo vì kết quả có thể không tái lập.
    """
    if not score_rows:
        raise ValueError("score_rows rỗng — không có k nào để chọn")

    stable = [r for r in score_rows if r["stability_spread"] <= tolerance]
    excluded = [r["k"] for r in score_rows if r["stability_spread"] > tolerance]

    pool = stable or score_rows
    return max(pool, key=lambda r: r["silhouette"]), excluded
