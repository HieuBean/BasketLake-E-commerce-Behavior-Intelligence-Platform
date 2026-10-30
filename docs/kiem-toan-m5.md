# Kiểm toán kết quả thành viên 5 — 24/10/2026

## Phạm vi và cách kiểm tra

Đối chiếu [phân công M5](phan-cong-nhom.md), [báo cáo Chương 2/6](baocao_chuong2_chuong6_m5.md), [nhật ký](log/m5-w1-w3.md), mã E5/E6/RFM, các CSV trong `results/` và các hình trong `docs/figures/`. Đếm dòng từ Parquet; tính lại KPI và RFM bằng DuckDB từ Silver; tính lại trung vị của ba lần chạy E5 từ `experiment_log.csv`. Tạo lại Gold và analytics tier S sau khi sửa Recency; sau đó tạo Gold và analytics tier M trong thư mục riêng.

| Hạng mục | Kết quả kiểm toán |
| --- | --- |
| Bronze/Silver/quarantine | Tier S: `1,060,438 = 1,060,392 + 46`; tier M: `42,448,764 = 42,418,544 + 30,220`. |
| E5 | Cả 40 ô trung vị (2 tier × 4 engine × 5 mức mẫu) trong báo cáo khớp ba bản ghi hợp lệ mới nhất/ô. Mỗi mức dùng cùng số dòng cho bốn engine. |
| Hình E5 | [Tier S](figures/e5_crossover_S.png) và [tier M](figures/e5_crossover_M.png) vẽ lại từ 40 trung vị; mỗi hình có ô phóng chi tiết. `e5_crossover.png` là bản sao của tier S. |
| E6 tier S | DuckDB tính lại: revenue `5,813,952.29`, sessions `921,388`, conversion rate `0.02070812`. Tệp Spark lưu `0.02070000` cho conversion rate, lệch `0.0392%`; cả ba KPI đạt ngưỡng `0.1%`. Xem [CSV E6](../results/e6_correctness_report.csv). |
| E6 tier M | DuckDB tính lại: revenue `229,933,212.63`, sessions `9,244,772`, conversion rate `0.06811208`. Tệp Spark lưu `0.06810000`, lệch `0.0177%`; cả ba KPI đạt ngưỡng `0.1%`. Xem [CSV E6 M](../results/tier_m/e6_correctness_report.csv). |
| RFM tier S | Có `17,246` người mua; tổng Frequency `18,361`, tổng Monetary `5,813,952.29`. Đối chiếu trực tiếp từng người với sự kiện purchase trong Silver cho **0 bản ghi lệch** ở Recency, Frequency hoặc Monetary (ngưỡng Monetary 0.01). |
| KMeans | [CSV điểm](../results/kmeans_scores.csv) và [hình](figures/kmeans_elbow_silhouette.png) cùng ghi silhouette cao nhất `0.835945` tại $k=2$; $k=4$ đạt `0.549776`. [Profile được chọn](../results/cluster_profiles/) có `904 + 16,342 = 17,246` người. Trung vị trong profile là ước lượng `percentile_approx`. |
| Seed | Với $k=2$, ba seed chia nhóm rất khác nhau. Với $k=4$, quy mô và trung vị nhóm gần nhau khi ghép theo Monetary, nhưng chưa so sánh mức trùng khớp thành viên. Xem [bảng $k=2$](../results/kmeans_stability.csv) và [bảng $k=4$](../results/kmeans_stability_k4.csv). |
| RFM tier M | Có `347,118` người mua. [Phép kiểm tra DuckDB](../experiments/validate_rfm.py) đối chiếu từng người từ Silver M với Gold M: `0` thiếu, `0` lệch Recency, `0` lệch Frequency và `0` lệch Monetary (ngưỡng 0.01 USD). |
| KMeans tier M | [Bảng điểm](../results/tier_m/kmeans_scores.csv) và [hình](figures/kmeans_elbow_silhouette_M.png) cùng ghi silhouette lớn nhất `0.580124` tại $k=2$. [Profile](../results/tier_m/cluster_profiles/) có `79,098 + 268,020 = 347,118` người. [Bảng seed](../results/tier_m/kmeans_stability.csv) cho thấy quy mô nhóm gần nhau giữa ba lần fit riêng. |
| Hình analytics M | Tám biểu đồ trong [figures/tier_m](figures/tier_m/) đã được tạo từ CSV trong `results/tier_m/`; đã mở kiểm tra trực quan hình profile và hình chọn k. Hình KMeans M dùng trong báo cáo được mở kiểm tra và khớp với bảng điểm. |

## Sai lệch đã sửa

1. `build_rfm` trước đây dùng `session_start` để tính Recency, dù tài liệu định nghĩa theo lần purchase cuối. Có năm phiên purchase qua ngày khác với ngày bắt đầu phiên. Gold nay lưu `last_purchase_ts`; RFM dùng timestamp đó. Đã tạo lại Gold, analytics tier S và Hình 6.7.
2. E6 trước đây làm tròn oracle conversion rate trước khi đối chiếu, khiến bảng hiện `0.0000%` sai lệch. Oracle nay giữ độ chính xác gốc; bảng hiện `0.0392%`, vẫn đạt ngưỡng.
3. Nhật ký và phần câu hỏi bảo vệ từng chứa kết luận cũ: $k=4$ tối ưu, KMeans ổn định, E6 chứng minh toàn pipeline đúng, Pandas sẽ hết RAM ở L/XL. Các câu này đã được sửa hoặc ghi rõ là chưa được kiểm chứng.
4. Biểu đồ E5 tier S trước đây ép các đường Pandas/DuckDB sát trục ngang; hình mới có ô phóng chi tiết. Hình KMeans nay ghi rõ “silhouette cao nhất” và nhắc xem bảng seed trước khi đánh giá độ ổn định.
5. Lệnh `make` trước đây không truyền cấu hình tier M vào container cho Bronze/Silver/Gold/analytics/E6. Makefile nay nhận `PIPELINE_CONFIG`; cấu hình M ghi kết quả vào `results/tier_m` để bảo toàn kết quả tier S. Đã thực thi Gold/analytics/E6 tier M.

## Giới hạn còn lại

- E5 là thời gian truy vấn trên tệp mẫu đã chuẩn bị; SparkSession và khâu tạo mẫu nằm ngoài phép đo, còn kết nối DuckDB và mở tệp Pandas nằm trong phép đo. Chỉ có ba lần lặp mỗi ô, chưa có khoảng tin cậy; `local[*]` và cluster không được cố định cùng tài nguyên. Kết luận “Spark Local nhanh hơn DuckDB ở 100% tier M” đúng cho các lần đo này, chưa phải lợi thế đầu cuối hay ngưỡng giao chính xác.
- E6 đối chiếu ba KPI trên **tier S và M**, ở độ chính xác đã lưu và ngưỡng 0.1%. Manifest Silver dựa trên đường dẫn/kích thước/thời điểm sửa tệp, không băm nội dung. Ba KPI này không đại diện cho toàn bộ chỉ số của pipeline.
- RFM/KMeans đã chạy trên **tier S và M**. Tier M có đối chiếu RFM từng người bằng DuckDB; chưa kiểm chứng chất lượng nhãn kinh doanh hoặc độ trùng thành viên cụm giữa các seed. Không có số đo E5 tier L/XL hoặc kiểm chứng OOM.
- Nghiệm $k=2$ có một nhóm nhỏ 5.2% và nhạy với seed. Kết quả $k=4$ ổn định hơn ở profile tổng hợp trong ba seed, nhưng chưa kiểm tra độ trùng thành viên, khả năng lặp lại trên thời gian khác hay hiệu quả nhãn tiếp thị.

## Kiểm thử

Chạy lại bộ test trong container Spark: **22/22 pass**. Ruff trên các tệp mã và test vừa sửa: **pass**. `git diff --check`: **pass**. E6 M 3/3 KPI pass; DuckDB đối chiếu RFM M 347.118/347.118 hồ sơ không lệch. Máy hiện không có GNU Make nên chưa chạy được `make -n`; các lệnh Spark, DuckDB và vẽ biểu đồ được chạy trực tiếp trong container với cùng `CONFIG_PATH`.
