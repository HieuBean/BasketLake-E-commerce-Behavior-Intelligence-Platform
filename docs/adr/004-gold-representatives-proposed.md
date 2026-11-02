# ADR-004 — Gold representative values (APPROVED BY USER)

Ngày 2026-10-30. **APPROVED BY USER — Task 04B, phiên làm việc hiện tại.**
Evidence: người dùng yêu cầu: “Người dùng đã phê duyệt triển khai toàn bộ P1/P2/P3
và các quy tắc xử lý còn tồn đọng trong ADR-004.” Phê duyệt này bao gồm validity,
fallback, null ordering và brand độc lập. Không có bằng chứng M2/toàn nhóm duyệt;
không ghi nhận thay mặt họ. Giữ đường dẫn ADR cũ để không hỏng liên kết.

## Evidence và lý do

Task 01/03 tái hiện first phụ thuộc input/partition. Task 03 ghi rõ chờ duyệt;
Task 04 từng yêu cầu bằng chứng phê duyệt độc lập và chưa có khi thực hiện.
Task 04B hiện cung cấp phê duyệt rõ ràng, thay thế trạng thái pending trước đó.
ADR 001–003 nói về Spark/storage, không phê duyệt representative policy.

## Chính sách cuối cùng được duyệt

### P1 — fact_sessions.category_l1

Grain `(user_id,effective_session_id)` như Task 02. Ưu tiên category không null,
trim không rỗng và lower(trim(value)) khác unknown. Chọn mode chỉ trong nhóm
giá trị ưu tiên; tie giá trị gốc ASC theo Spark string ordering. Không có giá trị
ưu tiên thì trả literal `unknown`. Không trim/lower các giá trị hợp lệ trong output;
Silver chịu trách nhiệm chuẩn hóa. Phân loại validity chuẩn hóa trim/lower, không
gom hoặc sửa các giá trị hợp lệ verbatim; đây là chi tiết ADR được người dùng duyệt.

### P2 — dim_product category tuple

Chọn nguyên tuple `(category_id,category_l1,category_l2,category_l3)` đã xuất hiện.
Tuple hợp lệ khi category_id không null và
category_l1 hợp lệ theo P1; category_l2/l3 được null theo Silver schema.
Ưu tiên valid tuples rồi frequency DESC. Tie theo category_id numeric ASC,
l1/l2/l3 string ASC, nulls LAST cho từng cột. Không chọn từng cột độc lập.
Nếu mọi tuple không hợp lệ: giữ một observed tuple có frequency lớn nhất, tie
cùng thứ tự trên; giữ product kể cả tuple all-null. Không tổng hợp tuple unknown
mới nếu tuple đó không có trong nguồn. category_id khác null là điều kiện bắt buộc
để ưu tiên tuple; fallback giữ cả product có tuple all-null.

### P3 — dim_product.brand

Loại null/trim-empty/unknown khỏi tập ưu tiên, mode trong phần hợp lệ, tie string
ASC. Nếu không có phần hợp lệ, trả literal unknown. Giữ giá trị hợp lệ verbatim.
Brand chọn độc lập category tuple, có thể đến từ event khác. Output product không
được diễn giải là một record nguyên gốc; chỉ category tuple phải có thật trong nguồn.

## Thuật toán và tác động

Production trong src/processing/silver_to_gold.py: groupBy key + whole candidate,
count; Window partitionBy key orderBy validity DESC,count DESC,stable values ASC;
row_number=1. Không Python UDF, không Pandas, không global sort/collect dữ liệu lớn.
Giữ grain/public columns/types/order. Extra shuffle/window và joins có chi phí;
chưa benchmark. Production tính metrics riêng rồi join representatives duy nhất
theo grain, không tính first thừa. Tests giữ prototype làm oracle bổ sung.

Giữ price_min/max/avg, event/purchase/revenue/conversion/duration. Có thể đổi
category distribution khi consumer phân tích category chính; không đổi nghĩa
daily/funnel membership từ Silver. Không có benchmark hoặc speedup claim.

## Thực thi và giới hạn

Task 04B triển khai và kiểm thử theo policy này; evidence trong
docs/M3_SUMMARY.md. Không đổi public schema,
Silver R9, revenue rules hay modules M4/M5. Không promote validation output vào
Gold chính thức. Floating sums/averages vẫn có sai số double; deterministic
representatives không có nghĩa mọi numeric aggregate bit-for-bit giống nhau.
