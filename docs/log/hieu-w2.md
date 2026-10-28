# Nhật Ký Tuần 2 & 3 (W2–W3) — Thí Nghiệm & Báo Cáo Đồ Án

**Thành viên**: Hiếu (M1)  
**Vai trò**: Hạ tầng Docker, Bronze Ingestion, Thí nghiệm E2 & E7  
**Thời gian**: Tuần 2 và Tuần 3

---

## [E2] Tăng Tài Nguyên, Cố Định Dữ Liệu (Resource Scaling & Amdahl's Law)

**Người chạy**: Hiếu (M1)  
**Tier**: M (Tháng 10/2019) | **Data**: `data/lake/silver/events`  
**Cấu hình so sánh**: `local[1]`, `local[2]`, `local[4]`, `local[*]`, `cluster` (2 workers)

**Giả thuyết**:
Thời gian thực thi giảm khi tăng số core từ 1 lên 4, nhưng hiệu suất song song (parallel efficiency) giảm dần do phần tuần tự (driver planning, DAG scheduling) và I/O shuffle theo Định luật Amdahl.

**Lệnh chạy**:
```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  spark-submit /app/experiments/e2_scale_resource.py --tier M
```

**Kỳ vọng kết quả**:
- `Speedup(n) = T(1) / T(n)`
- Thấy rõ điểm bão hòa khi worker cạnh tranh tài nguyên CPU/RAM vật lý trên cùng 1 máy host.

---

## [E7] MapReduce Mô Phỏng: Phân Tích Hiệu Quả Combiner

**Người chạy**: Hiếu (M1)  
**Tác vụ**: Tính tổng doanh thu theo `category_l1` cho các sự kiện `purchase`.  
**So sánh**:
1. MapReduce KHÔNG Combiner (phát toàn bộ cặp trung gian qua Shuffle).
2. MapReduce CÓ Combiner (tổng hợp cục bộ trước khi phát).

**Lệnh chạy**:
```bash
docker compose -f docker/docker-compose.yml exec spark-master \
  python3 /app/experiments/e7_mapreduce.py
```

**Công thức tính**:
$$\% \text{ giảm intermediate pairs} = \frac{\text{Cặp không combiner} - \text{Cặp có combiner}}{\text{Cặp không combiner}} \times 100\%$$

---

## [Checklist hoàn thành W2–W3]
- [x] Kiểm tra pipeline CI GitHub Actions (`.github/workflows/ci.yml`).
- [x] Tích hợp `experiments/harness.py` đo 3 lần lấy trung vị (median).
- [x] Hoàn thiện mã nguồn `experiments/e2_scale_resource.py` và `experiments/e7_mapreduce.py`.
- [x] Biên soạn đầy đủ Chương 1, Chương 4 và Phụ lục kỹ thuật tại `docs/bao-cao-m1-chuong-1-4-phu-luc.md`.
