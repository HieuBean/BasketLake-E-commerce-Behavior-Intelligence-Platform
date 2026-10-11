# Nhật ký thí nghiệm / quyết định

> Mỗi người tạo file riêng: `docs/log/<tên>-<tuần>.md`
> Ví dụ: `docs/log/hieu-w1.md`

## Mẫu entry thí nghiệm

```
## [E?] <tên thí nghiệm> — <ngày>

**Người chạy**: <tên>  
**git_sha**: <sha>  
**Tier**: M | **Cores**: 4 | **RAM**: 4G | **shuffle_partitions**: 200 | **AQE**: true

**Giả thuyết**: ...

**Lệnh đã chạy**:
\`\`\`bash
make silver TIER=M
\`\`\`

**Kết quả (3 lần)**: 120s, 118s, 122s → trung vị: 120s  
**Ảnh/log lưu tại**: docs/figures/e?_run1.png, results/experiment_log.csv

**Quan sát bất thường**: ...

**Kết luận**: ... (không vượt quá dữ liệu đo)
```

## Mẫu entry quyết định kỹ thuật

```
## [QĐ] <tiêu đề> — <ngày>

**Vấn đề**: ...  
**Lựa chọn xem xét**: A, B, C  
**Quyết định**: Chọn A vì ...  
**Hệ quả**: ...
```
