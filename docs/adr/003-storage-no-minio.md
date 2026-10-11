# ADR-003: Storage giả lập — Parquet trên volume, không dùng MinIO

Trạng thái: **chấp nhận** | Ngày: 07/10/2026 | Người quyết: Nhóm (Ngày 0)

## Bối cảnh

Cần storage giả lập cho tầng bronze (vai Storage của pipeline lớn hơn).
Ban đầu cân nhắc MinIO (S3-compatible object storage).

## Vấn đề với MinIO

Từ tháng 10/2025, MinIO ngừng phát hành Docker image và binary cho bản community;
repository bị archive đầu năm 2026. Dùng MinIO cho dự án mới gây rủi ro tái lập.

## Quyết định

Dùng **Parquet trên Docker volume dùng chung** làm storage giả lập cho Core.
Phase 2 (cá nhân Hiếu): cân nhắc SeaweedFS hoặc Garage nếu cần S3 API.

## Lý do

1. Đơn giản, tái lập tốt, không phụ thuộc dịch vụ ngoài.
2. Phạm vi Core là Processing → Analytics; storage là điều kiện đầu vào.
3. Data contract đảm bảo Phase 2 có thể thay storage thật mà không đổi mã processing.

## Hệ quả

- `prepare_input` đóng vai Storage giả lập: đọc CSV, ghi Parquet bronze đúng contract.
- Khi tích hợp storage thật (Phase 2): chỉ cần ghi đúng contract, không đổi silver/gold.
