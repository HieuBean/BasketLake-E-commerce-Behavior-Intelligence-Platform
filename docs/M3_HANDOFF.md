# M3 — Hướng dẫn bàn giao

## Đọc và kiểm tra

- Kết quả: [M3_SUMMARY](M3_SUMMARY.md).
- Kiểm chứng offline, không đọc lake hoặc submit Spark:
  `python -m tests.run_m3_tests audit-delivery`.
- Kiểm chứng original results/manifests/hashes nếu còn dataset local:
  `python -m tests.run_m3_tests audit`.
- Gold/Tier L/E1 đã hoàn tất; **không cần chạy lại benchmark để bàn giao**.
- README, AGENTS, PLANS và tài liệu thành viên khác giữ nguyên.

## Scripts còn dùng

- `scripts/start-m3-environment.ps1`: temp/cache của terminal hiện tại trênE;
  không sửa global environment.
- `scripts/check-m3-storage.ps1`: E-backed mounts, C/E reserves, cluster health;
  chỉ preflight, không thay cấu hình hoặc tự chạy pipeline.
- `scripts/plot-e1-official.ps1`: tạo biểu đồ từ một completed analysis vào thư mục mới.
- Analysis: `experiments/e1_official_analysis.py --run-dir <completed-run>`;
  writer từ chối ghi đè artifact đã có. Kết quả analysis hiện tại đã đầy đủ, chỉ đọc khi review.
- Migration/recovery/watch/await/finish scripts cũ được loại nếu không protected/được sử dụng.

## Fixture tests nhỏ

Trong repository root, dùng Docker Spark3.5.4 hiện có:

```powershell
docker compose -f docker/docker-compose.yml exec -T -e PYTHONPATH=/app spark-master /opt/spark/bin/spark-submit --master 'local[1]' /app/tests/run_m3_tests.py gold
docker compose -f docker/docker-compose.yml exec -T -e PYTHONPATH=/app spark-master /opt/spark/bin/spark-submit --master 'local[1]' /app/tests/run_m3_tests.py e1
python -m tests.run_m3_tests audit-delivery
```

- Expected20/4/9PASS; không cần cài pytest để dùng các fallback runners.
- Host requirements đang ghim PySpark3.5.3; fixtures đã xác minh trong Docker3.5.4,
  không tự sửa dependency chung. Không chạy Gold CLI overwrite để kiểm tra.

## Tái lập E1 — chỉ khi được yêu cầu

- Preflight storage/cluster/READY/fingerprint trước; không chạy đồng thời workload khác.
- Dùng entrypoint `experiments/e1_scale_data.py`; legacy Makefile E1 thiếu arguments.
- Exact command đã đo ở `results/m3/e1/task06b/recovery-2/runner.json`, phase official.
  Với reproduction được duyệt, giữ nguyên command/resource/reader/seed/fractions,
  thay `--git-sha` bằng HEAD thực tế và `--output-root` thành thư mục mới riêng.
- CLI tối thiểu cần `--tier L --git-sha <SHA> --silver-path
  /app/data/lake/benchmark/e1/tier_l/silver/events --fractions .25 .01 1 .05 .50 .10
  --runs 3 --seed 42 --output-root /app/results/m3/e1/<fresh-run>`.
- Giữ driver2g, executor1536m/1core, total2cores, shuffle1024, AQE coalescing off,
  maxPartitionBytes67108864, vectorizedReader=false, reader batch512(inactive),
  inMemoryColumnarStorage.batchSize1000. Temp driver/workers phải E-backed như command gốc.
- Hash trên toàn bộ schema nguồn rồi projection3cột; MEMORY_AND_DISK và one warmup
  ngoài timer. Không đổi sampling/workload để tối ưu kết quả.
- Controller recovery đã nghỉ; future run cần giám sát reserves/OOM riêng, không
  chạy unattended hoặc tái dùng ownership/PID files cũ. Không tự retry khi lỗi.
- Vẽ vào output mới bằng `plot-e1-official.ps1 -RunDirectory <run> -FigureDirectory <new-folder>`.

## Bảo toàn và Git

- Không xóa lake, metrics/CSV/JSON, READY,5figures, protected evidence hay Spark event logs.
- Không can thiệp VHDX, backup, WSL swap hoặc pagefile. Backup checksum **NOT REVALIDATED**.
- Core để review commit: Gold/E1 runtime, focused tests/analysis, approved isolated
  TierL config, ADR, hai summaries và gói aggregate/5figures. Không bulk-stage results/scripts.
- Compose là shared/M1-owned thay đổi có sẵn; shared experiment CSV và DQ output
  giữ riêng. Raw logs, temp, secrets và machine snapshots không đưa vào core commit.
- Một số báo cáo protected phải giữ nguyên; các đường dẫn README/PLANS bắt buộc
  được giữ bằng trang chuyển hướng. Không thể xóa toàn bộ Markdown cũ khi giữ các ràng buộc này.
- **E4 NOT RUN.** Không commit/push/PR nếu chưa có yêu cầu mới.
