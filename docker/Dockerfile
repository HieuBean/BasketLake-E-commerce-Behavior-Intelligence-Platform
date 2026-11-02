FROM apache/spark:3.5.4

USER root

# Cài pip packages cần thiết cho pipeline, analytics và testing bên trong container.
#
# LƯU Ý QUAN TRỌNG VỀ PHIÊN BẢN:
# Base image apache/spark:3.5.4 dùng Python 3.8.10, KHÔNG phải 3.11 như máy local.
# Vì vậy một số gói phải ghim thấp hơn requirements.txt:
#   - pandas 3.11 local dùng 2.2.3, nhưng 2.2.3 yêu cầu Python >=3.9
#     → trong container phải dùng 2.0.3 (bản cuối còn hỗ trợ Python 3.8)
#   - matplotlib 3.9.2 yêu cầu Python >=3.9
#     → trong container dùng 3.7.5 (bản cuối còn hỗ trợ Python 3.8)
# Các gói còn lại khớp requirements.txt.
RUN pip3 install --no-cache-dir \
    pyyaml==6.0.2 \
    pytest==8.3.3 \
    ruff==0.6.9 \
    pyarrow==17.0.0 \
    pandas==2.0.3 \
    duckdb==1.1.3 \
    matplotlib==3.7.5

WORKDIR /app

# Giữ root: compose mount repo vào /app và các job cần ghi data/lake.
USER root
