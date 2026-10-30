FROM apache/spark:3.5.3-python3

USER root

# Cài đặt pip packages cần thiết cho pipeline và testing
RUN pip3 install --no-cache-dir \
    pyyaml==6.0.2 \
    pytest==8.3.3 \
    pyarrow==17.0.0 \
    pandas==2.0.3 \
    duckdb==1.1.3

WORKDIR /app
USER spark
