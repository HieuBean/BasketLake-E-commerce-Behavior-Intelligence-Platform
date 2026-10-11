"""
src/common/logger.py
Logger chuẩn dùng chung — ghi ra stdout (Docker thu log từ stdout).
"""
import logging
import sys


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
