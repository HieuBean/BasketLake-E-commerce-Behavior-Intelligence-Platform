"""
src/common/config.py
Đọc configs/pipeline.yaml và trả về dict cấu hình.
Dùng: from src.common.config import load_config
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml


_CONFIG_PATH = Path(__file__).parent.parent.parent / "configs" / "pipeline.yaml"


def load_config(config_path: str | Path | None = None) -> dict:
    """Đọc pipeline.yaml. Path có thể override qua biến môi trường CONFIG_PATH."""
    path = Path(config_path or os.getenv("CONFIG_PATH", str(_CONFIG_PATH)))
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_paths(cfg: dict) -> dict:
    return cfg["paths"]


def get_tier_config(cfg: dict, tier: str) -> dict:
    """Trả về cấu hình tier (S/M/L/XL)."""
    tiers = cfg.get("tiers", {})
    if tier not in tiers:
        raise ValueError(f"Tier '{tier}' không hợp lệ. Chọn: {list(tiers.keys())}")
    return tiers[tier]
