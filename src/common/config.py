"""
src/common/config.py
Đọc configs/pipeline.yaml và trả về dict cấu hình.
Dùng: from src.common.config import load_config
"""
from __future__ import annotations

import os
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None


def _parse_val(v: str):
    v = v.strip()
    if v.startswith("[") and v.endswith("]"):
        return [x.strip().strip("'\"") for x in v[1:-1].split(",") if x.strip()]
    if v.lower() in ("true", "yes"):
        return True
    if v.lower() in ("false", "no"):
        return False
    try:
        if "." in v:
            return float(v)
        return int(v)
    except ValueError:
        return v.strip("'\"")


def _fallback_yaml_parse(text: str) -> dict:
    """Bộ phân tích YAML dự phòng khi môi trường chưa cài PyYAML."""
    res = {}
    curr_sec, curr_sub = None, None
    for line in text.splitlines():
        line = line.split("#")[0].rstrip()
        if not line:
            continue
        indent = len(line) - len(line.lstrip())
        s = line.strip()
        if indent == 0 and s.endswith(":"):
            curr_sec = s[:-1].strip()
            res[curr_sec] = {}
            curr_sub = None
        elif indent == 2 and ":" in s and curr_sec:
            k, v = s.split(":", 1)
            k, v = k.strip(), v.strip()
            if not v:
                curr_sub = k
                res[curr_sec][curr_sub] = {}
            else:
                curr_sub = None
                res[curr_sec][k] = _parse_val(v)
        elif indent == 4 and ":" in s and curr_sec and curr_sub:
            k, v = s.split(":", 1)
            res[curr_sec][curr_sub][k.strip()] = _parse_val(v.strip())
    return res


_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "configs" / "pipeline.yaml"


def load_config(config_path: str | Path | None = None) -> dict:
    """Đọc pipeline.yaml. Tự động dự phòng nếu chưa cài pyyaml."""
    path = Path(config_path or os.getenv("CONFIG_PATH", str(_CONFIG_PATH)))
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    if yaml is not None:
        return yaml.safe_load(content)
    return _fallback_yaml_parse(content)


def get_paths(cfg: dict) -> dict:
    paths = dict(cfg.get("paths", {}))
    # Nếu chạy ngoài container (không có /app), tự động map /app về thư mục gốc của repo
    if not os.path.exists("/app"):
        repo_root = Path(__file__).resolve().parent.parent.parent
        for k, v in paths.items():
            if isinstance(v, str) and v.startswith("/app/"):
                paths[k] = str(repo_root / v[len("/app/"):])
            elif v == "/app":
                paths[k] = str(repo_root)
    return paths


def get_tier_config(cfg: dict, tier: str) -> dict:
    """Trả về cấu hình tier (S/M/L/XL)."""
    tiers = cfg.get("tiers", {})
    if tier not in tiers:
        raise ValueError(f"Tier '{tier}' không hợp lệ. Chọn: {list(tiers.keys())}")
    return tiers[tier]
