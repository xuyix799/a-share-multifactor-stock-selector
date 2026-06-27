import os
from pathlib import Path
from typing import Any

import yaml


class ConfigError(RuntimeError):
    pass


def _default_config_dir() -> Path:
    return Path(__file__).resolve().parents[4] / "config"


def get_config_dir() -> Path:
    configured = os.getenv("STOCK_CONFIG_DIR")
    config_dir = Path(configured) if configured else _default_config_dir()
    if not config_dir.exists():
        raise ConfigError(f"config directory does not exist: {config_dir}")
    return config_dir


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"missing config file: {path}")
    with path.open("r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh) or {}
    if not isinstance(loaded, dict):
        raise ConfigError(f"config file must contain a YAML mapping: {path}")
    return loaded


def load_settings() -> dict[str, Any]:
    return _load_yaml(get_config_dir() / "settings.yaml")


def load_factor_weights() -> dict[str, float]:
    weights = load_factor_weights_config()
    score_keys = {"quality_score", "growth_score", "valuation_score", "industry_score", "trend_score"}
    return {key: float(value) for key, value in weights.items() if key in score_keys}


def load_factor_weights_config() -> dict[str, Any]:
    return _load_yaml(get_config_dir() / "factor_weights.yaml")
