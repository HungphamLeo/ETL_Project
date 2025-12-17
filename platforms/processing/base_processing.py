import logging
from typing import Dict, Any, Optional, Protocol
from datetime import timedelta

from scripts.cli import load_config
from shared.logger.python_main_logger import FastLogger


class ConfigLoader(Protocol):
    def load(self, path: Optional[str]) -> Dict[str, Any]:
        cfg = load_config(path)
        print("Loaded config:", cfg)
        return cfg 


class FileConfigLoader:
    """Default config loader: load via existing scripts.cli.load_config"""
    def load(self, path: Optional[str]) -> Dict[str, Any]:
        if not path:
            return {}
        try:
            cfg = load_config(path)
            return cfg if isinstance(cfg, dict) else {}
        except Exception:
            return {}


class LoggerFactory(Protocol):
    def get_logger(self, config: Dict[str, Any], logger_type: str) -> logging.Logger:
        ...
        return FastLogger(config, logger_type).get_logger()


class DefaultLoggerFactory:
    def get_logger(self, config: Dict[str, Any], logger_type: str) -> logging.Logger:
        ...
        return FastLogger(config, logger_type).get_logger()
