import logging
from typing import Dict, Any, Optional, Protocol
from datetime import timedelta

from scripts.cli.load_config import func_load_config
from shared.logger.python_main_logger import logger_manager


class ConfigLoader(Protocol):
    def load(self, path: Optional[str]) -> Dict[str, Any]:
        cfg = func_load_config(path)
        return cfg 


class FileConfigLoader:
    """Default config loader: load via existing scripts.cli.load_config"""
    def load(self, path: Optional[str]) -> Dict[str, Any]:
        if not path:
            return {}
        try:
            cfg = func_load_config(path)
            return cfg if isinstance(cfg, dict) else {}
        except Exception:
            return {}



class LoggerFactory(Protocol):
    def get_logger(self, module_name: str) -> logging.Logger:
        ...

class DefaultLoggerFactory:
    def get_logger(self, module_name: str) -> logging.Logger:
        if module_name.startswith("logger."):
            return logger_manager.get_logger(module_name, logger_name=module_name)
        return logger_manager.get_logger(module_name)
