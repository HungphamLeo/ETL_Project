import logging
import logging.config
from pathlib import Path
from typing import Dict, Any, Optional
import yaml

class LoggerManager:
    _instance: Optional['LoggerManager'] = None
    _loggers: Dict[str, logging.Logger] = {}

    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: Optional[str] = None):
        if self._initialized:
            return
        self.config_path = config_path or Path(__file__).parent / "config" / "logger_config.yaml"
        self.config = self._load_config()
        self._configure_logging()
        self._initialized = True

    def _load_config(self) -> Dict[str, Any]:
        """Load logger config từ YAML, với fallback mặc định."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Fallback config mặc định
            return {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "format": '{"time":"%(asctime)s", "level":"%(levelname)s", "message":"%(message)s", "caller":"%(pathname)s:%(lineno)d"}',
                        "datefmt": "%Y-%m-%dT%H:%M:%S"
                    }
                },
                "handlers": {
                    "console": {
                        "class": "logging.StreamHandler",
                        "formatter": "default",
                        "level": "INFO"
                    },
                    "file_debug": {
                        "class": "logging.handlers.RotatingFileHandler",
                        "filename": Path(__file__).parent / "logs" / "debug.log",
                        "maxBytes": 10 * 1024 * 1024,
                        "backupCount": 3,
                        "formatter": "default",
                        "level": "DEBUG"
                    },
                    # Thêm handlers cho info, warning, error tương tự
                },
                "loggers": {
                    "root": {
                        "level": "INFO",
                        "handlers": ["console", "file_debug"]
                    }
                }
            }

    def _configure_logging(self):
        """Cấu hình logging từ dict."""
        logging.config.dictConfig(self.config)

    def get_logger(self, module_name: str) -> logging.Logger:
        """
        Lấy logger cho module cụ thể.
        :param module_name: Tên module (sử dụng __name__), ví dụ: 'platforms.ingestion.cophieu68.extract'
        :return: Logger instance
        """
        if module_name in self._loggers:
            return self._loggers[module_name]
        
        # Tạo logger với tên dựa trên module (ánh xạ với cấu trúc thư mục)
        logger_name = f"etl.{module_name.replace('.', '_')}"
        logger = logging.getLogger(logger_name)
        
        # Nếu config có logger cụ thể cho module, sử dụng; ngược lại dùng root
        if logger_name in self.config.get("loggers", {}):
            pass  # Đã cấu hình trong dictConfig
        else:
            logger.setLevel(logging.INFO)  # Fallback
        
        self._loggers[module_name] = logger
        return logger

# Singleton instance
logger_manager = LoggerManager()