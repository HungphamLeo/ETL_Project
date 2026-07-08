import logging
import logging.config
from pathlib import Path
from typing import Dict, Any, Optional
import yaml


def _ensure_log_dir(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

class LoggerManager:
    _instance: Optional['LoggerManager'] = None
    _loggers: Dict[str, logging.Logger] = {}

    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else Path(__file__).parent / "config" / "logger_config.yaml"
        self.config: Dict[str, Any] = {}
        self._initialized = False
        if config_path is not None:
            self.configure(config_path)

    def configure(self, config_path: Optional[str] = None) -> None:
        """Configure or reconfigure logging from a YAML file path."""
        if config_path is not None:
            self.config_path = Path(config_path)
        self._loggers.clear()
        self.config = self._load_config()
        self._configure_logging()
        self._initialized = True

    def configure_from_project_config(self, project_config_path: str) -> None:
        """Configure logging from an ETL project YAML config file."""
        self._loggers.clear()
        project_config_path = Path(project_config_path)
        try:
            with open(project_config_path, 'r', encoding='utf-8') as f:
                project_config = yaml.safe_load(f) or {}
        except Exception as exc:
            raise RuntimeError(f"Failed to load project config for logger: {exc}")

        project_logger = project_config.get("project_params", {}).get("logger", {})
        self.config = self._build_dict_config_from_project_logger(project_logger)
        self._configure_logging()
        self._initialized = True

    def _build_dict_config_from_project_logger(self, project_logger: Dict[str, Any]) -> Dict[str, Any]:
        formatter_config = {
            "format": '{"time":"%(asctime)s", "level":"%(levelname)s", "message":"%(message)s", "caller":"%(pathname)s:%(lineno)d"}',
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        }
        handlers: Dict[str, Any] = {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": project_logger.get("level", "INFO"),
            }
        }
        loggers: Dict[str, Any] = {}

        def build_handlers(tree: Dict[str, Any], prefix: str) -> None:
            for key, value in tree.items():
                if not isinstance(value, dict):
                    continue
                nested_prefix = f"{prefix}.{key}" if prefix else key
                if "files" in value and "storage_path" in value:
                    storage_path = Path(value["storage_path"])
                    for level_name, filename in value.get("files", {}).items():
                        handler_name = f"{nested_prefix}.{level_name}"
                        handler_path = storage_path / filename
                        handlers[handler_name] = {
                            "class": "logging.handlers.RotatingFileHandler",
                            "formatter": "default",
                            "level": level_name.upper(),
                            "filename": str(handler_path),
                            "maxBytes": 10 * 1024 * 1024,
                            "backupCount": value.get("backup_count", 3),
                        }
                        _ensure_log_dir(handler_path)
                    loggers[f"logger.{nested_prefix}"] = {
                        "level": value.get("level", project_logger.get("level", "INFO")),
                        "handlers": [f"{nested_prefix}.{level_name}" for level_name in value.get("files", {})],
                        "propagate": False,
                    }
                else:
                    build_handlers(value, nested_prefix)

        build_handlers(project_logger.get("ingestion_log", {}), "ingestion_log")
        build_handlers(project_logger.get("storage_log", {}), "storage_log")

        root_handlers = ["console"]
        if any(name.endswith(".debug") for name in handlers if name != "console"):
            root_handlers.append(next((name for name in handlers if name.endswith(".debug")), "console"))

        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"default": formatter_config},
            "handlers": handlers,
            "loggers": loggers,
            "root": {
                "level": project_logger.get("level", "INFO"),
                "handlers": root_handlers,
            },
        }

    def _load_config(self) -> Dict[str, Any]:
        """Load logger config từ YAML, với fallback mặc định."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
            if isinstance(config, dict):
                for handler in config.get("handlers", {}).values():
                    if isinstance(handler, dict) and "filename" in handler:
                        _ensure_log_dir(handler["filename"])
            return config
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
        if not isinstance(self.config, dict):
            self.config = {}
        logging.config.dictConfig(self.config)

    def get_logger(self, module_name: str, logger_name: Optional[str] = None) -> logging.Logger:
        """
        Lấy logger cho module cụ thể.
        :param module_name: Tên module (sử dụng __name__), ví dụ: 'platforms.ingestion.cophieu68.extract'
        :param logger_name: Tên logger cụ thể nếu muốn ghi đè mặc định.
        :return: Logger instance
        """
        cache_key = logger_name or module_name
        if cache_key in self._loggers:
            return self._loggers[cache_key]

        if logger_name:
            logger = logging.getLogger(logger_name)
        else:
            logger = logging.getLogger(f"etl.{module_name.replace('.', '_')}")

        if not self.config.get("loggers"):
            logger.setLevel(logging.INFO)

        self._loggers[cache_key] = logger
        return logger

# Singleton instance
logger_manager = LoggerManager()