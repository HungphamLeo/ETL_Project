import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

class FastLogger:
    def __init__(self, config: dict, logger_type: str = "etl_logger.extract_log"):
        """
        Custom logger with rotating file handlers and console output.
        :param config: dict loaded from YAML
        :param logger_type: path tới loại logger, ví dụ:
                            "etl_logger.extract_log",
                            "etl_logger.transform_log",
                            "etl_logger.load_log"
        """
        # Tách nhánh config theo logger_type
        logger_config = config
        for key in logger_type.split("."):
            logger_config = logger_config.get(key, {})
        if not logger_config:
            raise ValueError(f"Logger type '{logger_type}' không tồn tại trong config")

        # Lấy level (ưu tiên trong nhánh logger, fallback sang root)
        level = logger_config.get("level") or config.get("level") or "INFO"

        # Tạo logger
        self.logger = logging.getLogger(f"AppLogger.{logger_type}")
        self.logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        self.logger.propagate = False

        log_path = Path(logger_config["storage_path"])
        log_path.mkdir(parents=True, exist_ok=True)

        log_files = {
            logging.DEBUG: logger_config["files"]["debug"],
            logging.INFO: logger_config["files"]["info"],
            logging.WARNING: logger_config["files"]["warning"],
            logging.ERROR: logger_config["files"]["error"],
        }

        max_bytes = logger_config.get("max_size_mb", 10) * 1024 * 1024
        backup_count = logger_config.get("backup_count", 3)

        formatter = logging.Formatter(
            fmt='{"time":"%(asctime)s", "level":"%(levelname)s", "message":"%(message)s", "caller":"%(pathname)s:%(lineno)d"}',
            datefmt="%Y-%m-%dT%H:%M:%S"
        )

        # File handlers
        for level, filename in log_files.items():
            handler = RotatingFileHandler(
                filename=log_path / filename,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
            handler.setLevel(level)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        return self.logger