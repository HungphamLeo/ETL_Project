# shared/libs/python/base_service/logger_setup.
import logging
import logging.config
from pathlib import Path
from pythonjsonlogger import jsonlogger
from python_loki_logger.Logger import LokiLogger
from logging.handlers import RotatingFileHandler

class FastLogger:
    def __init__(self, logger_config: dict):
        """
        Custom logger with rotating file handlers and console output.
        """
        logger_config = logger_config['logging']
        self.logger = logging.getLogger("AppLogger")
        self.logger.setLevel(logger_config['level'])
        self.logger.propagate = False

        log_path = Path(logger_config['storage_path'])
        log_path.mkdir(parents=True, exist_ok=True)

        log_files = {
            logging.DEBUG: logger_config['files']['debug'],
            logging.INFO: logger_config['files']['info'],
            logging.WARNING: logger_config['files']['warning'],
            logging.ERROR: logger_config['files']['error'],
        }

        formatter = logging.Formatter(
            fmt='{"time":"%(asctime)s", "level":"%(levelname)s", "message":"%(message)s", "caller":"%(pathname)s:%(lineno)d"}',
            datefmt="%Y-%m-%dT%H:%M:%S"
        )

        # File handlers
        for level, filename in log_files.items():
            handler = RotatingFileHandler(
                filename=log_path / filename,
                maxBytes=10 * 1024 * 1024,
                backupCount=3
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