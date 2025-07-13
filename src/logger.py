# shared/libs/python/base_service/logger_setup.py

import logging
import logging.config
from pathlib import Path
from pythonjsonlogger import jsonlogger
from python_loki_logger.Logger import LokiLogger

def setup_logging(cfg: dict, service_name: str):
    level = cfg['logging'].get('level', 'INFO')
    log_format = cfg['logging'].get('format', 'json')
    output = cfg['logging'].get('output', 'stdout')
    file_path = cfg['logging'].get('file_path', f'/var/log/{service_name}/{service_name}.log')

    handlers = ['console']
    log_handlers = {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'json' if log_format == 'json' else 'standard',
            'level': level,
        }
    }

    if output == 'file':
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        log_handlers['file'] = {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': file_path,
            'maxBytes': cfg['logging'].get('max_bytes', 104857600),
            'backupCount': cfg['logging'].get('backup_count', 5),
            'formatter': 'json' if log_format == 'json' else 'standard',
            'level': level,
        }
        handlers.append('file')

    config_dict = {
        'version': 1,
        'formatters': {
            'json': {
                '()': jsonlogger.JsonFormatter,
                'format': '%(asctime)s %(levelname)s %(name)s %(message)s'
            },
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        },
        'handlers': log_handlers,
        'root': {
            'handlers': handlers,
            'level': level
        }
    }

    logging.config.dictConfig(config_dict)
    logger = logging.getLogger(service_name)

    if cfg['logging'].get('loki_enabled'):
        loki_handler = LokiLogger(
            baseUrl=cfg['logging']['loki_url'],
            pushUrl=cfg['logging'].get('loki_push_url'),
            auth=tuple(cfg['logging'].get('loki_auth', ())),
            severity_label=cfg['logging'].get('severity_label', 'level')
        )
        logger.addHandler(loki_handler)
        logger.info("Loki logging handler added")

    logger.info(f"Logging initialized for {service_name}")
    return logger