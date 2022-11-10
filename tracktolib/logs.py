import logging
from typing import Literal
from pythonjsonlogger import jsonlogger

logger = logging.getLogger('sync-legacy')


class CustomJsonFormatter(jsonlogger.JsonFormatter):

    def __init__(self, version: str, *args, **kwargs):
        self.version: str = version
        super().__init__(*args, **kwargs)

    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)

        log_record.pop('color_message', None)
        if not log_record.get('version'):
            log_record['version'] = self.version


def init_logging(log_format: Literal['json', 'console']):
    stream_handler = logging.StreamHandler()
    if log_format == 'json':
        formatter = CustomJsonFormatter('%(asctime)s [%(levelname)s] %(message)s',
                                        datefmt='%Y-%m-%d %H:%M:%S')
    elif log_format == 'console':
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
    else:
        raise NotImplementedError(f'Invalid log format {log_format!r}')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
