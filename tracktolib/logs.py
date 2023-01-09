import logging
from typing import Literal, overload, Any, TypeGuard
from dataclasses import dataclass

try:
    from pythonjsonlogger import jsonlogger
except ImportError:
    raise ImportError('Please install pythonjsonlogger or tracktolib with "log" to use this module')

LogFormat = Literal['json', 'console']


@dataclass
class CustomJsonFormatter(jsonlogger.JsonFormatter):
    version: str

    def __init__(self, version: str, *args, **kwargs):
        self.version: str = version
        super().__init__(*args, **kwargs)

    def add_fields(self,
                   log_record: dict[str, Any],
                   record: logging.LogRecord,
                   message_dict: dict[str, Any]):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)

        log_record.pop('color_message', None)
        if not log_record.get('version'):
            log_record['version'] = self.version


@overload
def init_logging(logger: logging.Logger,
                 log_format: Literal['json'],
                 version: str,
                 *,
                 stream_handler: logging.StreamHandler | None = None) -> tuple[
    CustomJsonFormatter, logging.StreamHandler]: ...


@overload
def init_logging(logger: logging.Logger,
                 log_format: Literal['console'],
                 version: str,
                 *,
                 stream_handler: logging.StreamHandler | None = None) -> tuple[
    logging.Formatter, logging.StreamHandler]: ...


def init_logging(logger: logging.Logger,
                 log_format: LogFormat,
                 version: str,
                 *,
                 stream_handler: logging.StreamHandler | None = None):
    _stream_handler = stream_handler or logging.StreamHandler()
    match log_format:
        case 'json':
            formatter = CustomJsonFormatter(version,
                                            '%(asctime)s [%(levelname)s] %(message)s',
                                            datefmt='%Y-%m-%d %H:%M:%S')
        case 'console':
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s',
                                          datefmt='%Y-%m-%d %H:%M:%S')
        case _:
            raise NotImplementedError(f'Invalid log format {log_format!r}')

    _stream_handler.setFormatter(formatter)
    logger.addHandler(_stream_handler)

    return formatter, _stream_handler


def is_valid_log_format(log_format: str) -> TypeGuard[LogFormat]:
    return log_format in {'json', 'console'}
