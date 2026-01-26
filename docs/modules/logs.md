---
title: "Logs"
---

# Logs

Utility functions to initialize logging formatting and streams.

## Installation

```bash
uv add tracktolib[logs]
```

## Dependencies

- [python-json-logger](https://github.com/madzak/python-json-logger)

## Functions

### `init_logging`

Initialize logging with either JSON or console format.

```python
import logging
from tracktolib.logs import init_logging

logger = logging.getLogger()

# JSON format (for production/structured logging)
formatter, stream_handler = init_logging(logger, 'json', version='0.0.1')

# Console format (for development)
formatter, stream_handler = init_logging(logger, 'console', version='0.0.1')
```

### `is_valid_log_format`

Type guard to validate log format strings.

```python
from tracktolib.logs import is_valid_log_format

user_input = "json"
if is_valid_log_format(user_input):
    # user_input is now typed as LogFormat
    formatter, handler = init_logging(logger, user_input, version='1.0.0')
```

## Custom JSON Formatter

The `CustomJsonFormatter` class extends `JsonFormatter` to automatically include the application version in every log
entry.

### Extending the Formatter

You can extend `CustomJsonFormatter` to automatically inject the version from your app config:

```python
from tracktolib.logs import CustomJsonFormatter


class AppJsonFormatter(CustomJsonFormatter):
    def __init__(self, *args, **kwargs):
        from myapp.config import VERSION
        super().__init__(version=VERSION, *args, **kwargs)
```

### Integration with Uvicorn/FastAPI

Example of integrating with uvicorn's logging configuration:

```python
import logging
from uvicorn.config import LOGGING_CONFIG
from tracktolib.logs import init_logging as _init_logging, is_valid_log_format


def init_logging(
        logger: logging.Logger,
        logs_format: str,
        logger_name: str,
        version: str,
        log_level: int = logging.INFO,
):
    if not is_valid_log_format(logs_format):
        raise ValueError(f"Got invalid log format: {logs_format!r}")

    log_config = {**LOGGING_CONFIG}
    log_config['loggers'][logger_name] = {'handlers': ['default'], 'level': log_level}

    use_colors = True
    formatter, stream_handler = _init_logging(logger=logger, log_format=logs_format, version=version)

    if logs_format == 'json':
        log_config['formatters']['json'] = {
            '()': 'myapp.logs.AppJsonFormatter',
            'fmt': '%(asctime)s [%(levelname)s] %(message)s',
        }
        log_config['handlers']['default']['formatter'] = 'json'
        log_config['handlers']['access']['formatter'] = 'json'
        use_colors = False

    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return log_config, use_colors
```

## Output Examples

### JSON Format

```json
{
  "asctime": "2024-01-15 10:30:45",
  "levelname": "INFO",
  "message": "Application started",
  "version": "1.0.0"
}
```

### Console Format

```text
2024-01-15 10:30:45 [INFO] Application started
```
