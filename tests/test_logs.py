import pytest
import logging


@pytest.mark.parametrize('log_format',
                         ['console', 'json'])
def test_init_logging(log_format, caplog):
    from tracktolib.logs import init_logging

    logger = logging.getLogger('test')
    stream_handler = logging.StreamHandler()

    logger.setLevel(logging.INFO)
    init_logging(logger, log_format, '0.0.1',
                 stream_handler=stream_handler)
    with caplog.at_level(logging.INFO):
        logger.info('hello')
