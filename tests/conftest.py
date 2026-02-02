from pathlib import Path

import pytest

_cur_dir = Path(__file__).parent

STATIC_DIR = _cur_dir / "static"


@pytest.fixture()
def static_dir():
    return STATIC_DIR
