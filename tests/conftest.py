import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from dataloop.common.config import load_profile


@pytest.fixture(scope="session")
def cfg():
    return load_profile("configs/mini.yaml")
