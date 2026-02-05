import sys
from pathlib import Path

import pytest

from factor_engine.db import Base, configure_engine
from factor_engine.db.session import init_db

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture()
def temp_db(tmp_path):
    db_path = tmp_path / "test.db"
    configure_engine(db_path)
    init_db(Base)
    yield db_path
