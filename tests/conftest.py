import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pytest

from db import DBSqLite
from fs import MockFs
from scanner import Scanner


@pytest.fixture
def tmp_db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def mock_fs():
    return MockFs(seed=1337, max_depth=4, max_children=4, p_dir=0.45, max_file_size=10_000)


@pytest.fixture
def scanner(tmp_db_path, mock_fs):
    db = DBSqLite(tmp_db_path)
    s = Scanner(fs=mock_fs, db=db, root=mock_fs.root_path, db_version=0)
    yield s
    db.close()
