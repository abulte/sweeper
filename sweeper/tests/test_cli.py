import os
import pytest

from pathlib import Path
from unittest import mock

from sweeper import get_db
from sweeper.sync import run


@pytest.fixture(autouse=True)
def setup():
    with mock.patch.dict(os.environ, {"DATABASE_URL": "sqlite:///:memory:"}):
        yield


def test_cli():
    """Dummy test config and backend to check that loading works"""
    run("test", config=Path(__file__).resolve().parent / "./jobs_test.toml")
    db = get_db()
    assert db["metadata"].count() == 1
    assert db["metadata"].find_one()["error"] is None
