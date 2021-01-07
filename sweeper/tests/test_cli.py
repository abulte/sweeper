import pytest

from sweeper.sync import run


def test_cli(config_file, db):
    """Dummy test config and backend to check that loading works"""
    run("test", config=config_file)
    assert db["metadata"].count(job="test") == 1
    job = db["metadata"].find_one(job="test")
    assert job["error"] is None
    _run = db["test"].find_one()
    assert _run["metadata_id"] == job["id"]


def test_cli_error(config_file, db):
    with pytest.raises(Exception):
        run("test_error", config=config_file)
    assert db["metadata"].count(job="test_error") == 1
    assert db["metadata"].find_one(job="test_error")["error"] == "ERROR"


def test_cli_run_error(config_file, db):
    run("test_run_error", config=config_file)
    assert db["metadata"].count(job="test_run_error") == 1
    job = db["metadata"].find_one(job="test_run_error")
    assert job["has_run_errors"]
    assert db["test_run_error"].count() == 1
    _run = db["test_run_error"].find_one()
    assert _run["error"] == "ERROR"
    assert _run["name"] == "dumdum"
    assert _run["metadata_id"] == job["id"]
