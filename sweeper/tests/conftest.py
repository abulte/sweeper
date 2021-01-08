import os

import pytest

from pathlib import Path
from unittest import mock

from sweeper import clean_db, get_db
from sweeper.utils.config import load as load_config


@pytest.fixture
def config(config_file):
    return load_config(config_file, "test")


@pytest.fixture(autouse=True)
def setup():
    with mock.patch.dict(os.environ, {"DATABASE_URL": "sqlite:///:memory:"}):
        clean_db()
        yield
        clean_db()


@pytest.fixture
def config_file():
    return Path(__file__).resolve().parent / "./jobs_test.toml"


@pytest.fixture
def db():
    return get_db()


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
