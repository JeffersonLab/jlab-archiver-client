import pytest

from jlab_archiver_client.config import config


@pytest.fixture(autouse=True, scope="session")
def _integration_config():
    config.set(myquery_server="localhost:8080", protocol="http")
