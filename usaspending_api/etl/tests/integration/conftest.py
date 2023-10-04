# Pulling in specific fixtures elsewhere
from usaspending_api.etl.tests.integration.test_load_to_from_delta import (
    populate_broker_data,
    populate_usas_data,
)

__all__ = [
    "populate_broker_data",
    "populate_usas_data",
]


# import logging
# import time

# import pytest

# logger = logging.getLogger(__name__)


# @pytest.hookimpl(hookwrapper=True)
# def pytest_fixture_setup(fixturedef, request):
#     start = time.time()
#     yield
#     end = time.time()

#     logger.info("pytest_fixture_setup" f", request={request}" f", time={round(end - start)}")
