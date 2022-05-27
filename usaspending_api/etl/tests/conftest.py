# Compose other supporting conftest files
from usaspending_api.etl.tests.conftest_spark import *  # noqa

# Pulling in specific fixtures elsewhere
from usaspending_api.etl.tests.data.submissions import submissions

__all__ = [
    "submissions",
]
