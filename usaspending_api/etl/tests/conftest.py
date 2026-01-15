import pytest

from model_bakery import baker

from usaspending_api.etl.tests.data.submissions import submissions

# Pulling in specific fixtures elsewhere
__all__ = [
    "submissions",
]


@pytest.fixture
def external_data_type(db):
    for _id in [201, 202, 203, 204]:
        baker.make("broker.ExternalDataType", external_data_type_id=_id)
