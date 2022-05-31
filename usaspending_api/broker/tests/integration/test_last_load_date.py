import pytest
import pytz

from datetime import datetime
from model_bakery import baker

from usaspending_api.broker.helpers.last_load_date import (
    get_earliest_load_date,
    get_latest_load_date,
    get_last_load_date,
)


@pytest.fixture
def load_dates():
    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make("broker.ExternalDataType", name="fpds", external_data_type_id=1)
    baker.make("broker.ExternalDataLoadDate", last_load_date="2020-03-01", external_data_type=edt)

    edt2 = baker.make("broker.ExternalDataType", name="fabs", external_data_type_id=2)
    baker.make("broker.ExternalDataLoadDate", last_load_date="2020-08-01", external_data_type=edt2)


@pytest.mark.django_db
def test_last_load_date_happy(client, load_dates):
    load_date = get_last_load_date("fpds")
    assert load_date == datetime(2020, 3, 1, tzinfo=pytz.UTC)


@pytest.mark.django_db
def test_last_load_date_not_set(client, load_dates):
    load_date = get_last_load_date("es_deletes")
    assert load_date is None


@pytest.mark.django_db
def test_earliest_load_date_happy(client, load_dates):
    load_date = get_earliest_load_date(["fpds", "fabs", "es_deletes"])
    assert load_date == datetime(2020, 3, 1, tzinfo=pytz.UTC)


@pytest.mark.django_db
def test_latest_load_date_happy(client, load_dates):
    load_date = get_latest_load_date(["fpds", "fabs", "es_deletes"])
    assert load_date == datetime(2020, 8, 1, tzinfo=pytz.UTC)
