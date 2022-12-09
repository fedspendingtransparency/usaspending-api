import pytest

from datetime import datetime, timedelta, timezone
from model_bakery import baker
from usaspending_api.awards.models import TransactionFABS
from usaspending_api.broker.lookups import EXTERNAL_DATA_TYPE_DICT
from usaspending_api.broker.management.commands.fabs_nightly_loader import (
    get_incremental_load_start_datetime,
    LAST_LOAD_LOOKBACK_MINUTES,
    UPDATED_AT_MODIFIER_MS,
)
from usaspending_api.broker.models import ExternalDataLoadDate


@pytest.mark.django_db()
@pytest.mark.skip(reason="Test based on pre-databricks loader code. Remove when fully cut over.")
def test_get_incremental_load_start_datetime():

    may4 = datetime(2020, 5, 4, tzinfo=timezone.utc)
    may5 = datetime(2020, 5, 5, tzinfo=timezone.utc)
    may6 = datetime(2020, 5, 6, tzinfo=timezone.utc)
    lookback_minutes = timedelta(minutes=LAST_LOAD_LOOKBACK_MINUTES)
    updated_at_modifier = timedelta(milliseconds=UPDATED_AT_MODIFIER_MS)

    # With no data in the database, this should fail.
    with pytest.raises(RuntimeError):
        get_incremental_load_start_datetime()

    # Add a last load.
    baker.make(
        "broker.ExternalDataLoadDate",
        external_data_type__external_data_type_id=EXTERNAL_DATA_TYPE_DICT["fabs"],
        last_load_date=may5,
    )
    assert get_incremental_load_start_datetime() == may5 - lookback_minutes

    # Add a FABS updated_at that is older than last load date so it is chosen.
    baker.make("search.TransactionSearch", transaction_id=1, updated_at=may4, is_fpds=False)
    assert get_incremental_load_start_datetime() == may4 + updated_at_modifier

    # Make FABS updated_at newer so last load date is chosen.
    TransactionFABS.objects.filter(transaction_id=1).update(updated_at=may6)
    assert get_incremental_load_start_datetime() == may5 - lookback_minutes

    # Getting rid of last load date should cause retrieval to fail.  Basically the same as the first test.
    ExternalDataLoadDate.objects.all().delete()
    with pytest.raises(RuntimeError):
        get_incremental_load_start_datetime()
