import pytest

from model_bakery import baker

from usaspending_api.etl.tests.data.submissions import submissions
from usaspending_api.etl.tests.integration.test_load_transactions_in_delta_lookups import (
    _BEGINNING_OF_TIME,
    _INITIAL_ASSISTS,
    _INITIAL_PROCURES,
    _INITIAL_SOURCE_TABLE_LOAD_DATETIME,
)

# Pulling in specific fixtures elsewhere
__all__ = [
    "submissions",
]


@pytest.fixture
def _populate_initial_source_tables_pg(db):
    # Populate transactions.SourceAssistanceTransaction and associated broker.ExternalDataType data in Postgres
    for assist in _INITIAL_ASSISTS:
        baker.make("transactions.SourceAssistanceTransaction", **assist)

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make(
        "broker.ExternalDataType",
        name="source_assistance_transaction",
        external_data_type_id=11,
        update_date=_INITIAL_SOURCE_TABLE_LOAD_DATETIME,
    )
    baker.make(
        "broker.ExternalDataLoadDate", last_load_date=_INITIAL_SOURCE_TABLE_LOAD_DATETIME, external_data_type=edt
    )

    # Populate transactions.SourceProcurementTransaction and associated broker.ExternalDataType data in Postgres
    for procure in _INITIAL_PROCURES:
        baker.make("transactions.SourceProcurementTransaction", **procure)

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make(
        "broker.ExternalDataType",
        name="source_procurement_transaction",
        external_data_type_id=10,
        update_date=_INITIAL_SOURCE_TABLE_LOAD_DATETIME,
    )
    baker.make(
        "broker.ExternalDataLoadDate", last_load_date=_INITIAL_SOURCE_TABLE_LOAD_DATETIME, external_data_type=edt
    )

    # Also need to populate values for es_deletes, int.transaction_[fabs|fpds|normalized], int.awards,
    #   and id lookup tables in broker.ExternalData[Type|LoadDate] tables
    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make("broker.ExternalDataType", name="es_deletes", external_data_type_id=102, update_date=None)
    baker.make("broker.ExternalDataLoadDate", last_load_date=_BEGINNING_OF_TIME, external_data_type=edt)

    for table_name, id in zip(
        (
            "transaction_fpds",
            "transaction_fabs",
            "transaction_normalized",
            "awards",
            "transaction_id_lookup",
            "award_id_lookup",
        ),
        range(201, 207),
    ):
        edt = baker.make("broker.ExternalDataType", name=table_name, external_data_type_id=id, update_date=None)
        baker.make("broker.ExternalDataLoadDate", last_load_date=_BEGINNING_OF_TIME, external_data_type=edt)
