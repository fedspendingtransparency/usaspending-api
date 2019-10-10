import usaspending_api.etl.transaction_loaders.fpds_loader as fpds_loader
import random
from usaspending_api.etl.transaction_loaders.fpds_loader import (
    _extract_broker_objects,
    _create_load_object,
    _transform_objects,
    _load_transactions,
)
from usaspending_api.etl.transaction_loaders.field_mappings_fpds import (
    transaction_fpds_nonboolean_columns,
    transaction_normalized_nonboolean_columns,
    legal_entity_nonboolean_columns,
    legal_entity_boolean_columns,
    recipient_location_nonboolean_columns,
    recipient_location_functions,
    place_of_performance_nonboolean_columns,
    place_of_performance_functions,
    transaction_fpds_boolean_columns,
)
from usaspending_api.etl.transaction_loaders.data_load_helpers import format_insert_or_update_column_sql

from unittest.mock import MagicMock, patch


def mock_cursor(monkeypatch, result_value):
    """
    A mock connection/cursor that ignores calls to cursor.execute and returns from fetchall
    whatever you tell it based on mock_cursor.expected_value.  This is a bare minimum
    implementation.  If you use any other connection or cursor features you may need to tweak
    this accordingly (or you may not... MagicMock is, um, "magic").
    """

    class Result:
        pass

    result = Result()
    result.expected_value = result_value

    def mock_cursor():
        mock_context = MagicMock()
        mock_context.execute.return_value = None
        mock_context.mogrify = mogrify
        mock_context.fetchall.return_value = result.expected_value
        return mock_context

    def mock_connection(cursor_factory=None):
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_cursor()
        mock_context.__exit__.return_value = False
        return mock_context

    def mock_connect(dsn):
        mock_context = MagicMock()
        mock_context.__enter__.return_value.cursor = mock_connection
        mock_context.__exit__.return_value = False
        return mock_context

    def mogrify(val1, val2):
        return str(val2[0]).encode()

    # I don't think it works this way...
    result.mogrify = mogrify

    monkeypatch.setattr("psycopg2.connect", mock_connect)

    return result


def _assemble_dummy_broker_data():
    return {
        **transaction_fpds_nonboolean_columns,
        **transaction_normalized_nonboolean_columns,
        **legal_entity_nonboolean_columns,
        **{key: str(random.choice([True, False])) for key in legal_entity_boolean_columns},
        **recipient_location_nonboolean_columns,
        **place_of_performance_nonboolean_columns,
        **{key: str(random.choice([True, False])) for key in transaction_fpds_boolean_columns},
    }


def _stub___extract_broker_objects(id_list):
    """Return a list of dictionaries, in the same structure as `fetchall` when using psycopg2.extras.DictCursor"""
    for id in id_list:
        dummy_row = _assemble_dummy_broker_data()
        dummy_row["detached_award_procurement_id"] = id
        dummy_row["detached_award_proc_unique"] = str(id)
        yield dummy_row


def test_load_ids_empty():
    fpds_loader.load_ids([])


# These are patched in opposite order from when they're listed in the function params, because that's how the fixture works
@patch("usaspending_api.etl.transaction_loaders.fpds_loader._extract_broker_objects")
@patch(
    "usaspending_api.etl.transaction_loaders.derived_field_functions_fpds.fy", return_value=random.randint(2001, 2019)
)
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.bulk_insert_recipient_location")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.bulk_insert_recipient")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.bulk_insert_place_of_performance")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader._matching_award")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.insert_award")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader._lookup_existing_transaction")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.update_transaction_normalized")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.update_transaction_fpds")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.insert_transaction_normalized")
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.insert_transaction_fpds")
def test_load_ids_dummy_id(
    mock__insert_transaction_fpds_transaction,
    mock__insert_transaction_normalized_transaction,
    mock__update_transaction_fpds_transaction,
    mock__update_transaction_normalized_transaction,
    mock__lookup_existing_transaction,
    mock__insert_award,
    mock__matching_award,
    mock__bulk_insert_place_of_performance,
    mock__bulk_insert_recipient,
    mock__bulk_insert_recipient_location,
    mock__fy,
    mock__extract_broker_objects,
):

    # Mock output data of key participant functions in this test scenario
    # This is the baseline unconstrained scenario, where all patched functions' MagicMocks will behave as
    # required by the code
    mock__extract_broker_objects.side_effect = _stub___extract_broker_objects

    # Test run of the loader
    dummy_broker_ids = [101, 201, 301]
    fpds_loader.load_ids(dummy_broker_ids)

    # Since the mocks will return "data" always when called, if not told to return "None", the branching logic in
    # load_ids like: "lookup award, if not exists, create ... lookup transaction, if not exists, create", will
    # always "find" a *mock* award and transaction.
    # So, assert this baseline run followed that logic. That is:
    # - for each broker transaction extracted,
    # - an existing award that it belongs to was found in usaspending
    # - and an existing transaction that it belongs to was found in usaspending

    # One call per batch of transactions to load from broker into usaspending
    assert mock__bulk_insert_recipient_location.call_count == 1
    assert mock__bulk_insert_recipient.call_count == 1
    assert mock__bulk_insert_place_of_performance.call_count == 1

    # One call per transactions to load from broker into usaspending
    assert mock__matching_award.call_count == 3
    assert mock__lookup_existing_transaction.call_count == 3
    assert mock__update_transaction_normalized_transaction.call_count == 3
    assert mock__update_transaction_fpds_transaction.call_count == 3

    # With all broker data being found in usaspending (so no inserts, only updates)
    mock__insert_award.assert_not_called()
    mock__insert_transaction_normalized_transaction.assert_not_called()
    mock__insert_transaction_fpds_transaction.assert_not_called()

    # Check that the correct data (e.g. IDs) are being propagated via the load_objects dictionary from call to call
    # Check only first transaction iteration
    load_objects_pre_transaction = mock__lookup_existing_transaction.call_args_list[0][0][1]
    final_award_id = mock__matching_award()

    # Compare data is as expected
    assert load_objects_pre_transaction["award"]["transaction_unique_id"] == str(dummy_broker_ids[0])
    assert load_objects_pre_transaction["transaction_normalized"]["transaction_unique_id"] == str(dummy_broker_ids[0])
    assert load_objects_pre_transaction["transaction_normalized"]["award_id"] == final_award_id
    assert 2001 <= load_objects_pre_transaction["transaction_normalized"]["fiscal_year"] <= 2019


def test_setup_load_lists(monkeypatch):
    test_object = {"table": {"val1": 4, "string_val": "bob"}, "wrong_table": {"val": "wrong"}}

    columns, values, pairs = format_insert_or_update_column_sql(
        mock_cursor(monkeypatch, "mogrified"), test_object, "table"
    )
    # code is non-deterministic in order to be performant
    assert columns == '("val1","string_val")' or columns == '("string_val","val1")'
    assert values == "(4,bob)" or values == "(bob,4)"
    assert pairs == " val1=4, string_val=bob" or pairs == " string_val=bob, val1=4"


def test_load_from_broker(monkeypatch):
    # There isn't much to test here since this is just a basic DB query
    mock_cursor(monkeypatch, ["mock thing 1", "mock thing 2"])
    assert _extract_broker_objects([17459650, 678]) == ["mock thing 1", "mock thing 2"]


# This only runs for one of the most simple tables, since this is supposed to be a test to ensure that the loader works,
# NOT that the map is accurate
def test_create_load_object(monkeypatch):
    custom_bools = {"is_awesome": "awesome", "is_not_awesome": "nawesome", "reasons_to_not_be_awesome": "noawersn"}

    data = {
        "legal_entity_country_code": "countrycode",
        "legal_entity_country_name": "countryname",
        "legal_entity_state_code": "statecode",
        "legal_entity_state_descrip": "statedescription",
        "legal_entity_county_code": "countycode",
        "legal_entity_county_name": "countyname",
        "legal_entity_congressional": "congressionalcode",
        "legal_entity_city_name": "cityname",
        "legal_entity_address_line1": "addressline1",
        "legal_entity_address_line2": "addressline2",
        "legal_entity_address_line3": "addressline3",
        "legal_entity_zip4": "zipfour",
        "legal_entity_zip5": "zipfive",
        "legal_entity_zip_last4": "zip last 4",
        "detached_award_proc_unique": 5,
        "is_awesome": "true",
        "is_not_awesome": "false",
        "reasons_to_not_be_awesome": None,
    }
    result = {
        "location_country_code": "COUNTRYCODE",
        "country_name": "COUNTRYNAME",
        "state_code": "STATECODE",
        "state_description": "STATEDESCRIPTION",
        "county_code": "COUNTYCODE",
        "county_name": "COUNTYNAME",
        "congressional_code": "CONGRESSIONALCODE",
        "city_name": "CITYNAME",
        "address_line1": "ADDRESSLINE1",
        "address_line2": "ADDRESSLINE2",
        "address_line3": "ADDRESSLINE3",
        "zip4": "ZIPFOUR",
        "zip5": "ZIPFIVE",
        "zip_last4": "ZIP LAST 4",
        "is_fpds": True,
        "place_of_performance_flag": False,
        "recipient_flag": True,
        "transaction_unique_id": 5,
        "awesome": "true",
        "nawesome": "false",
        "noawersn": False,
    }
    mock_cursor(monkeypatch, data)

    actual_result = _create_load_object(data, recipient_location_nonboolean_columns, custom_bools, recipient_location_functions)
    actual_result.pop("create_date", None)
    actual_result.pop("update_date", None)
    assert actual_result == result


# Mostly testing that everything gets the primary keys it was looking for
def test_load_transactions(monkeypatch):
    mega_key_list = {}
    mega_key_list.update(transaction_fpds_nonboolean_columns)
    mega_key_list.update(transaction_normalized_nonboolean_columns)
    mega_key_list.update(legal_entity_nonboolean_columns)
    mega_key_list.update(recipient_location_nonboolean_columns)
    mega_key_list.update(recipient_location_functions)
    mega_key_list.update(place_of_performance_nonboolean_columns)
    mega_key_list.update(place_of_performance_functions)

    unique_val = 1
    for key in mega_key_list.keys():
        mega_key_list[key] = "value{}".format(unique_val)

    mega_boolean_key_list = {}
    mega_boolean_key_list.update(transaction_fpds_boolean_columns)
    mega_boolean_key_list.update(legal_entity_boolean_columns)

    for key in mega_boolean_key_list:
        mega_boolean_key_list[key] = "false"

    mega_key_list.update(mega_boolean_key_list)

    mega_key_list["action_date"] = "2007-10-01 00:00:00"

    load_objects = _transform_objects([mega_key_list])

    mock_cursor(monkeypatch, [[10]])  # ids for linked objects. No real DB so can't check that they really link
    _load_transactions(load_objects)
