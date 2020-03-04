import usaspending_api.etl.transaction_loaders.fpds_loader as fpds_loader
import random
import datetime
from usaspending_api.etl.transaction_loaders.fpds_loader import (
    _create_load_object,
    _transform_objects,
    _load_transactions,
)
from usaspending_api.etl.transaction_loaders.field_mappings_fpds import (
    transaction_fpds_nonboolean_columns,
    transaction_normalized_nonboolean_columns,
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
        **{key: str(random.choice([True, False])) for key in transaction_fpds_boolean_columns},
    }


def _stub___extract_broker_objects(id_list):
    """Return a list of dictionaries, in the same structure as `fetchall` when using psycopg2.extras.DictCursor"""
    for id in id_list:
        dummy_row = _assemble_dummy_broker_data()
        dummy_row["detached_award_procurement_id"] = id
        dummy_row["detached_award_proc_unique"] = str(id)
        dummy_row["ordering_period_end_date"] = "2010-01-01 00:00:00"
        dummy_row["action_date"] = "2010-01-01 00:00:00"
        dummy_row["initial_report_date"] = "2010-01-01 00:00:00"
        dummy_row["solicitation_date"] = "2010-01-01 00:00:00"
        dummy_row["created_at"] = datetime.datetime.fromtimestamp(0)
        dummy_row["updated_at"] = datetime.datetime.fromtimestamp(0)
        yield dummy_row


def test_load_ids_empty():
    fpds_loader.load_fpds_transactions([])


# These are patched in opposite order from when they're listed in the function params, because that's how the fixture works
@patch("usaspending_api.etl.transaction_loaders.fpds_loader.connection")
@patch("usaspending_api.etl.transaction_loaders.derived_field_functions_fpds._fetch_subtier_agency_id", return_value=1)
@patch("usaspending_api.etl.transaction_loaders.fpds_loader._extract_broker_objects")
@patch(
    "usaspending_api.etl.transaction_loaders.derived_field_functions_fpds.fy", return_value=random.randint(2001, 2019)
)
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
    mock__fy,
    mock__extract_broker_objects,
    mock___fetch_subtier_agency_id,
    mock_connection,
):
    """
    End-to-end unit test (which should not attempt database connections) to exercise the code-under-test
    independently, given fake broker IDs to load
    """
    ###################
    # BEGIN SETUP MOCKS
    ###################
    # Mock output data of key participant functions in this test scenario
    # This is the baseline unconstrained scenario, where all patched functions' MagicMocks will behave as
    # required by the code

    # Mock the broker objects' data
    mock__extract_broker_objects.side_effect = _stub___extract_broker_objects

    ###################
    # END SETUP MOCKS
    ###################

    # Test run of the loader
    dummy_broker_ids = [101, 201, 301]
    fpds_loader.load_fpds_transactions(dummy_broker_ids)

    # Since the mocks will return "data" always when called, if not told to return "None", the branching logic in
    # load_fpds_transactions like: "lookup award, if not exists, create ... lookup transaction, if not exists, create", will
    # always "find" a *mock* award and transaction.
    # So, assert this baseline run followed that logic. That is:
    # - for each broker transaction extracted,
    # - an existing award that it belongs to was found in usaspending
    # - and an existing transaction that it belongs to was found in usaspending

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
    assert load_objects_pre_transaction["transaction_normalized"]["funding_agency_id"] == 1
    assert load_objects_pre_transaction["transaction_normalized"]["awarding_agency_id"] == 1
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


# This only runs for one of the most simple tables, since this is supposed to be a test to ensure that the loader works,
# NOT that the map is accurate
def test_create_load_object(monkeypatch):
    columns = {"input_field_1": "output_field_1", "input_field_2": "output_field_2"}
    bools = {"input_field_3": "output_field_3", "input_field_4": "output_field_4", "input_field_5": "output_field_5"}
    functions = {"output_field_6": lambda t: t["input_field_6"] * 2, "output_field_7": lambda t: "replacement"}

    data = {
        "input_field_1": "this is field 1",
        "input_field_2": "this is field 2",
        "input_field_3": "true",
        "input_field_4": "false",
        "input_field_5": None,
        "input_field_6": 5,
        "input_field_7": "this is field 7",
    }
    expected_result = {
        "output_field_1": "THIS IS FIELD 1",
        "output_field_2": "THIS IS FIELD 2",
        "output_field_3": "true",
        "output_field_4": "false",
        "output_field_5": False,
        "output_field_6": 10,
        "output_field_7": "replacement",
    }
    mock_cursor(monkeypatch, data)

    actual_result = _create_load_object(data, columns, bools, functions)

    assert actual_result == expected_result


@patch("usaspending_api.etl.transaction_loaders.fpds_loader.connection")
@patch("usaspending_api.etl.transaction_loaders.derived_field_functions_fpds._fetch_subtier_agency_id", return_value=1)
def test_load_transactions(mock__fetch_subtier_agency_id, mock_connection):
    """Mostly testing that everything gets the primary keys it was looking for"""

    # Setup Mocks
    dummy_data = [[10]]
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = None
    mock_cursor.fetchall.return_value = dummy_data
    mock_cursor.mogrify = lambda val1, val2: str(val2[0]).encode()
    mock_connection.connection.cursor().__enter__.return_value = mock_cursor

    mega_key_list = {}
    mega_key_list.update(transaction_fpds_nonboolean_columns)
    mega_key_list.update(transaction_normalized_nonboolean_columns)

    unique_val = 1
    for key in mega_key_list.keys():
        mega_key_list[key] = "value{}".format(unique_val)

    for key in ["ordering_period_end_date", "action_date", "initial_report_date", "solicitation_date"]:
        mega_key_list[key] = "2010-01-01 00:00:00"

    for key in ["created_at", "updated_at"]:
        mega_key_list[key] = datetime.datetime.fromtimestamp(0)

    mega_boolean_key_list = {}
    mega_boolean_key_list.update(transaction_fpds_boolean_columns)

    for key in mega_boolean_key_list:
        mega_boolean_key_list[key] = "false"

    mega_key_list.update(mega_boolean_key_list)

    load_objects = _transform_objects([mega_key_list])
    _load_transactions(load_objects)
