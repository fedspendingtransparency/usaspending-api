from usaspending_api.data_load.fpds_loader import (
    setup_load_lists,
    _extract_broker_objects,
    _create_load_object,
    _transform_objects,
    _load_transactions,
)
from usaspending_api.data_load.field_mappings_fpds import (
    transaction_fpds_columns,
    transaction_normalized_columns,
    legal_entity_columns,
    legal_entity_boolean_columns,
    recipient_location_columns,
    recipient_location_functions,
    place_of_performance_columns,
    place_of_performance_functions,
    transaction_fpds_boolean_columns,
)

from unittest.mock import MagicMock


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

    monkeypatch.setattr("psycopg2.connect", mock_connect)

    return result


def test_setup_load_lists():
    test_object = {"table": {"val1": 4, "string_val": "bob"}, "wrong_table": {"val": "wrong"}}

    columns, values, pairs = setup_load_lists(test_object, "table")
    # code is non-deterministic in order to be performance
    assert columns == '("val1","string_val")' or columns == '("string_val","val1")'
    assert values == "(4,'bob')" or values == "('bob',4)"
    assert pairs == " val1=4, string_val='bob'" or pairs == " string_val='bob', val1=4"


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

    actual_result = _create_load_object(data, recipient_location_columns, custom_bools, recipient_location_functions)
    actual_result.pop("create_date", None)
    actual_result.pop("update_date", None)
    assert actual_result == result


# Mostly testing that everything gets the primary keys it was looking for
def test_load_transactions(monkeypatch):
    mega_key_list = {}
    mega_key_list.update(transaction_fpds_columns)
    mega_key_list.update(transaction_normalized_columns)
    mega_key_list.update(legal_entity_columns)
    mega_key_list.update(recipient_location_columns)
    mega_key_list.update(recipient_location_functions)
    mega_key_list.update(place_of_performance_columns)
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
