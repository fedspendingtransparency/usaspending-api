from usaspending_api.etl.es_etl_helpers import (
    convert_postgres_array_as_string_to_list,
    convert_postgres_json_array_as_string_to_list,
    convert_postgres_json_as_string_to_sorted_json,
)


def test_convert_postgres_array_as_string_to_list():
    empty_array = "{}"
    array_length_one = "{ITEM 1}"
    array_length_two = "{ITEM 1,ITEM 2}"

    expected_empty = None
    expected_length_one = ["ITEM 1"]
    expected_length_two = ["ITEM 1", "ITEM 2"]

    assert convert_postgres_array_as_string_to_list(empty_array) == expected_empty
    assert convert_postgres_array_as_string_to_list(array_length_one) == expected_length_one
    assert convert_postgres_array_as_string_to_list(array_length_two) == expected_length_two


def test_convert_postgres_json_array_as_string_to_list():
    empty_string = ""
    array_length_one = '[{"ITEM 1": "HELLO"}]'
    array_length_two = '[{"ITEM 2":"WORLD","ITEM 1":"HELLO"}, {"ITEM 3":"BLAH"}]'

    expected_empty = None
    expected_length_one = ['{"ITEM 1": "HELLO"}']
    expected_length_two = ['{"ITEM 1": "HELLO", "ITEM 2": "WORLD"}', '{"ITEM 3": "BLAH"}']

    assert convert_postgres_json_array_as_string_to_list(empty_string) == expected_empty
    assert convert_postgres_json_array_as_string_to_list(array_length_one) == expected_length_one
    assert convert_postgres_json_array_as_string_to_list(array_length_two) == expected_length_two


def test_convert_postgres_json_as_string_to_sorted_json():
    empty_string = ""
    json_length_one = '{"ITEM 1":"HELLO"}'
    json_length_two = '{"ITEM 2":"WORLD","ITEM 1":"HELLO"}'

    expected_empty_string = None
    expected_length_one = '{"ITEM 1": "HELLO"}'
    expected_length_two = '{"ITEM 1": "HELLO", "ITEM 2": "WORLD"}'

    assert convert_postgres_json_as_string_to_sorted_json(empty_string) == expected_empty_string
    assert convert_postgres_json_as_string_to_sorted_json(json_length_one) == expected_length_one
    assert convert_postgres_json_as_string_to_sorted_json(json_length_two) == expected_length_two
