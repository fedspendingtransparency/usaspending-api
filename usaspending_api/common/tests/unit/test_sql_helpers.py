from usaspending_api.common.helpers.sql_helpers import convert_list_to_sql_array


def test_convert_list_to_sql_array():
    list_of_strings = ["A", "B", "C"]
    result = convert_list_to_sql_array(list_of_strings)
    assert result == "'A','B','C'"

    list_of_ints = [1, 2, 3]
    result = convert_list_to_sql_array(list_of_ints)
    assert result == "1,2,3"
