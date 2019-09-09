from usaspending_api.data_load.fpds_loader import setup_load_lists


def test_setup_load_lists():
    test_object = {"table": {"val1": 4, "string_val": "bob"}, "wrong_table": {"val": "wrong"}}

    columns, values, pairs = setup_load_lists(test_object, "table")
    # code is non-deterministic in order to be performance
    assert columns == '("val1","string_val")' or columns == '("string_val","val1")'
    assert values == "(4,'bob')" or values == "('bob',4)"
    assert pairs == " val1=4, string_val='bob'" or pairs == " string_val='bob', val1=4"
