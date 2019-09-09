from usaspending_api.data_load.fpds_loader import run_fpds_load, setup_load_lists


def test_setup_load_lists():
    test_object = {"table": {"val1": 4, "string_val": "bob"}, "wrong_table": {"val": "wrong"}}

    print(setup_load_lists(test_object, "table"))
