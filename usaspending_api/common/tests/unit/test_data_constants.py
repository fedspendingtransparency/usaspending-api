from usaspending_api.common.helpers.data_constants import state_code_from_name, state_name_from_code


def test_name_to_code():
    assert state_code_from_name("Maryland") == "MD"
    assert state_code_from_name("MARYLAND") == "MD"
    assert state_code_from_name("MaRyLand") == "MD"
    assert state_code_from_name("total nonsense") is None
    assert state_code_from_name(None) is None


def test_code_to_name():
    assert state_name_from_code("MD") == "Maryland"
    assert state_name_from_code("MH") == "Marshall Islands"
    assert state_name_from_code(None) is None
