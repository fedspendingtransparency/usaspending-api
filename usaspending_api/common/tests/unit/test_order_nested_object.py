from collections import OrderedDict
from usaspending_api.common.helpers.dict_helpers import order_nested_object


def test_order_nested_object():
    assert order_nested_object("A") == "A"
    assert order_nested_object([2, 1]) == [1, 2]
    assert order_nested_object({"B": 1, "A": 2}) == OrderedDict((("A", 2), ("B", 1)))
    assert order_nested_object({"B": [2, 1], "A": [4, 3]}) == OrderedDict((("A", [3, 4]), ("B", [1, 2])))
    assert order_nested_object({"tas_codes": [2, 1]}) == {"tas_codes": [1, 2]}

    # Not "require": [[]] so should sort.
    assert order_nested_object({"tas_codes": {"require": {"A": [2, 1]}}}) == {"tas_codes": {"require": {"A": [1, 2]}}}

    # Inner lists for require and exclude for naics_codes, psc_codes, and tas_codes should not
    # be sorted.  Everything else should be.
    assert order_nested_object(
        {
            "tas_codes": {
                "whatever": [["Service", "B", "B5", "B502"], ["D", "C"]],
                "require": [["Service", "B", "B5"], ["D", "C", "A"]],
                "exclude": [["Service", "B", "B5", "B502"], ["D", "C"]],
            },
            "some_other_codes_we_dont_care_about": {
                "whatever": [["Service", "B", "B5", "B502"], ["D", "C"]],
                "require": [["Service", "B", "B5"], ["D", "C", "A"]],
                "exclude": [["Service", "B", "B5", "B502"], ["D", "C"]],
            },
            "psc_codes": {
                "whatever": [["Service", "B", "B5", "B502"], ["D", "C"]],
                "require": [["Service", "B", "B5"], ["D", "C", "A"]],
                "exclude": [["Service", "B", "B5", "B502"], ["D", "C"]],
            },
            "naics_codes": {
                "whatever": [["Service", "B", "B5", "B502"], ["D", "C"]],
                "require": [["Service", "B", "B5"], ["D", "C", "A"]],
                "exclude": [["Service", "B", "B5", "B502"], ["D", "C"]],
            },
        }
    ) == OrderedDict(
        [
            (
                "naics_codes",
                OrderedDict(
                    [
                        ("exclude", [["D", "C"], ["Service", "B", "B5", "B502"]]),
                        ("require", [["D", "C", "A"], ["Service", "B", "B5"]]),
                        ("whatever", [["B", "B5", "B502", "Service"], ["C", "D"]]),
                    ]
                ),
            ),
            (
                "psc_codes",
                OrderedDict(
                    [
                        ("exclude", [["D", "C"], ["Service", "B", "B5", "B502"]]),
                        ("require", [["D", "C", "A"], ["Service", "B", "B5"]]),
                        ("whatever", [["B", "B5", "B502", "Service"], ["C", "D"]]),
                    ]
                ),
            ),
            (
                "some_other_codes_we_dont_care_about",
                OrderedDict(
                    [
                        ("exclude", [["B", "B5", "B502", "Service"], ["C", "D"]]),
                        ("require", [["A", "C", "D"], ["B", "B5", "Service"]]),
                        ("whatever", [["B", "B5", "B502", "Service"], ["C", "D"]]),
                    ]
                ),
            ),
            (
                "tas_codes",
                OrderedDict(
                    [
                        ("exclude", [["D", "C"], ["Service", "B", "B5", "B502"]]),
                        ("require", [["D", "C", "A"], ["Service", "B", "B5"]]),
                        ("whatever", [["B", "B5", "B502", "Service"], ["C", "D"]]),
                    ]
                ),
            ),
        ]
    )
