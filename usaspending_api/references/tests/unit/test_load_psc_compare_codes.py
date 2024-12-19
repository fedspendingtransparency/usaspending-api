from usaspending_api.references.management.commands.load_psc import compare_codes


def test_load_psc_compare_codes():
    old_pscs = {
        (1, "delete me"),
        (2, "unchanged"),
        (3, "update me"),
    }
    new_pscs = {
        (2, "unchanged"),
        (3, "I'm updated"),
        (4, "added"),
    }
    result = compare_codes(old_pscs, new_pscs)
    expected = {
        "added": {(4, "added")},
        "updated": [{"old": (3, "update me"), "new": (3, "I'm updated")}],
        "unchanged": {(2, "unchanged")},
        "deleted": {(1, "delete me")},
    }
    assert result == expected
