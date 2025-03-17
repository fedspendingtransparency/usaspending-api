import pytest

from usaspending_api.references.management.commands.load_psc import compare_codes
from usaspending_api.references.models import PSC


@pytest.fixture
def old_pscs():
    return {
        "1": PSC(code="1", description="delete me"),
        "2": PSC(code="2", description="unchanged"),
        "3": PSC(code="3", description="update me"),
    }


@pytest.fixture
def new_pscs():
    return {
        "2": PSC(code="2", description="unchanged"),
        "3": PSC(code="3", description="I'm updated"),
        "4": PSC(code="4", description="add me"),
    }


def test_load_psc_compare_codes(old_pscs, new_pscs):
    result = compare_codes(old_pscs, new_pscs)
    expected = {
        "added": {"4": PSC(code="4", description="add me")},
        "updated": {
            "3": {"old": PSC(code="3", description="update me"), "new": PSC(code="3", description="I'm updated")}
        },
        "unchanged": {"2": PSC(code="2", description="unchanged")},
        "deleted": {"1": PSC(code="1", description="delete me")},
    }
    assert result == expected
