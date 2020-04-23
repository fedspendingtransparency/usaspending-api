import pytest

from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.accounts.models import TreasuryAppropriationAccount


def test_unparsible_tas():
    with pytest.raises(UnprocessableEntityException):
        TreasuryAppropriationAccount.tas_rendering_label_to_component_dictionary("badinput")


def test_no_ata_tas():
    assert TreasuryAppropriationAccount.tas_rendering_label_to_component_dictionary("000-2010/2011-0400-080") == {
        "aid": "000",
        "bpoa": "2010",
        "epoa": "2011",
        "main": "0400",
        "sub": "080",
    }


def test_ata_tas():
    assert TreasuryAppropriationAccount.tas_rendering_label_to_component_dictionary("555-000-2010/2011-0400-080") == {
        "ata": "555",
        "aid": "000",
        "bpoa": "2010",
        "epoa": "2011",
        "main": "0400",
        "sub": "080",
    }


def test_availability_type_code_tas():
    assert TreasuryAppropriationAccount.tas_rendering_label_to_component_dictionary("000-X-0400-080") == {
        "aid": "000",
        "a": "X",
        "main": "0400",
        "sub": "080",
    }
