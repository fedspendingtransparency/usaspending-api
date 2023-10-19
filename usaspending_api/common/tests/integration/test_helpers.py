import pytest
from model_bakery import baker

from usaspending_api.common.helpers.generic_helper import check_valid_toptier_agency


@pytest.mark.django_db
def test_check_valid_toptier_agency_valid():
    baker.make("references.Agency", id=12345, toptier_flag=True, _fill_optional=True)
    assert check_valid_toptier_agency(12345)


@pytest.mark.django_db
def test_check_valid_toptier_agency_invalid():
    baker.make("references.Agency", id=54321, toptier_flag=False, _fill_optional=True)
    assert not check_valid_toptier_agency(54321)
