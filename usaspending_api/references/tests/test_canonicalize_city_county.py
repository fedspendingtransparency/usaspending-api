import pytest

from model_mommy import mommy
from usaspending_api.references.models import RefCityCountyCode


@pytest.mark.django_db
def test_canonicalize_city_county():
    mommy.make("references.RefCityCountyCode", _fill_optional=True, _quantity=3)
    RefCityCountyCode.canonicalize()
    for cccode in RefCityCountyCode.objects.all():
        assert cccode.city_name == cccode.city_name.upper().strip()
        assert cccode.county_name == cccode.county_name.upper().strip()
