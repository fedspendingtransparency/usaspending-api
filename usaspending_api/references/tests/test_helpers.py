from copy import copy
import pytest

from usaspending_api.references import helpers as h
from usaspending_api.references.models import Location


def test_canonicalize_string():
    raw = " Däytön\n"
    assert h.canonicalize_string(raw) == "DÄYTÖN"


raw = {
    "state_code": "oh",  # state_code not canonicalized
    "city_name": " Dayton\n",
    "foreign_city_name": " Däytön\n",
    "country_name": "\t\t\tusa",  # also not canonicalized
    "state_name": "oHIo ",
    "county_name": " montgomery ",
    "address_line1": "200   w 2nd\tST.",
    "address_line2": "#100",
    "address_line3": "suite\n22",
    "foreign_province": " ontariO",
    "foreign_city_name": " Däytön\n",
    "place_of_perform_county_na": "Montgomery",
}

desired = {
    "address_line1": "200 W 2ND ST.",
    "address_line2": "#100",
    "address_line3": "SUITE 22",
    "city_name": "DAYTON",
    "country_name": "\t\t\tusa",
    "county_name": "MONTGOMERY",
    "foreign_city_name": "DÄYTÖN",
    "foreign_province": "ONTARIO",
    "state_code": "oh",
    "state_name": "OHIO",
    "place_of_perform_county_na": "MONTGOMERY",
}

desired_actual_field_names = copy(desired)
desired_actual_field_names.pop("place_of_perform_county_na")


def test_canonicalize_location_dict():
    assert h.canonicalize_location_dict(raw) == desired


@pytest.mark.django_db
def test_canonicalize_location_inst():
    field_names = [f.name for f in Location._meta.fields]
    valid_fields = {k: raw[k] for k in raw if k in field_names}
    loc = Location(**valid_fields)
    h.canonicalize_location_instance(loc)
    loc.save()
    actual = set(loc.__dict__.items())
    desired_set = set(desired_actual_field_names.items())
    assert not (desired_set - actual)
