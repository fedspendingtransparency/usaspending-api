import contextlib
import csv
import os
import tempfile

import pytest
from model_mommy import mommy

from usaspending_api.etl import helpers
from usaspending_api.etl.management.commands import load_usaspending_assistance, load_usaspending_contracts
from usaspending_api.references.models import Location
from usaspending_api.references.helpers import canonicalize_location_dict
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.mark.django_db
def test_fetch_country_code():
    """Grab the location with this reference country code"""
    # dummy data
    code_match = mommy.make("references.RefCountryCode", country_code="USA", _fill_optional=True)
    name_match = mommy.make("references.RefCountryCode", country_name="AmErIcA", _fill_optional=True)
    complex_name = mommy.make(
        "references.RefCountryCode", country_name="aMerIca United States of (U.S.A.)", _fill_optional=True
    )
    assert helpers.fetch_country_code("USA") == code_match
    assert helpers.fetch_country_code("USA:more:stuff") == code_match
    assert helpers.fetch_country_code("america") == name_match
    assert helpers.fetch_country_code("aMEriCA: : : :") == name_match
    assert helpers.fetch_country_code("america (u.s.a.):stuff") == complex_name
    assert helpers.fetch_country_code("") is None


@pytest.mark.django_db
def test_get_or_create_location_non_usa():
    """We should query different fields if it's a non-US row"""
    expected = mommy.make(
        "references.Location",
        location_country_code="UAE",
        zip5="12345",
        zip_last4="6789",
        # @todo: can't use _fill_optional on this model because data
        # will exceed allowable index length
        address_line1="line one of address",
        address_line2="line two of address",
        address_line3="line thre of address",
        state_code="GG",
        city_name="AAAAAAAA",
    )

    row = dict(
        vendorcountrycode="UAE",
        zipcode="12345-6789",
        streetaddress=expected.address_line1,
        streetaddress2=expected.address_line2,
        streetaddress3=expected.address_line3,
        state=expected.state_code,
        city=expected.city_name,
    )

    # can't find it because we're looking at the POP fields
    assert (
        helpers.get_or_create_location(row, load_usaspending_contracts.location_mapper_place_of_performance) != expected
    )


@pytest.mark.django_db
def test_get_or_create_location_creates_new_locations():
    """If no location is found, we create a new one"""
    row = dict(
        vendorcountrycode="USA",
        zipcode="12345-6789",
        streetaddress="Addy1",
        streetaddress2="Addy2",
        streetaddress3=None,
        vendor_state_code="ST",
        city="My Town",
    )

    # this canonicalization step runs during load_submission, also
    row = canonicalize_location_dict(row)

    # can't find it because we're looking at the US fields
    assert Location.objects.count() == 0

    helpers.get_or_create_location(row, load_usaspending_contracts.location_mapper_vendor)
    assert Location.objects.count() == 1

    loc = Location.objects.all().first()
    assert loc.location_country_code == "USA"
    assert loc.zip5 == "12345"
    assert loc.zip_last4 == "6789"
    assert loc.address_line1 == "ADDY1"
    assert loc.address_line2 == "ADDY2"
    assert loc.address_line3 is None
    assert loc.state_code == "ST"
    assert loc.city_name == "MY TOWN"


@pytest.mark.django_db
def test_get_or_create_fa_place_of_performance_location_creates_new_locations():
    """If no location is found, we create a new one

    For financial assistance place of performance locations."""
    row = dict(
        principal_place_country_code="USA",
        principal_place_zip="12345-6789",
        principal_place_state_code="OH",
        principal_place_cc="MONTGOMERY",
    )

    # can't find it because we're looking at the US fields
    assert Location.objects.count() == 0

    helpers.get_or_create_location(row, load_usaspending_assistance.location_mapper_fin_assistance_principal_place)
    assert Location.objects.count() == 1

    loc = Location.objects.all().first()
    assert loc.location_country_code == "USA"
    assert loc.zip5 == "12345"
    assert loc.zip_last4 == "6789"
    assert loc.state_code == "OH"
    assert loc.county_name == "MONTGOMERY"


@pytest.mark.django_db
def test_get_or_create_fa_recipient_location_creates_new_locations():
    """If no location is found, we create a new one

    For financial assistance recipient locations."""
    row = dict(
        recipient_country_code="USA",
        recipient_zip="12345-6789",
        recipient_state_code="OH",
        recipient_county_name="MONTGOMERY",
    )

    # can't find it because we're looking at the US fields
    assert Location.objects.count() == 0

    helpers.get_or_create_location(row, load_usaspending_assistance.location_mapper_fin_assistance_recipient)
    assert Location.objects.count() == 1

    loc = Location.objects.all().first()
    assert loc.location_country_code == "USA"
    assert loc.zip5 == "12345"
    assert loc.zip_last4 == "6789"
    assert loc.state_code == "OH"
    assert loc.county_name == "MONTGOMERY"


@contextlib.contextmanager
def mutated_csv(filename, mutator):
    """Returns tempfile copied from filename, but mutated.

    If `filename` points to a CSV, then we load it,
    apply the `mutator` function to each row, and save
    the changed data to a tempfile, which is returned.

    Used to create small, specific variants on test CSVs.
    """

    outfile = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
    with open(filename) as infile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            writer.writerow(mutator(row))
    outfile.close()
    yield outfile
    os.unlink(outfile.name)


@pytest.mark.django_db
def test_get_previous_submission():
    """Test the process for determining the most recent submission in the current FY."""
    # set up some related submissions
    sub1 = mommy.make(
        SubmissionAttributes,
        cgac_code="073",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=9,
        quarter_format_flag=True,
    )
    mommy.make(
        SubmissionAttributes,
        cgac_code="073",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=6,
        quarter_format_flag=True,
    )

    # Submission for same CGAC + a later period should return sub1 as previous submission
    assert helpers.get_previous_submission("073", 2017, 12) == sub1
    # Previous submission lookup should not find a match for an earlier submission
    assert helpers.get_previous_submission("073", 2017, 3) is None
    # Previous submission lookup should not match against a different fiscal year
    assert helpers.get_previous_submission("073", 2018, 3) is None
    # Previous submission lookup should not match against a different agency (CGAC)
    assert helpers.get_previous_submission("ABC", 2017, 12) is None

    mommy.make(
        SubmissionAttributes,
        cgac_code="020",
        reporting_fiscal_year=2016,
        reporting_fiscal_period=6,
        quarter_format_flag=False,
    )

    # Previous submission lookup should only match a quarterly submission
    assert helpers.get_previous_submission("020", 2016, 9) is None
