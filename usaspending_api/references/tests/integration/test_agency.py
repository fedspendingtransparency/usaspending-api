import datetime

import pytest
from model_bakery import baker

from usaspending_api.references.models import Agency


@pytest.mark.django_db
def test_get_by_toptier():
    """Test Agency lookup by toptier CGAC code."""
    toptier = baker.make("references.ToptierAgency", toptier_code="xyz", name="yo", _fill_optional=True)
    subtier = baker.make("references.SubtierAgency", subtier_code="abc", name="yo", _fill_optional=True)

    baker.make(
        "references.Agency",
        toptier_agency=toptier,
        subtier_agency=baker.make("references.SubtierAgency", subtier_code="bbb", name="no", _fill_optional=True),
        update_date=datetime.date(2017, 10, 10),
    )
    agency1 = baker.make("references.Agency", toptier_agency=toptier, subtier_agency=subtier, _fill_optional=True)

    # lookup should return agency w/ most recent update_date that
    # matches the cgac code
    assert Agency.get_by_toptier("xyz") == agency1
    # If there's no match, we should get none
    assert Agency.get_by_toptier("nope") is None


@pytest.mark.django_db
def test_get_by_subtier():
    """Test Agency lookup by subtier."""
    toptier = baker.make("references.ToptierAgency", toptier_code="xyz", name="yo", _fill_optional=True)
    subtier = baker.make("references.SubtierAgency", subtier_code="abc", name="hi", _fill_optional=True)

    baker.make(
        "references.Agency",
        toptier_agency=toptier,
        subtier_agency=baker.make("references.SubtierAgency", subtier_code="bbb", _fill_optional=True),
    )
    agency1 = baker.make("references.Agency", toptier_agency=toptier, subtier_agency=subtier, _fill_optional=True)

    # lookup should return agency w/ most recent updatea_date that
    # matches the subtier code
    assert Agency.get_by_subtier("abc") == agency1
    # if there's no match, we should get none
    assert Agency.get_by_subtier("nope") is None
    # if called with an empty argument, we should get None
    assert Agency.get_by_subtier("") is None
    assert Agency.get_by_subtier(None) is None


@pytest.mark.django_db
def test_get_by_toptier_subtier():
    """Test Agency lookup by subtier."""
    toptier = baker.make("references.ToptierAgency", toptier_code="xyz", name="yo", _fill_optional=True)
    subtier = baker.make("references.SubtierAgency", subtier_code="abc", name="hi", _fill_optional=True)

    baker.make(
        "references.Agency",
        toptier_agency=toptier,
        subtier_agency=baker.make("references.SubtierAgency", subtier_code="bbb", _fill_optional=True),
    )
    agency1 = baker.make("references.Agency", toptier_agency=toptier, subtier_agency=subtier, _fill_optional=True)

    # lookup should return agency w/ most recent updated_date that
    # matches the toptier and subtier code
    assert Agency.get_by_toptier_subtier("xyz", "abc") == agency1
    # if there's no match, we should get none
    assert Agency.get_by_toptier_subtier("nope", "nada") is None
    assert Agency.get_by_toptier_subtier("xyz", "nada") is None
    assert Agency.get_by_toptier_subtier("nope", "bbb") is None
