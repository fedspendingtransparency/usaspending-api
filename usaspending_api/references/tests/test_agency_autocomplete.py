import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.common.tests.autocomplete import check_autocomplete
from usaspending_api.references.models import Agency


@pytest.fixture
def agency_data(db):
    mommy.make(
        Agency,
        toptier_agency__name="Lunar Colonization Society",
        toptier_agency__cgac_code="LCS123",
        _fill_optional=True,
    ),
    mommy.make(
        Agency,
        toptier_agency__name="Cerean Mineral Extraction Corp.",
        toptier_agency__cgac_code="CMEC",
        _fill_optional=True,
    ),
    mommy.make(
        Agency,
        toptier_agency__name="Department of Transportation",
        subtier_agency__name="Department of Transportation",
        toptier_flag=True,
        _fill_optional=True,
    )
    mommy.make(
        Agency,
        toptier_agency__name="Department of Defence",
        subtier_agency__name="Department of the Army",
        toptier_flag=False,
        _fill_optional=True,
    )


@pytest.mark.parametrize(
    "fields,value,expected",
    [
        (["toptier_agency__name"], "ext", {"toptier_agency__name": ["Cerean Mineral Extraction Corp."]}),
        (
            ["toptier_agency__name", "toptier_agency__cgac_code"],
            "123",
            {"toptier_agency__name": [], "toptier_agency__cgac_code": ["LCS123"]},
        ),
    ],
)
@pytest.mark.django_db
def test_agency_autocomplete(client, agency_data, fields, value, expected):
    """test partial-text search on recipients."""

    check_autocomplete("references/agency", client, fields, value, expected)


@pytest.mark.django_db
def test_bad_agency_autocomplete_request(client):
    """Verify error on bad autocomplete request for recipients."""

    resp = client.post("/api/v1/references/agency/autocomplete/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_agency_autocomplete_sorting(client, agency_data):
    """Verify error on bad autocomplete request for recipients."""

    resp = client.post(
        "/api/v1/references/agency/autocomplete/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["subtier_agency__name"],
                "matched_objects": True,
                "value": "Department",
                "order": ["-toptier_flag"],
            }
        ),
    )
    assert resp.data["matched_objects"]["subtier_agency__name"][0]["toptier_flag"]
    assert not resp.data["matched_objects"]["subtier_agency__name"][1]["toptier_flag"]
