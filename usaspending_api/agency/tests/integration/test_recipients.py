import pytest

from model_mommy import mommy
from rest_framework import status
from usaspending_api.recipient.tests.integration.test_recipient import create_recipient_profile_test_data, \
    TEST_RECIPIENT_PROFILES

url = "/api/v2/agency/{toptier_code}/recipients/{filter}"


@pytest.mark.django_db
def setup_recipient_agency():
    for recipient_lookup in recipient_agency_list:
        mommy.make("recipient.RecipientLookup", **recipient_lookup)

@pytest.mark.django_db
def test_all_categories(client):
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021"))

    assert resp.status_code == status.HTTP_200_OK

    # assert resp.json() == expected_results