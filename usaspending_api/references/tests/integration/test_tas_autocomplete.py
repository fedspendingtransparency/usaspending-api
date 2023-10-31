import json
import pytest

from model_bakery import baker


BASE_ENDPOINT = "/api/v2/autocomplete/accounts/"


@pytest.fixture
def test_data(db):
    baker.make("references.CGAC", cgac_code="000", agency_name="Agency 000", agency_abbreviation="A000")
    baker.make("references.CGAC", cgac_code="002", agency_name="Agency 002", agency_abbreviation="A002")

    award = baker.make("search.AwardSearch", award_id=1)

    taa = baker.make(
        "accounts.TreasuryAppropriationAccount",
        allocation_transfer_agency_id=None,
        agency_id="000",
        beginning_period_of_availability=None,
        ending_period_of_availability=None,
        availability_type_code=None,
        main_account_code="2121",
        sub_account_code="212",
    )
    baker.make("awards.FinancialAccountsByAwards", treasury_account=taa, award=award)

    taa = baker.make(
        "accounts.TreasuryAppropriationAccount",
        allocation_transfer_agency_id="000",
        agency_id="001",
        beginning_period_of_availability="123456",
        ending_period_of_availability="234567",
        availability_type_code=None,
        main_account_code="1234",
        sub_account_code="321",
    )
    baker.make("awards.FinancialAccountsByAwards", treasury_account=taa, award=award)

    taa = baker.make(
        "accounts.TreasuryAppropriationAccount",
        allocation_transfer_agency_id="001",
        agency_id="002",
        beginning_period_of_availability="923456",
        ending_period_of_availability="934567",
        availability_type_code="X",
        main_account_code="9234",
        sub_account_code="921",
    )
    baker.make("awards.FinancialAccountsByAwards", treasury_account=taa, award=award)

    taa = baker.make(
        "accounts.TreasuryAppropriationAccount",
        allocation_transfer_agency_id="001",
        agency_id="002",
        beginning_period_of_availability="923456",
        ending_period_of_availability="934567",
        availability_type_code="X",
        main_account_code="9234",
        sub_account_code="921",
    )
    baker.make("awards.FinancialAccountsByAwards", treasury_account=taa, award=award)


def _post(client, endpoint, request, expected_response):
    response = client.post(endpoint, json.dumps(request), content_type="application/json")
    assert response.status_code == 200
    assert json.loads(response.content.decode("utf-8")) == expected_response


@pytest.mark.django_db
def test_autocomplete_filters(client, test_data):
    """
    We're going to do one thoroughish set of tests for ata then a bunch of
    happy path tests for the other individual components.
    """
    endpoint = BASE_ENDPOINT + "ata"

    # Test with no filter.
    _post(
        client,
        endpoint,
        {},
        {
            "results": [
                {"ata": "000", "agency_name": "Agency 000", "agency_abbreviation": "A000"},
                {"ata": "001", "agency_name": None, "agency_abbreviation": None},
                {"ata": None, "agency_name": None, "agency_abbreviation": None},
            ]
        },
    )

    # Test with limit.
    _post(
        client,
        endpoint,
        {"limit": 1},
        {"results": [{"ata": "000", "agency_name": "Agency 000", "agency_abbreviation": "A000"}]},
    )

    # Test with filter on component of interest.
    _post(
        client,
        endpoint,
        {"filters": {"ata": "0"}},
        {
            "results": [
                {"ata": "000", "agency_name": "Agency 000", "agency_abbreviation": "A000"},
                {"ata": "001", "agency_name": None, "agency_abbreviation": None},
            ]
        },
    )

    # Test with null filter on component of interest.
    _post(
        client,
        endpoint,
        {"filters": {"ata": None}},
        {"results": [{"ata": None, "agency_name": None, "agency_abbreviation": None}]},
    )

    # Test with a bunch of filters.
    _post(
        client,
        endpoint,
        {
            "filters": {
                "ata": "001",
                "aid": "002",
                "bpoa": "923456",
                "epoa": "934567",
                "a": "X",
                "main": "9234",
                "sub": "921",
            }
        },
        {"results": [{"ata": "001", "agency_name": None, "agency_abbreviation": None}]},
    )


@pytest.mark.django_db
def test_autocomplete_aid(client, test_data):

    _post(
        client,
        BASE_ENDPOINT + "aid",
        {"filters": {"aid": "002"}},
        {"results": [{"aid": "002", "agency_name": "Agency 002", "agency_abbreviation": "A002"}]},
    )
    _post(client, BASE_ENDPOINT + "aid", {"filters": {"aid": "2"}}, {"results": []})


@pytest.mark.django_db
def test_autocomplete_bpoa(client, test_data):

    _post(client, BASE_ENDPOINT + "bpoa", {"filters": {"bpoa": "9"}}, {"results": ["923456"]})
    _post(client, BASE_ENDPOINT + "bpoa", {"filters": {"bpoa": "6"}}, {"results": []})


@pytest.mark.django_db
def test_autocomplete_epoa(client, test_data):

    _post(client, BASE_ENDPOINT + "epoa", {"filters": {"epoa": "9"}}, {"results": ["934567"]})
    _post(client, BASE_ENDPOINT + "epoa", {"filters": {"epoa": "7"}}, {"results": []})


@pytest.mark.django_db
def test_autocomplete_a(client, test_data):

    _post(client, BASE_ENDPOINT + "a", {"filters": {"a": "X"}}, {"results": ["X"]})
    _post(client, BASE_ENDPOINT + "a", {"filters": {"a": "Z"}}, {"results": []})


@pytest.mark.django_db
def test_autocomplete_main(client, test_data):

    _post(client, BASE_ENDPOINT + "main", {"filters": {"main": "9"}}, {"results": ["9234"]})
    _post(client, BASE_ENDPOINT + "main", {"filters": {"main": "4"}}, {"results": []})


@pytest.mark.django_db
def test_autocomplete_sub(client, test_data):

    _post(client, BASE_ENDPOINT + "sub", {"filters": {"sub": "9"}}, {"results": ["921"]})
    _post(client, BASE_ENDPOINT + "sub", {"filters": {"sub": "1"}}, {"results": []})
