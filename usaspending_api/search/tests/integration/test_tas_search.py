# Stdlib imports
import pytest
import json

from datetime import datetime
from rest_framework import status

# Core Django imports
from django.conf import settings

# Third-party app imports
from fiscalyear import FiscalDate
from model_bakery import baker

# Imports from your apps
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def mock_tas_data(db):

    a1 = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=99,
        name="Department of Pizza",
        toptier_code="DOP",
        abbreviation="DOP",
    )
    a2 = baker.make(
        "references.SubtierAgency",
        subtier_agency_id=22,
        name="Department of Sub-Pizza",
        abbreviation="DOSP",
        subtier_code="DOSP",
    )
    baker.make("references.Agency", id=1, toptier_agency=a1, subtier_agency=a2, _fill_optional=True)
    baker.make(FederalAccount, id=1, parent_toptier_agency_id=99, agency_identifier="99", main_account_code="0001")
    baker.make(
        TreasuryAppropriationAccount,
        treasury_account_identifier=1,
        allocation_transfer_agency_id="028",
        agency_id="028",
        federal_account_id=1,
        main_account_code="8006",
        sub_account_code="000",
        availability_type_code="X",
        beginning_period_of_availability="2011",
        ending_period_of_availability="2013",
        tas_rendering_label="028-028-2011/2013-X-8006-000",
    )
    baker.make(
        TreasuryAppropriationAccount,
        treasury_account_identifier=2,
        allocation_transfer_agency_id="004",
        agency_id="028",
        federal_account_id=1,
        main_account_code="8006",
        sub_account_code="005",
        availability_type_code=None,
        beginning_period_of_availability="2012",
        ending_period_of_availability="2013",
        tas_rendering_label="004-028-2012/2013-8006-005",
    )
    baker.make(
        TreasuryAppropriationAccount,
        treasury_account_identifier=3,
        allocation_transfer_agency_id="001",
        agency_id="011",
        federal_account_id=1,
        main_account_code="8007",
        sub_account_code="001",
        availability_type_code="X",
        beginning_period_of_availability="2001",
        ending_period_of_availability="2002",
        tas_rendering_label="001-011-2001/2002-X-8007-001",
    )

    baker.make(FinancialAccountsByAwards, treasury_account_id=1, award_id=1)
    baker.make(FinancialAccountsByAwards, treasury_account_id=2, award_id=2)
    baker.make(FinancialAccountsByAwards, treasury_account_id=3, award_id=3)

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        action_date="2010-10-01",
        award_id=1,
        is_fpds=True,
        type="A",
        awarding_agency_id=1,
        awarding_agency_code="DOP",
        awarding_toptier_agency_name="Department of Pizza",
        awarding_sub_tier_agency_c="DOSP",
        awarding_subtier_agency_name="Department of Sub-Pizza",
        recipient_location_city_name="BURBANK",
        recipient_location_country_code="USA",
        recipient_location_state_code="CA",
        piid="piiiiid",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_city_name="AUSTIN",
        pop_state_code="TX",
        tas_components=["aid=028main=8006ata=028sub=000bpoa=2011epoa=2013a=X"],
    )

    baker.make(
        "search.AwardSearch",
        award_id=1,
        is_fpds=True,
        latest_transaction_id=1,
        piid="piid",
        type="A",
        awarding_agency_id=1,
        tas_components="{aid=028main=8006ata=028sub=000bpoa=2011epoa=2013a=X}",
        action_date="2020-01-01",
        subaward_count=1,
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        is_fpds=True,
        latest_transaction_id=1,
        piid="piid2",
        type="B",
        tas_components="{aid=028main=8006ata=004sub=005bpoa=2012epoa=2013a=X}",
        action_date="2020-01-01",
        subaward_count=1,
    )
    baker.make(
        "search.AwardSearch",
        award_id=3,
        is_fpds=True,
        latest_transaction_id=1,
        piid="piid3",
        type="C",
        tas_components="{aid=011main=8007ata=001sub=001bpoa=2001epoa=2002a=X}",
        action_date="2020-01-01",
        subaward_count=1,
    )

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award_id=1,
        subaward_amount=123.45,
        prime_award_type="A",
        prime_award_group="procurement",
        subaward_number="1A",
        treasury_account_identifiers=[1],
        action_date="2020-01-01",
        sub_action_date="2020-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        award_id=2,
        subaward_amount=5000.00,
        prime_award_type="A",
        prime_award_group="procurement",
        subaward_number="2A",
        treasury_account_identifiers=[2],
        action_date="2020-01-01",
        sub_action_date="2020-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        award_id=3,
        subaward_amount=0.00,
        prime_award_type="A",
        prime_award_group="procurement",
        subaward_number="3A",
        treasury_account_identifiers=[3],
        action_date="2020-01-01",
        sub_action_date="2020-01-01",
    )

    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")


@pytest.mark.django_db
def test_spending_by_award_tas_success(client, monkeypatch, elasticsearch_award_index, mock_tas_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    data = {
        "filters": {"tas_codes": [{"aid": "028", "main": "8006"}], "award_type_codes": ["A", "B", "C", "D"]},
        "fields": ["Award ID"],
        "page": 1,
        "limit": 60,
        "sort": "Award ID",
        "order": "desc",
        "subawards": False,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    data = {
        "filters": {"tas_codes": [{"aid": "011", "main": "8007"}], "award_type_codes": ["A", "B", "C", "D"]},
        "fields": ["Award ID"],
        "subawards": False,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_spending_by_award_tas_dates(client, monkeypatch, elasticsearch_award_index, mock_tas_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    data = {
        "filters": {
            "tas_codes": [{"aid": "028", "main": "8006", "bpoa": "2011"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": ["Award ID"],
        "subawards": False,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    data = {
        "filters": {
            "tas_codes": [{"aid": "028", "main": "8006", "epoa": "2013"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": ["Award ID"],
        "subawards": False,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2


@pytest.mark.django_db
def test_spending_by_award_tas_sub_account(client, monkeypatch, elasticsearch_award_index, mock_tas_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    data = {
        "filters": {
            "tas_codes": [{"aid": "028", "main": "8006", "sub": "000"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": ["Award ID"],
        "subawards": False,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    data = {
        "filters": {
            "tas_codes": [{"aid": "028", "main": "8006", "sub": "005"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": ["Award ID"],
        "subawards": False,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_spending_by_award_tas_ata(client, monkeypatch, elasticsearch_award_index, mock_tas_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    data = {
        "filters": {
            "tas_codes": [{"aid": "028", "main": "8006", "ata": "004"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": ["Award ID"],
        "subawards": False,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_spending_by_award_subaward_success(client, mock_tas_data, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    data = {
        "filters": {"tas_codes": [{"aid": "028", "main": "8006"}], "award_type_codes": ["A", "B", "C", "D"]},
        "fields": ["Sub-Award ID"],
        "subawards": True,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    data = {
        "filters": {
            "tas_codes": [{"aid": "028", "main": "8006", "ata": "004"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": ["Sub-Award ID"],
        "subawards": True,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_spending_by_award_subaward_failure(client, mock_tas_data):
    data = {
        "filters": {"tas_codes": [{"aid": "000", "main": "0000"}], "award_type_codes": ["A", "B", "C", "D"]},
        "fields": ["Sub-Award ID"],
        "subawards": True,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0


@pytest.mark.django_db
def test_spending_over_time(client, monkeypatch, elasticsearch_transaction_index, mock_tas_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    data = {"group": "fiscal_year", "filters": {"tas_codes": [{"aid": "028", "main": "8006"}]}, "subawards": False}
    resp = client.post("/api/v2/search/spending_over_time", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    earliest_fiscal_year_we_care_about = datetime.strptime(settings.API_SEARCH_MIN_DATE, "%Y-%m-%d").year
    assert len(resp.data["results"]) == FiscalDate.today().fiscal_year - earliest_fiscal_year_we_care_about


@pytest.mark.django_db
def test_spending_by_geography(client, monkeypatch, elasticsearch_transaction_index, mock_tas_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    data = {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "filters": {"tas_codes": [{"aid": "028", "main": "8006"}]},
        "subawards": False,
    }
    resp = client.post("/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_spending_by_category(client, monkeypatch, elasticsearch_transaction_index, mock_tas_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    data = {
        "filters": {"tas_codes": [{"aid": "028", "main": "8006"}]},
        "subawards": False,
    }
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency", content_type="application/json", data=json.dumps(data)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
