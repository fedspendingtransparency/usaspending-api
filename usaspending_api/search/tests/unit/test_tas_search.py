# Stdlib imports
import pytest
import json

from datetime import datetime
from rest_framework import status

# Core Django imports
from django.conf import settings

# Third-party app imports
from fiscalyear import FiscalDate
from model_mommy import mommy

# Imports from your apps
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def mock_tas_data(db):

    a1 = mommy.make("references.ToptierAgency", toptier_agency_id=99, name="Department of Pizza", abbreviation="DOP")
    a2 = mommy.make(
        "references.SubtierAgency", subtier_agency_id=22, name="Department of Sub-Pizza", abbreviation="DOSP"
    )
    mommy.make("references.Agency", id=1, toptier_agency=a1, subtier_agency=a2)
    mommy.make(FederalAccount, id=1, parent_toptier_agency_id=99, agency_identifier="99", main_account_code="0001")
    mommy.make(
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
    mommy.make(
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
    mommy.make(
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

    mommy.make(FinancialAccountsByAwards, treasury_account_id=1, award_id=1)
    mommy.make(FinancialAccountsByAwards, treasury_account_id=2, award_id=2)
    mommy.make(FinancialAccountsByAwards, treasury_account_id=3, award_id=3)

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        action_date="2010-10-01",
        award_id=1,
        is_fpds=True,
        type="A",
        awarding_agency_id=1,
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        legal_entity_city_name="BURBANK",
        legal_entity_country_code="USA",
        legal_entity_state_code="CA",
        piid="piiiiid",
        place_of_perform_city_name="AUSTIN",
        place_of_performance_state="TX",
        place_of_perform_country_c="USA",
    )

    mommy.make("awards.Award", id=1, is_fpds=True, latest_transaction_id=1, piid="piid", type="A", awarding_agency_id=1)
    mommy.make("awards.Award", id=2, is_fpds=True, latest_transaction_id=1, piid="piid2", type="B")
    mommy.make("awards.Award", id=3, is_fpds=True, latest_transaction_id=1, piid="piid3", type="C")

    mommy.make(
        "awards.Subaward",
        id=1,
        award_id=1,
        amount=123.45,
        prime_award_type="A",
        award_type="procurement",
        subaward_number="1A",
    )
    mommy.make(
        "awards.Subaward",
        id=2,
        award_id=2,
        amount=5000.00,
        prime_award_type="A",
        award_type="procurement",
        subaward_number="2A",
    )
    mommy.make(
        "awards.Subaward",
        id=3,
        award_id=3,
        amount=0.00,
        prime_award_type="A",
        award_type="procurement",
        subaward_number="3A",
    )

    mommy.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")


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


def test_spending_by_award_subaward_success(client, mock_tas_data):
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


def test_spending_by_award_subaward_failure(client, mock_tas_data):
    data = {
        "filters": {"tas_codes": [{"aid": "000", "main": "0000"}], "award_type_codes": ["A", "B", "C", "D"]},
        "fields": ["Sub-Award ID"],
        "subawards": True,
    }
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0


def test_spending_over_time(client, monkeypatch, elasticsearch_transaction_index, mock_tas_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    data = {"group": "fiscal_year", "filters": {"tas_codes": [{"aid": "028", "main": "8006"}]}, "subawards": False}
    resp = client.post("/api/v2/search/spending_over_time", content_type="application/json", data=json.dumps(data))
    assert resp.status_code == status.HTTP_200_OK
    earliest_fiscal_year_we_care_about = datetime.strptime(settings.API_SEARCH_MIN_DATE, "%Y-%m-%d").year
    assert len(resp.data["results"]) == FiscalDate.today().fiscal_year - earliest_fiscal_year_we_care_about


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
