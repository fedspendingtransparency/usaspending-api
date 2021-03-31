import pytest

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


url = "/api/v2/agency/{toptier_code}/obligations_by_award_category/{filter}"


@pytest.fixture
def transaction_search_1():

    # Submission
    dsws = mommy.make("submissions.DABSSubmissionWindowSchedule", id=444, submission_reveal_date="2020-10-09")
    mommy.make("submissions.SubmissionAttributes", toptier_code="001", submission_window=dsws)
    mommy.make("submissions.SubmissionAttributes", toptier_code="002", submission_window=dsws)

    # Toptier and Awarding Agency
    toptier_agency_1 = mommy.make(
        "references.ToptierAgency",
        toptier_code="001",
    )

    toptier_agency_2 = mommy.make(
        "references.ToptierAgency",
        toptier_code="002",
    )

    awarding_agency_1 = mommy.make("references.Agency", toptier_agency=toptier_agency_1, toptier_flag=True)
    awarding_agency_2 = mommy.make("references.Agency", toptier_agency=toptier_agency_2, toptier_flag=True)

    # Awards
    award_contract = mommy.make("awards.Award", category="contract")
    award_idv = mommy.make("awards.Award", category="idv")
    award_grant = mommy.make("awards.Award", category="grant")
    award_loan = mommy.make("awards.Award", category="loan")
    award_dp = mommy.make("awards.Award", category="direct_payment")
    award_bc = mommy.make("awards.Award", category="bad_cat")

    mommy.make(
        TransactionNormalized,
        award=award_contract,
        federal_action_obligation=101,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
    )

    mommy.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=102,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
    )

    mommy.make(
        TransactionNormalized,
        award=award_grant,
        federal_action_obligation=103,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
    )

    mommy.make(
        TransactionNormalized,
        award=award_loan,
        federal_action_obligation=104,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
    )

    mommy.make(
        TransactionNormalized,
        award=award_dp,
        federal_action_obligation=105,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
    )

    mommy.make(
        TransactionNormalized,
        award=award_bc,
        federal_action_obligation=106,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
    )

    # Alternate Year
    mommy.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=300,
        action_date="2020-04-01",
        awarding_agency=awarding_agency_1,
    )

    # Alternate Agency
    mommy.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=400,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_2,
    )


@pytest.mark.django_db
def test_all_categories(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021"))

    expected_results = {
        "total_aggregated_amount": 621.0,
        "results": [
            {"category": "contract", "aggregated_amount": 101.0},
            {"category": "direct_payment", "aggregated_amount": 105.0},
            {"category": "grant", "aggregated_amount": 103.0},
            {"category": "idv", "aggregated_amount": 102.0},
            {"category": "loan", "aggregated_amount": 104.0},
            {"category": "other", "aggregated_amount": 106.0},
        ],
    }

    assert resp.json() == expected_results


@pytest.mark.django_db
def test_alternate_year(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = {
        "total_aggregated_amount": 300.0,
        "results": [
            {"category": "contract", "aggregated_amount": 0.0},
            {"category": "direct_payment", "aggregated_amount": 0.0},
            {"category": "grant", "aggregated_amount": 0.0},
            {"category": "idv", "aggregated_amount": 300.0},
            {"category": "loan", "aggregated_amount": 0.0},
            {"category": "other", "aggregated_amount": 0.0},
        ],
    }

    assert resp.json() == expected_results


@pytest.mark.django_db
def test_alternate_agency(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="002", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = {
        "total_aggregated_amount": 400.0,
        "results": [
            {"category": "contract", "aggregated_amount": 0.0},
            {"category": "direct_payment", "aggregated_amount": 0.0},
            {"category": "grant", "aggregated_amount": 0.0},
            {"category": "idv", "aggregated_amount": 400.0},
            {"category": "loan", "aggregated_amount": 0.0},
            {"category": "other", "aggregated_amount": 0.0},
        ],
    }

    assert resp.json() == expected_results


@pytest.mark.django_db
def test_invalid_agency(client, monkeypatch, transaction_search_1, elasticsearch_account_index):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND
