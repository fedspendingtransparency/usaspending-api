import pytest

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def transaction_search_1():

    # Submission
    dsws = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2021-04-09",
        submission_fiscal_year=2021,
        submission_fiscal_month=7,
        submission_fiscal_quarter=3,
        is_quarter=False,
        period_start_date="2021-03-01",
        period_end_date="2021-04-01",
    )
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
    award_loan = mommy.make("awards.Award", category="loans")
    award_dp = mommy.make("awards.Award", category="direct payment")

    mommy.make(
        TransactionNormalized,
        award=award_contract,
        federal_action_obligation=101,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="A",
    )

    mommy.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=102,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="IDV_A",
    )

    mommy.make(
        TransactionNormalized,
        award=award_grant,
        federal_action_obligation=103,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="02",
    )

    mommy.make(
        TransactionNormalized,
        award=award_loan,
        federal_action_obligation=104,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="08",
    )

    mommy.make(
        TransactionNormalized,
        award=award_dp,
        federal_action_obligation=105,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="10",
    )

    # Alternate Year
    mommy.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=300,
        action_date="2020-04-01",
        awarding_agency=awarding_agency_1,
        type="IDV_A",
    )

    # Alternate Agency
    mommy.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=400,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_2,
        type="IDV_C",
    )


url = "/api/v2/agency/{toptier_code}/awards/{filter}"


@pytest.mark.django_db
def test_all_categories(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021"))

    expected_results = {
        "fiscal_year": 2021,
        "toptier_code": "001",
        "transaction_count": 5,
        "obligations": 411.0,
        "messages": [],
    }

    assert resp.json() == expected_results


@pytest.mark.django_db
def test_award_categories(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021&award_type_codes=[A]"))

    expected_results = {
        "fiscal_year": 2021,
        "toptier_code": "001",
        "transaction_count": 1,
        "obligations": 101.0,
        "messages": [],
    }
    assert resp.json() == expected_results

    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021&award_type_codes=[10]"))

    expected_results = {
        "fiscal_year": 2021,
        "toptier_code": "001",
        "transaction_count": 1,
        "obligations": 105.0,
        "messages": [],
    }
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_alternate_year(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = {
        "fiscal_year": 2020,
        "toptier_code": "001",
        "transaction_count": 1,
        "obligations": 300.0,
        "messages": [],
    }

    assert resp.json() == expected_results


@pytest.mark.django_db
def test_alternate_agency(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="002", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = {
        "fiscal_year": 2021,
        "toptier_code": "002",
        "transaction_count": 1,
        "obligations": 400.0,
        "messages": [],
    }

    assert resp.json() == expected_results


@pytest.mark.django_db
def test_invalid_agency(client, monkeypatch, transaction_search_1, elasticsearch_account_index):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND
