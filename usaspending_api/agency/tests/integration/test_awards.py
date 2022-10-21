import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def transaction_search_1():

    # Submission
    dsws = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2021-04-09",
        submission_fiscal_year=2021,
        submission_fiscal_month=7,
        submission_fiscal_quarter=3,
        is_quarter=False,
        period_start_date="2021-03-01",
        period_end_date="2021-04-01",
    )
    baker.make("submissions.SubmissionAttributes", toptier_code="001", submission_window=dsws)
    baker.make("submissions.SubmissionAttributes", toptier_code="002", submission_window=dsws)

    # Toptier and Awarding Agency
    toptier_agency_1 = baker.make(
        "references.ToptierAgency",
        toptier_code="001",
    )

    toptier_agency_2 = baker.make(
        "references.ToptierAgency",
        toptier_code="002",
    )

    awarding_agency_1 = baker.make("references.Agency", toptier_agency=toptier_agency_1, toptier_flag=True)
    awarding_agency_2 = baker.make("references.Agency", toptier_agency=toptier_agency_2, toptier_flag=True)

    # Awards
    award_contract = baker.make("awards.Award", category="contract")
    award_idv = baker.make("awards.Award", category="idv")
    award_grant = baker.make("awards.Award", category="grant")
    award_loan = baker.make("awards.Award", category="loans")
    award_dp = baker.make("awards.Award", category="direct payment")

    baker.make(
        TransactionNormalized,
        id=1,
        award=award_contract,
        federal_action_obligation=101,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="A",
    )
    baker.make(
        TransactionFABS, transaction_id=1, awarding_agency_code="001", awarding_agency_name=toptier_agency_1.name
    )

    baker.make(
        TransactionNormalized,
        id=2,
        award=award_idv,
        federal_action_obligation=102,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="IDV_A",
    )
    baker.make(
        TransactionFABS, transaction_id=2, awarding_agency_code="001", awarding_agency_name=toptier_agency_1.name
    )

    baker.make(
        TransactionNormalized,
        id=3,
        award=award_grant,
        federal_action_obligation=103,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="02",
    )
    baker.make(
        TransactionFABS, transaction_id=3, awarding_agency_code="001", awarding_agency_name=toptier_agency_1.name
    )

    baker.make(
        TransactionNormalized,
        id=4,
        award=award_loan,
        federal_action_obligation=104,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="08",
    )
    baker.make(
        TransactionFABS, transaction_id=4, awarding_agency_code="001", awarding_agency_name=toptier_agency_1.name
    )

    baker.make(
        TransactionNormalized,
        id=5,
        award=award_dp,
        federal_action_obligation=105,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        type="10",
    )
    baker.make(
        TransactionFABS, transaction_id=5, awarding_agency_code="001", awarding_agency_name=toptier_agency_1.name
    )

    # Alternate Year
    baker.make(
        TransactionNormalized,
        id=6,
        award=award_idv,
        federal_action_obligation=300,
        action_date="2020-04-01",
        awarding_agency=awarding_agency_1,
        type="IDV_A",
    )
    baker.make(
        TransactionFABS, transaction_id=6, awarding_agency_code="001", awarding_agency_name=toptier_agency_1.name
    )

    # Alternate Agency
    baker.make(
        TransactionNormalized,
        id=7,
        award=award_idv,
        federal_action_obligation=400,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_2,
        type="IDV_C",
    )
    baker.make(
        TransactionFABS, transaction_id=7, awarding_agency_code="002", awarding_agency_name=toptier_agency_2.name
    )


url = "/api/v2/agency/{toptier_code}/awards/{filter}"


@pytest.mark.django_db
def test_all_categories(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021"))

    expected_results = {
        "fiscal_year": 2021,
        "latest_action_date": "2021-04-01T00:00:00",
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
        "latest_action_date": "2021-04-01T00:00:00",
        "toptier_code": "001",
        "transaction_count": 1,
        "obligations": 101.0,
        "messages": [],
    }
    assert resp.json() == expected_results

    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021&award_type_codes=[10]"))

    expected_results = {
        "fiscal_year": 2021,
        "latest_action_date": "2021-04-01T00:00:00",
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
        "latest_action_date": "2020-04-01T00:00:00",
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
        "latest_action_date": "2021-04-01T00:00:00",
        "toptier_code": "002",
        "transaction_count": 1,
        "obligations": 400.0,
        "messages": [],
    }

    assert resp.json() == expected_results


@pytest.mark.django_db
def test_invalid_agency(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
def test_no_award_data_for_fy(client, transaction_search_1, elasticsearch_transaction_index):
    resp = client.get(url.format(toptier_code="002", filter="?fiscal_year=2017"))
    expected_result = {
        "fiscal_year": 2017,
        "latest_action_date": None,
        "toptier_code": "002",
        "transaction_count": 0,
        "obligations": 0.0,
        "messages": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
