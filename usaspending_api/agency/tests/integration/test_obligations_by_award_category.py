import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.search.models import TransactionSearch
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/agency/{toptier_code}/obligations_by_award_category/{filter}"


@pytest.fixture
def transaction_search_1(db):

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

    awarding_agency_1 = baker.make(
        "references.Agency", toptier_agency=toptier_agency_1, toptier_flag=True, _fill_optional=True
    )
    awarding_agency_2 = baker.make(
        "references.Agency", toptier_agency=toptier_agency_2, toptier_flag=True, _fill_optional=True
    )

    # Awards
    award_contract = baker.make("search.AwardSearch", award_id=1, category="contract")
    award_idv = baker.make("search.AwardSearch", award_id=2, category="idv")
    award_grant = baker.make("search.AwardSearch", award_id=3, category="grant")
    award_loan = baker.make("search.AwardSearch", award_id=4, category="loans")
    award_dp = baker.make("search.AwardSearch", award_id=5, category="direct payment")
    award_bc = baker.make("search.AwardSearch", award_id=6, category="bad_cat")

    baker.make(
        TransactionSearch,
        transaction_id=1,
        award=award_contract,
        award_category=award_contract.category,
        type="D",
        federal_action_obligation=101,
        generated_pragmatic_obligation=101,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        awarding_agency_code=awarding_agency_1.toptier_agency.toptier_code,
    )

    baker.make(
        TransactionSearch,
        transaction_id=2,
        award=award_idv,
        award_category=award_idv.category,
        type="IDV_A",
        federal_action_obligation=102,
        generated_pragmatic_obligation=102,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        awarding_agency_code=awarding_agency_1.toptier_agency.toptier_code,
    )

    baker.make(
        TransactionSearch,
        transaction_id=3,
        award=award_grant,
        award_category=award_grant.category,
        type="02",
        federal_action_obligation=103,
        generated_pragmatic_obligation=103,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        awarding_agency_code=awarding_agency_1.toptier_agency.toptier_code,
    )

    baker.make(
        TransactionSearch,
        transaction_id=4,
        award=award_loan,
        award_category=award_loan.category,
        type="07",
        original_loan_subsidy_cost=104,
        generated_pragmatic_obligation=104,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        awarding_agency_code=awarding_agency_1.toptier_agency.toptier_code,
    )

    baker.make(
        TransactionSearch,
        transaction_id=5,
        award=award_dp,
        award_category=award_dp.category,
        type="06",
        federal_action_obligation=105,
        generated_pragmatic_obligation=105,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        awarding_agency_code=awarding_agency_1.toptier_agency.toptier_code,
    )

    baker.make(
        TransactionSearch,
        transaction_id=6,
        award=award_bc,
        award_category=award_bc.category,
        federal_action_obligation=106,
        generated_pragmatic_obligation=106,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        awarding_agency_code=awarding_agency_1.toptier_agency.toptier_code,
    )
    # Alternate Year
    baker.make(
        TransactionSearch,
        transaction_id=7,
        award=award_idv,
        award_category=award_idv.category,
        type="IDV_A",
        federal_action_obligation=300,
        generated_pragmatic_obligation=300,
        action_date="2020-04-01",
        fiscal_action_date="2020-07-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        awarding_agency_code=awarding_agency_1.toptier_agency.toptier_code,
    )
    # Alternate Agency
    baker.make(
        TransactionSearch,
        transaction_id=8,
        award=award_idv,
        type="IDV_A",
        award_category=award_idv.category,
        federal_action_obligation=400,
        generated_pragmatic_obligation=400,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        awarding_agency_id=awarding_agency_2.id,
        awarding_toptier_agency_id=awarding_agency_2.id,
        awarding_agency_code=awarding_agency_2.toptier_agency.toptier_code,
    )


@pytest.mark.django_db
def test_all_categories(client, monkeypatch, transaction_search_1, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021"))

    expected_results = {
        "total_aggregated_amount": 621.0,
        "results": [
            {"category": "contracts", "aggregated_amount": 101.0},
            {"category": "direct_payments", "aggregated_amount": 105.0},
            {"category": "grants", "aggregated_amount": 103.0},
            {"category": "idvs", "aggregated_amount": 102.0},
            {"category": "loans", "aggregated_amount": 104.0},
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
            {"category": "contracts", "aggregated_amount": 0.0},
            {"category": "direct_payments", "aggregated_amount": 0.0},
            {"category": "grants", "aggregated_amount": 0.0},
            {"category": "idvs", "aggregated_amount": 300.0},
            {"category": "loans", "aggregated_amount": 0.0},
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
            {"category": "contracts", "aggregated_amount": 0.0},
            {"category": "direct_payments", "aggregated_amount": 0.0},
            {"category": "grants", "aggregated_amount": 0.0},
            {"category": "idvs", "aggregated_amount": 400.0},
            {"category": "loans", "aggregated_amount": 0.0},
            {"category": "other", "aggregated_amount": 0.0},
        ],
    }

    assert resp.json() == expected_results


@pytest.mark.django_db
def test_invalid_agency(client, monkeypatch, transaction_search_1):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND
