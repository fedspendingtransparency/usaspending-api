import pytest
from model_bakery import baker

from rest_framework import status
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/agency/awards/count/{filters}"


@pytest.fixture
def award_data(db):
    dabs_submission_window_lazy_ref = "submissions.DABSSubmissionWindowSchedule"
    dabs = baker.make(
        dabs_submission_window_lazy_ref,
        submission_reveal_date="2021-08-09",
        submission_fiscal_year=2021,
        submission_fiscal_month=11,
        submission_fiscal_quarter=4,
        is_quarter=False,
        period_start_date="2021-10-01",
        period_end_date="2021-11-01",
    )

    submission_attributes_lazy_ref = "submissions.SubmissionAttributes"
    baker.make(
        submission_attributes_lazy_ref,
        reporting_fiscal_year=2021,
        reporting_fiscal_period=11,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )

    toptier_agency_lazy_ref = "references.ToptierAgency"
    ta1 = baker.make(toptier_agency_lazy_ref, toptier_agency_id=1, toptier_code=123)
    ta2 = baker.make(toptier_agency_lazy_ref, toptier_agency_id=2, toptier_code=456)
    ta3 = baker.make(toptier_agency_lazy_ref, toptier_agency_id=3, toptier_code=789)

    agency_lazy_ref = "references.Agency"
    ag1 = baker.make(agency_lazy_ref, id=1, toptier_agency=ta1)
    ag2 = baker.make(agency_lazy_ref, id=2, toptier_agency=ta2)
    ag3 = baker.make(agency_lazy_ref, id=3, toptier_agency=ta3)

    awards_lazy_ref = "search.AwardSearch"
    award1 = baker.make(
        awards_lazy_ref,
        award_id=1,
        type="A",
        date_signed="2019-10-15",
        action_date="2019-10-15",
        earliest_transaction_id=10,
        latest_transaction_id=10,
        awarding_agency_id=ag1.id,
        awarding_toptier_agency_code=ta1.toptier_code,
        awarding_toptier_agency_name="Department",
        funding_agency_id=ag2.id,
        funding_toptier_agency_code=ta2.toptier_code,
    )
    award2 = baker.make(
        awards_lazy_ref,
        award_id=2,
        type="B",
        date_signed="2019-12-15",
        action_date="2019-12-15",
        earliest_transaction_id=20,
        latest_transaction_id=20,
        awarding_agency_id=ag1.id,
        awarding_toptier_agency_code=ta1.toptier_code,
        awarding_toptier_agency_name="Department",
        funding_agency_id=ag2.id,
        funding_toptier_agency_code=ta2.toptier_code,
    )
    award3 = baker.make(
        awards_lazy_ref,
        award_id=3,
        type="07",
        date_signed="2020-01-30",
        action_date="2020-01-30",
        earliest_transaction_id=30,
        latest_transaction_id=30,
        awarding_agency_id=ag1.id,
        awarding_toptier_agency_code=ta1.toptier_code,
        awarding_toptier_agency_name="Department",
        funding_agency_id=ag2.id,
        funding_toptier_agency_code=ta2.toptier_code,
    )
    award4 = baker.make(
        awards_lazy_ref,
        award_id=4,
        type="B",
        date_signed="2019-09-30",
        action_date="2019-09-30",
        earliest_transaction_id=40,
        latest_transaction_id=40,
        awarding_agency_id=ag1.id,
        awarding_toptier_agency_code=ta1.toptier_code,
        awarding_toptier_agency_name="Department",
        funding_agency_id=ag2.id,
        funding_toptier_agency_code=ta2.toptier_code,
    )
    award5 = baker.make(
        awards_lazy_ref,
        award_id=5,
        type="08",
        date_signed="2020-12-15",
        action_date="2020-12-15",
        earliest_transaction_id=50,
        latest_transaction_id=50,
        awarding_agency_id=ag1.id,
        awarding_toptier_agency_code=ta1.toptier_code,
        awarding_toptier_agency_name="Department",
        funding_agency_id=ag2.id,
        funding_toptier_agency_code=ta2.toptier_code,
    )
    award6 = baker.make(
        awards_lazy_ref,
        award_id=6,
        type="08",
        date_signed="2021-07-05",
        action_date="2021-07-05",
        earliest_transaction_id=60,
        latest_transaction_id=60,
        awarding_agency_id=ag3.id,
        awarding_toptier_agency_code=ta3.toptier_code,
        awarding_toptier_agency_name="Department of Commerce",
        funding_agency_id=ag2.id,
        funding_toptier_agency_code=ta2.toptier_code,
    )

    transaction_search_lazy_ref = "search.TransactionSearch"
    baker.make(transaction_search_lazy_ref, transaction_id=10, award=award1, action_date="2019-10-15")
    baker.make(transaction_search_lazy_ref, transaction_id=20, award=award2, action_date="2020-12-15")
    baker.make(transaction_search_lazy_ref, transaction_id=30, award=award3, action_date="2020-01-30")
    baker.make(transaction_search_lazy_ref, transaction_id=40, award=award4, action_date="2019-09-30")
    baker.make(transaction_search_lazy_ref, transaction_id=50, award=award5, action_date="2020-12-15")
    baker.make(transaction_search_lazy_ref, transaction_id=60, award=award6, action_date="2021-07-05")


@pytest.mark.django_db
def test_award_count_success(client, monkeypatch, award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.mock_current_fiscal_year(monkeypatch)

    resp = client.get(url.format(filters=""))
    results = resp.data["results"]
    assert resp.status_code == status.HTTP_200_OK
    assert len(results) == 1


@pytest.mark.django_db
def test_award_count_specific_year(client, monkeypatch, award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.get(url.format(filters="?fiscal_year=2019"))
    results = resp.data["results"]
    assert resp.status_code == status.HTTP_200_OK
    assert len(results) == 1

    resp = client.get(url.format(filters="?fiscal_year=2021"))
    results = resp.data["results"]
    assert resp.status_code == status.HTTP_200_OK
    assert len(results) == 1


@pytest.mark.django_db
def test_award_count_cfo_agencies_only(client, monkeypatch, award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.mock_current_fiscal_year(monkeypatch)

    resp = client.get(url.format(filters="?group=cfo"))
    results = resp.data["results"]
    assert resp.status_code == status.HTTP_200_OK
    assert len(results) == 1
