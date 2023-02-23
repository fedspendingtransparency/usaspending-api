import pytest
from model_bakery import baker

from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/agency/{code}/awards/new/count/{filter}"


@pytest.fixture
def new_award_data(db):
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
def test_new_award_count_success(client, monkeypatch, new_award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.mock_current_fiscal_year(monkeypatch)

    resp = client.get(url.format(code="123", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["new_award_count"] == 3


@pytest.mark.django_db
def test_new_award_count_too_early(client, monkeypatch, new_award_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.get(url.format(code="123", filter="?fiscal_year=2007"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_new_award_count_future(client, monkeypatch, new_award_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.get(url.format(code="123", filter=f"?fiscal_year={current_fiscal_year() + 1}"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_new_award_count_specific_year(client, monkeypatch, new_award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.get(url.format(code="123", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["new_award_count"] == 3

    resp = client.get(url.format(code="123", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["new_award_count"] == 1


@pytest.mark.django_db
def test_new_award_count_specific_award_type(client, monkeypatch, new_award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.get(url.format(code="123", filter="?fiscal_year=2020&award_type_codes=[A,B]"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["new_award_count"] == 2

    resp = client.get(url.format(code="789", filter="?fiscal_year=2020&award_type_codes=[07,08]"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["new_award_count"] == 0

    resp = client.get(url.format(code="789", filter="?fiscal_year=2021&award_type_codes=[08]"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["new_award_count"] == 1


@pytest.mark.django_db
def test_new_award_count_invalid_award_type(client, monkeypatch, new_award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.mock_current_fiscal_year(monkeypatch)

    resp = client.get(url.format(code="123", filter=f"?award_type_codes=[99]"))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    resp = client.get(url.format(code="123", filter=f"?award_type_codes=[A,B,0]"))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_new_award_count_invalid_agency_type(client, monkeypatch, new_award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.mock_current_fiscal_year(monkeypatch)

    resp = client.get(url.format(code="123", filter=f"?agency_type=random"))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_new_award_count_specific_agency_type(client, monkeypatch, new_award_data, helpers, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.get(url.format(code="789", filter=f"?fiscal_year=2021&agency_type=awarding"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["agency_type"] == "awarding"
    assert resp.data["new_award_count"] == 1

    resp = client.get(url.format(code="789", filter=f"?fiscal_year=2021&agency_type=funding"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["agency_type"] == "funding"
    assert resp.data["new_award_count"] == 0

    resp = client.get(url.format(code="456", filter=f"?fiscal_year=2021&agency_type=funding"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["agency_type"] == "funding"
    assert resp.data["new_award_count"] == 2
