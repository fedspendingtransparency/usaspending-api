import pytest
from model_mommy import mommy
from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount
from usaspending_api.awards.models import TransactionNormalized, Award
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
import datetime


@pytest.fixture
def trans_exp_data(db):
    mommy.make(
        "awards.TransactionNormalized",
        usaspending_unique_transaction_id="123",
        federal_action_obligation=0,
        action_date=datetime.date(2018, 1, 1),
        original_loan_subsidy_cost=100,
        type="07",
    )

    mommy.make(
        "awards.TransactionNormalized",
        usaspending_unique_transaction_id="1234",
        federal_action_obligation=200,
        action_date=datetime.date(2018, 1, 1),
        type="A",
    )

    qs = TransactionNormalized.objects.all()
    qs = qs.values("action_date")

    return qs


@pytest.fixture
def award_exp_data(db):
    mommy.make(
        "awards.Award", generated_unique_award_id="123", total_obligation=0, total_subsidy_cost=1000.00, type="07"
    )

    mommy.make("awards.Award", generated_unique_award_id="1234", total_obligation=1200.00, type="B")

    qs = Award.objects.all()
    qs = qs.values("generated_unique_award_id")
    return qs


@pytest.mark.django_db
def test_sum_query_all_types_trans(trans_exp_data):
    """Tests aggregation on transactions where there is no award type filter"""
    result = sum_transaction_amount(trans_exp_data, filter_types=award_type_mapping)

    assert result[0]["transaction_amount"] == 300


@pytest.mark.django_db
def test_sum_query_filter_loans_trans(trans_exp_data):
    """Tests aggregation on transactions when there is a loan award type filter"""
    result = sum_transaction_amount(trans_exp_data, filter_types=["07", "08"])

    assert result[0]["transaction_amount"] == 100


@pytest.mark.django_db
def test_sum_query_filter_other_trans(trans_exp_data):
    """Tests aggregation on transactions when there is a non-loan award type filter"""
    result = sum_transaction_amount(trans_exp_data, filter_types=["A", "B", "C"])

    assert result[0]["transaction_amount"] == 200


@pytest.mark.django_db
def test_sum_query_all_types_awards(award_exp_data):
    """Tests on award results when there is a no award type filter"""
    result = sum_transaction_amount(award_exp_data, filter_types=award_type_mapping, calculate_totals=False)

    assert result.filter(type="07")[0]["transaction_amount"] == 1000.00
    assert result.filter(type="B")[0]["transaction_amount"] == 1200.00


@pytest.mark.django_db
def test_sum_query_filter_loans_awards(award_exp_data):
    """Tests on award results when there is a loan award type filter"""
    result = sum_transaction_amount(award_exp_data, filter_types=["07", "08"], calculate_totals=False)
    # Contract award's total should equal zero since total original subsity is zero
    assert result.filter(type="B")[0]["transaction_amount"] == 0
    assert result.filter(type="07")[0]["transaction_amount"] == 1000.00


@pytest.mark.django_db
def test_sum_query_filter_other_awards(award_exp_data):

    result = sum_transaction_amount(award_exp_data, filter_types=["A", "B", "C"], calculate_totals=False)
    # Contract award's total should equal zero since total obligation is zero
    assert result.filter(type="07")[0]["transaction_amount"] == 0
    assert result.filter(type="B")[0]["transaction_amount"] == 1200.00
