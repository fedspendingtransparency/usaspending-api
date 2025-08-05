import datetime
import pytest

from model_bakery import baker

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.etl.award_helpers import update_awards, update_procurement_awards, update_assistance_awards


@pytest.mark.django_db
def test_award_update_from_latest_transaction():
    """Test awards fields that should be updated with most recent transaction info."""

    agency1 = baker.make("references.Agency", _fill_optional=True)
    agency2 = baker.make("references.Agency", _fill_optional=True)

    award = baker.make(
        "search.AwardSearch",
        award_id=1,
        awarding_agency_id=agency1.id,
        period_of_performance_current_end_date=datetime.date(2016, 1, 1),
        description="original award",
        generated_unique_award_id="AWD_1",
    )

    # adding transaction with same info should not change award values
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award=award,
        awarding_agency_id=award.awarding_agency_id,
        period_of_performance_current_end_date=award.period_of_performance_current_end_date,
        transaction_description=award.description,
        action_date=datetime.date(2016, 2, 1),
        generated_unique_award_id="AWD_1",
    )
    transaction = TransactionNormalized.objects.filter(id=1).first()

    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency_id == agency1.id
    assert award.period_of_performance_current_end_date == datetime.date(2016, 1, 1)
    assert award.description == "original award"
    assert award.latest_transaction == transaction

    # adding an older transaction with different info updates award's total obligation amt and the description
    # (which is sourced from the earliest txn), but other info remains unchanged
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award=award,
        awarding_agency_id=agency2.id,
        period_of_performance_current_end_date=datetime.date(2017, 1, 1),
        transaction_description="older description",
        action_date=datetime.date(2016, 1, 1),
        generated_unique_award_id="AWD_1",
    )
    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency_id == agency1.id
    assert award.period_of_performance_current_end_date == datetime.date(2016, 1, 1)
    assert award.description == "older description"

    # adding an newer transaction with different info updates award's total obligation amt and also overrides
    # other values
    baker.make(
        "search.TransactionSearch",
        transaction_id=999,
        award=award,
        awarding_agency_id=agency2.id,
        period_of_performance_current_end_date=datetime.date(2010, 1, 1),
        transaction_description="new description",
        action_date=datetime.date(2017, 1, 1),
        generated_unique_award_id="AWD_1",
    )

    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency_id == agency2.id
    assert award.period_of_performance_current_end_date == datetime.date(2010, 1, 1)
    # award desc should still reflect the earliest txn
    assert award.description == "older description"


@pytest.mark.django_db
def test_award_update_from_earliest_transaction():
    """Test awards fields that should be updated with most earliest transaction info."""

    award = baker.make("search.AwardSearch", award_id=1, generated_unique_award_id="AWD_ALPHA")
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award=award,
        # since this is the award's first transaction,
        # the txn action_date will become the award
        # signed date
        action_date=datetime.date(2016, 1, 1),
        generated_unique_award_id="AWD_ALPHA",
    )

    # adding later transaction should not change award values
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award=award,
        action_date=datetime.date(2017, 1, 1),
        generated_unique_award_id="AWD_ALPHA",
    )

    update_awards()
    award.refresh_from_db()

    assert award.date_signed == datetime.date(2016, 1, 1)

    # adding earlier transaction should update award values
    baker.make(
        "search.TransactionSearch",
        transaction_id=5,
        award=award,
        action_date=datetime.date(2010, 1, 1),
        generated_unique_award_id="AWD_ALPHA",
    )

    update_awards()
    award.refresh_from_db()

    assert award.date_signed == datetime.date(2010, 1, 1)


@pytest.mark.django_db
def test_award_update_obligated_amt():
    """Test that the award obligated amt updates as child transactions change."""

    award = baker.make(
        "search.AwardSearch", award_id=1, total_obligation=1000, generated_unique_award_id="BIG_AGENCY_AWD_1"
    )
    for i in range(5):
        baker.make(
            "search.TransactionSearch",
            transaction_id=i + 1,
            award=award,
            federal_action_obligation=1000,
            generated_unique_award_id="BIG_AGENCY_AWD_1",
        )

    update_awards()
    award.refresh_from_db()

    assert award.total_obligation == 5000


@pytest.mark.django_db
def test_award_update_with_list():
    """Test optional parameter to update specific awards with txn data."""
    awards = [
        baker.make("search.AwardSearch", award_id=i, total_obligation=0, generated_unique_award_id=f"AWARD_{i}")
        for i in range(10)
    ]
    test_award = awards[3]

    # test a single award update
    for i in range(5):
        baker.make(
            "search.TransactionSearch",
            transaction_id=i + 1,
            award=test_award,
            federal_action_obligation=1000,
            generated_unique_award_id=test_award.generated_unique_award_id,
        )
    count = update_awards((test_award.award_id,))
    test_award.refresh_from_db()
    # one award is updated
    assert count == 1
    # specified award is updated
    assert test_award.total_obligation == 5000
    # other awards not updated
    assert awards[0].total_obligation == 0

    # test updating several awards
    for i in range(2):
        baker.make(
            "search.TransactionSearch",
            transaction_id=i + 1,
            award=awards[0],
            federal_action_obligation=2000,
            generated_unique_award_id=awards[0].generated_unique_award_id,
        )
    for i in range(3):
        baker.make(
            "search.TransactionSearch",
            transaction_id=i + 3,
            award=awards[1],
            federal_action_obligation=-1000,
            generated_unique_award_id=awards[1].generated_unique_award_id,
        )
    count = update_awards((awards[0].award_id, awards[1].award_id))
    awards[0].refresh_from_db()
    awards[1].refresh_from_db()
    # two awards are updated
    assert count == 2
    # specified awards are updated
    assert awards[0].total_obligation == 4000
    assert awards[1].total_obligation == -3000
    # other awards not updated
    assert awards[4].total_obligation == 0


@pytest.mark.django_db
def test_award_update_from_contract_transaction():
    """Test award updates specific to contract transactions."""

    # for contract type transactions, the base_and_all_options_value and base_exercised_options_val fields
    # should update the corresponding field on the award table
    award = baker.make("search.AwardSearch", award_id=1, generated_unique_award_id="EXAMPLE_AWARD_1")
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        is_fpds=True,
        award=award,
        base_and_all_options_value=1000,
        base_exercised_options_val=100,
        generated_unique_award_id="EXAMPLE_AWARD_1",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        is_fpds=True,
        award=award,
        base_and_all_options_value=1001,
        base_exercised_options_val=101,
        generated_unique_award_id="EXAMPLE_AWARD_1",
    )

    update_procurement_awards()
    award.refresh_from_db()

    assert award.base_and_all_options_value == 2001
    assert award.base_exercised_options_val == 201


@pytest.mark.django_db
def test_award_update_contract_txn_with_list():
    """Test optional parameter to update specific awards from txn contract."""
    awards = [
        baker.make("search.AwardSearch", award_id=i, total_obligation=0, generated_unique_award_id=f"AWARD_{i}")
        for i in range(5)
    ]
    baker.make(
        "search.TransactionSearch",
        transaction_id=10,
        award=awards[0],
        is_fpds=True,
        base_and_all_options_value=1000,
        base_exercised_options_val=100,
        generated_unique_award_id=awards[0].generated_unique_award_id,
    )
    # single award is updated
    count = update_procurement_awards((awards[0].award_id,))
    awards[0].refresh_from_db()
    assert count == 1
    assert awards[0].base_and_all_options_value == 1000

    # update multiple awards
    baker.make(
        "search.TransactionSearch",
        transaction_id=11,
        is_fpds=True,
        award=awards[1],
        base_and_all_options_value=4000,
        base_exercised_options_val=400,
        generated_unique_award_id=awards[1].generated_unique_award_id,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=12,
        is_fpds=True,
        award=awards[2],
        base_and_all_options_value=5000,
        base_exercised_options_val=500,
        generated_unique_award_id=awards[2].generated_unique_award_id,
    )
    # multiple awards updated
    count = update_procurement_awards((awards[1].award_id, awards[2].award_id))
    awards[1].refresh_from_db()
    awards[2].refresh_from_db()
    assert count == 2
    assert awards[1].base_and_all_options_value == 4000
    assert awards[1].base_exercised_options_val == 400
    assert awards[2].base_and_all_options_value == 5000
    assert awards[2].base_exercised_options_val == 500


@pytest.mark.django_db
def test_award_update_contract_executive_comp():
    """Test executive comp is loaded correctly awards from txn contract."""

    award = baker.make("search.AwardSearch", award_id=1, generated_unique_award_id="AWARD_CONT_IDV")
    baker.make(
        "search.TransactionSearch",
        transaction_id=13,
        is_fpds=True,
        award=award,
        action_date="2011-10-01",
        officer_1_name="Professor Plum",
        officer_1_amount=1,
        officer_2_name="Mrs. White",
        officer_2_amount=2,
        officer_3_name="Mrs. Peacock",
        officer_3_amount=3,
        officer_4_name="Mr. Green",
        officer_4_amount=4,
        officer_5_name="Colonel Mustard",
        officer_5_amount=5,
        generated_unique_award_id="AWARD_CONT_IDV",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=14,
        is_fpds=True,
        award=award,
        action_date="2012-10-01",
        officer_1_name="Jack Mustard",
        officer_1_amount=100,
        officer_2_name="Jacob Green",
        officer_2_amount=200,
        officer_3_name="Diane White",
        officer_3_amount=300,
        officer_4_name="Kasandra Scarlet",
        officer_4_amount=400,
        officer_5_name="Victor Plum",
        officer_5_amount=500,
        generated_unique_award_id="AWARD_CONT_IDV",
    )

    update_procurement_awards()
    award.refresh_from_db()

    assert award.officer_1_name == "Jack Mustard"
    assert award.officer_5_amount == 500

    # Test that a newer transaction without Executive Comp data doesn't overwrite the award values

    baker.make(
        "search.TransactionSearch",
        transaction_id=26,
        award=award,
        action_date="2013-10-01",
        generated_unique_award_id="AWARD_CONT_IDV",
    )

    update_procurement_awards()
    award.refresh_from_db()

    assert award.officer_1_name == "Jack Mustard"
    assert award.officer_5_amount == 500


@pytest.mark.django_db
def test_award_update_assistance_executive_comp():
    """Test executive comp is loaded correctly awards from txn contract."""

    award = baker.make("search.AwardSearch", award_id=1, generated_unique_award_id="ASST_ONE")
    baker.make(
        "search.TransactionSearch",
        transaction_id=15,
        is_fpds=False,
        award=award,
        action_date="2011-10-01",
        officer_1_name="Professor Plum",
        officer_1_amount=1,
        officer_2_name="Mrs. White",
        officer_2_amount=2,
        officer_3_name="Mrs. Peacock",
        officer_3_amount=3,
        officer_4_name="Mr. Green",
        officer_4_amount=4,
        officer_5_name="Colonel Mustard",
        officer_5_amount=5,
        generated_unique_award_id="ASST_ONE",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=16,
        is_fpds=False,
        award=award,
        action_date="2012-10-01",
        officer_1_name="Jack Mustard",
        officer_1_amount=100,
        officer_2_name="Jacob Green",
        officer_2_amount=200,
        officer_3_name="Diane White",
        officer_3_amount=300,
        officer_4_name="Kasandra Scarlet",
        officer_4_amount=400,
        officer_5_name="Victor Plum",
        officer_5_amount=500,
        generated_unique_award_id="ASST_ONE",
    )

    update_assistance_awards()
    award.refresh_from_db()

    assert award.officer_1_name == "Jack Mustard"
    assert award.officer_5_amount == 500

    # Test that a newer transaction without Executive Comp data doesn't overwrite the award values

    baker.make(
        "search.TransactionSearch",
        transaction_id=17,
        is_fpds=False,
        award=award,
        action_date="2013-10-01",
        generated_unique_award_id="ASST_ONE",
    )

    update_assistance_awards()
    award.refresh_from_db()

    assert award.officer_1_name == "Jack Mustard"
    assert award.officer_5_amount == 500


@pytest.mark.django_db
def test_award_update_transaction_fk():
    """Test executive comp is loaded correctly awards from txn contract."""

    award = baker.make("search.AwardSearch", award_id=1, generated_unique_award_id="FAKE_award_YELLOW_12")
    baker.make(
        "search.TransactionSearch",
        transaction_id=18,
        award=award,
        action_date="2011-10-01",
        transaction_description="Original Desc",
        modification_number="P0001",
        generated_unique_award_id="FAKE_award_YELLOW_12",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=19,
        award=award,
        action_date="2012-10-01",
        generated_unique_award_id="FAKE_award_YELLOW_12",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=20,
        award=award,
        action_date="2013-10-01",
        generated_unique_award_id="FAKE_award_YELLOW_12",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=21,
        award=award,
        action_date="2014-10-01",
        generated_unique_award_id="FAKE_award_YELLOW_12",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=22,
        award=award,
        action_date="2015-10-01",
        generated_unique_award_id="FAKE_award_YELLOW_12",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=23,
        award=award,
        action_date="2016-10-01",
        transaction_description="Last Desc",
        modification_number="P0011",
        period_of_performance_current_end_date="2020-10-01",
        generated_unique_award_id="FAKE_award_YELLOW_12",
    )
    txn1 = TransactionNormalized.objects.filter(id=18).first()
    txn6 = TransactionNormalized.objects.filter(id=23).first()

    update_awards()
    award.refresh_from_db()

    assert award.description == txn1.description
    assert award.earliest_transaction == txn1
    assert award.latest_transaction == txn6
    assert award.date_signed == txn1.action_date
    assert award.certified_date == txn6.action_date
    assert award.period_of_performance_current_end_date == txn6.period_of_performance_current_end_date

    baker.make(
        "search.TransactionSearch",
        transaction_id=24,
        award=award,
        action_date=txn1.action_date,
        transaction_description="Updated Original Desc",
        modification_number="P0000",
        generated_unique_award_id="FAKE_award_YELLOW_12",
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=25,
        award=award,
        action_date=txn6.action_date,
        modification_number="P1000",
        period_of_performance_current_end_date="2019-10-01",
        generated_unique_award_id="FAKE_award_YELLOW_12",
    )
    txn0 = TransactionNormalized.objects.filter(id=24).first()
    txn10 = TransactionNormalized.objects.filter(id=25).first()

    update_awards()
    award.refresh_from_db()

    assert award.description == txn0.description
    assert award.earliest_transaction == txn0
    assert award.latest_transaction == txn10
    assert award.date_signed == txn1.action_date
    assert award.certified_date == txn6.action_date
    assert award.period_of_performance_current_end_date == txn10.period_of_performance_current_end_date
