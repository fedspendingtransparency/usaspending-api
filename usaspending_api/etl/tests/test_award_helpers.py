import datetime
import pytest

from model_mommy import mommy

from usaspending_api.etl.award_helpers import update_awards, update_procurement_awards, update_assistance_awards


@pytest.mark.django_db
def test_award_update_from_latest_transaction():
    """Test awards fields that should be updated with most recent transaction info."""

    agency1 = mommy.make("references.Agency")
    agency2 = mommy.make("references.Agency")

    award = mommy.make(
        "awards.Award",
        awarding_agency=agency1,
        period_of_performance_current_end_date=datetime.date(2016, 1, 1),
        description="original award",
        generated_unique_award_id="AWD_1",
    )

    # adding transaction with same info should not change award values
    transaction = mommy.make(
        "awards.TransactionNormalized",
        award=award,
        awarding_agency=award.awarding_agency,
        period_of_performance_current_end_date=award.period_of_performance_current_end_date,
        description=award.description,
        action_date=datetime.date(2016, 2, 1),
        unique_award_key="AWD_1",
    )

    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency == agency1
    assert award.period_of_performance_current_end_date == datetime.date(2016, 1, 1)
    assert award.description == "original award"
    assert award.latest_transaction == transaction

    # adding an older transaction with different info updates award's total obligation amt and the description
    # (which is sourced from the earliest txn), but other info remains unchanged
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        awarding_agency=agency2,
        period_of_performance_current_end_date=datetime.date(2017, 1, 1),
        description="older description",
        action_date=datetime.date(2016, 1, 1),
        unique_award_key="AWD_1",
    )
    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency == agency1
    assert award.period_of_performance_current_end_date == datetime.date(2016, 1, 1)
    assert award.description == "older description"

    # adding an newer transaction with different info updates award's total obligation amt and also overrides
    # other values
    mommy.make(
        "awards.TransactionNormalized",
        id=999,
        award=award,
        awarding_agency=agency2,
        period_of_performance_current_end_date=datetime.date(2010, 1, 1),
        description="new description",
        action_date=datetime.date(2017, 1, 1),
        unique_award_key="AWD_1",
    )

    update_awards()
    award.refresh_from_db()

    assert award.awarding_agency == agency2
    assert award.period_of_performance_current_end_date == datetime.date(2010, 1, 1)
    # award desc should still reflect the earliest txn
    assert award.description == "older description"


@pytest.mark.django_db
def test_award_update_from_earliest_transaction():
    """Test awards fields that should be updated with most earliest transaction info."""

    award = mommy.make("awards.Award", generated_unique_award_id="AWD_ALPHA")
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        # since this is the award's first transaction,
        # the txn action_date will become the award
        # signed date
        action_date=datetime.date(2016, 1, 1),
        unique_award_key="AWD_ALPHA",
    )

    # adding later transaction should not change award values
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date=datetime.date(2017, 1, 1),
        unique_award_key="AWD_ALPHA",
    )

    update_awards()
    award.refresh_from_db()

    assert award.date_signed == datetime.date(2016, 1, 1)

    # adding earlier transaction should update award values
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date=datetime.date(2010, 1, 1),
        unique_award_key="AWD_ALPHA",
    )

    update_awards()
    award.refresh_from_db()

    assert award.date_signed == datetime.date(2010, 1, 1)


@pytest.mark.django_db
def test_award_update_obligated_amt():
    """Test that the award obligated amt updates as child transactions change."""

    award = mommy.make("awards.Award", total_obligation=1000, generated_unique_award_id="BIG_AGENCY_AWD_1")
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        federal_action_obligation=1000,
        _quantity=5,
        unique_award_key="BIG_AGENCY_AWD_1",
    )

    update_awards()
    award.refresh_from_db()

    assert award.total_obligation == 5000


@pytest.mark.django_db
def test_award_update_with_list():
    """Test optional parameter to update specific awards with txn data."""
    awards = [mommy.make("awards.Award", total_obligation=0, generated_unique_award_id=f"AWARD_{i}") for i in range(10)]
    test_award = awards[3]

    # test a single award update
    mommy.make(
        "awards.TransactionNormalized",
        award=test_award,
        federal_action_obligation=1000,
        _quantity=5,
        unique_award_key=test_award.generated_unique_award_id,
    )
    count = update_awards((test_award.id,))
    test_award.refresh_from_db()
    # one award is updated
    assert count == 1
    # specified award is updated
    assert test_award.total_obligation == 5000
    # other awards not updated
    assert awards[0].total_obligation == 0

    # test updating several awards
    mommy.make(
        "awards.TransactionNormalized",
        award=awards[0],
        federal_action_obligation=2000,
        _quantity=2,
        unique_award_key=awards[0].generated_unique_award_id,
    )
    mommy.make(
        "awards.TransactionNormalized",
        award=awards[1],
        federal_action_obligation=-1000,
        _quantity=3,
        unique_award_key=awards[1].generated_unique_award_id,
    )
    count = update_awards((awards[0].id, awards[1].id))
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
    award = mommy.make("awards.Award", generated_unique_award_id="EXAMPLE_AWARD_1")
    txn = mommy.make("awards.TransactionNormalized", award=award, unique_award_key="EXAMPLE_AWARD_1")
    txn2 = mommy.make("awards.TransactionNormalized", award=award, unique_award_key="EXAMPLE_AWARD_1")
    mommy.make(
        "awards.TransactionFPDS",
        transaction=txn,
        base_and_all_options_value=1000,
        base_exercised_options_val=100,
        unique_award_key="EXAMPLE_AWARD_1",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction=txn2,
        base_and_all_options_value=1001,
        base_exercised_options_val=101,
        unique_award_key="EXAMPLE_AWARD_1",
    )

    update_procurement_awards()
    award.refresh_from_db()

    assert award.base_and_all_options_value == 2001
    assert award.base_exercised_options_val == 201


@pytest.mark.django_db
def test_award_update_contract_txn_with_list():
    """Test optional parameter to update specific awards from txn contract."""
    awards = [mommy.make("awards.Award", total_obligation=0, generated_unique_award_id=f"AWARD_{i}") for i in range(5)]
    txn = mommy.make(
        "awards.TransactionNormalized", award=awards[0], unique_award_key=awards[0].generated_unique_award_id
    )
    mommy.make(
        "awards.TransactionFPDS", transaction=txn, base_and_all_options_value=1000, base_exercised_options_val=100
    )
    # single award is updated
    count = update_procurement_awards((awards[0].id,))
    awards[0].refresh_from_db()
    assert count == 1
    assert awards[0].base_and_all_options_value == 1000

    # update multipe awards
    txn1 = mommy.make(
        "awards.TransactionNormalized", award=awards[1], unique_award_key=awards[1].generated_unique_award_id
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction=txn1,
        base_and_all_options_value=4000,
        base_exercised_options_val=400,
        unique_award_key=awards[1].generated_unique_award_id,
    )
    txn2 = mommy.make(
        "awards.TransactionNormalized", award=awards[2], unique_award_key=awards[2].generated_unique_award_id
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction=txn2,
        base_and_all_options_value=5000,
        base_exercised_options_val=500,
        unique_award_key=awards[2].generated_unique_award_id,
    )
    # multiple awards updated
    count = update_procurement_awards((awards[1].id, awards[2].id))
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

    award = mommy.make("awards.Award", generated_unique_award_id="AWARD_CONT_IDV")
    txn = mommy.make(
        "awards.TransactionNormalized", award=award, action_date="2011-10-01", unique_award_key="AWARD_CONT_IDV"
    )
    txn2 = mommy.make(
        "awards.TransactionNormalized", award=award, action_date="2012-10-01", unique_award_key="AWARD_CONT_IDV"
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction=txn,
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
        unique_award_key="AWARD_CONT_IDV",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction=txn2,
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
        unique_award_key="AWARD_CONT_IDV",
    )

    update_procurement_awards()
    award.refresh_from_db()

    assert award.officer_1_name == "Jack Mustard"
    assert award.officer_5_amount == 500

    # Test that a newer transaction without Executive Comp data doesn't overwrite the award values

    txn3 = mommy.make(
        "awards.TransactionNormalized", award=award, action_date="2013-10-01", unique_award_key="AWARD_CONT_IDV"
    )
    mommy.make("awards.TransactionFPDS", transaction=txn3, unique_award_key="AWARD_CONT_IDV")

    update_procurement_awards()
    award.refresh_from_db()

    assert award.officer_1_name == "Jack Mustard"
    assert award.officer_5_amount == 500


@pytest.mark.django_db
def test_award_update_assistance_executive_comp():
    """Test executive comp is loaded correctly awards from txn contract."""

    award = mommy.make("awards.Award", generated_unique_award_id="ASST_ONE")
    txn = mommy.make("awards.TransactionNormalized", award=award, action_date="2011-10-01", unique_award_key="ASST_ONE")
    txn2 = mommy.make(
        "awards.TransactionNormalized", award=award, action_date="2012-10-01", unique_award_key="ASST_ONE"
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction=txn,
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
        unique_award_key="ASST_ONE",
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction=txn2,
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
        unique_award_key="ASST_ONE",
    )

    update_assistance_awards()
    award.refresh_from_db()

    assert award.officer_1_name == "Jack Mustard"
    assert award.officer_5_amount == 500

    # Test that a newer transaction without Executive Comp data doesn't overwrite the award values

    txn3 = mommy.make(
        "awards.TransactionNormalized", award=award, action_date="2013-10-01", unique_award_key="ASST_ONE"
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction=txn3,
        unique_award_key="ASST_ONE",
    )

    update_assistance_awards()
    award.refresh_from_db()

    assert award.officer_1_name == "Jack Mustard"
    assert award.officer_5_amount == 500


@pytest.mark.django_db
def test_award_update_transaction_fk():
    """Test executive comp is loaded correctly awards from txn contract."""

    award = mommy.make("awards.Award", generated_unique_award_id="FAKE_award_YELLOW_12")
    txn1 = mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date="2011-10-01",
        description="Original Desc",
        modification_number="P0001",
        unique_award_key="FAKE_award_YELLOW_12",
    )
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date="2012-10-01",
        unique_award_key="FAKE_award_YELLOW_12",
    )
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date="2013-10-01",
        unique_award_key="FAKE_award_YELLOW_12",
    )
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date="2014-10-01",
        unique_award_key="FAKE_award_YELLOW_12",
    )
    mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date="2015-10-01",
        unique_award_key="FAKE_award_YELLOW_12",
    )
    txn6 = mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date="2016-10-01",
        description="Last Desc",
        modification_number="P0011",
        period_of_performance_current_end_date="2020-10-01",
        unique_award_key="FAKE_award_YELLOW_12",
    )

    update_awards()
    award.refresh_from_db()

    assert award.description == txn1.description
    assert award.earliest_transaction == txn1
    assert award.latest_transaction == txn6
    assert award.date_signed.strftime("%Y-%m-%d") == txn1.action_date
    assert award.certified_date.strftime("%Y-%m-%d") == txn6.action_date
    assert (
        award.period_of_performance_current_end_date.strftime("%Y-%m-%d") == txn6.period_of_performance_current_end_date
    )

    txn0 = mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date=txn1.action_date,
        description="Updated Original Desc",
        modification_number="P0000",
        unique_award_key="FAKE_award_YELLOW_12",
    )

    txn10 = mommy.make(
        "awards.TransactionNormalized",
        award=award,
        action_date=txn6.action_date,
        modification_number="P1000",
        period_of_performance_current_end_date="2019-10-01",
        unique_award_key="FAKE_award_YELLOW_12",
    )

    update_awards()
    award.refresh_from_db()

    assert award.description == txn0.description
    assert award.earliest_transaction == txn0
    assert award.latest_transaction == txn10
    assert award.date_signed.strftime("%Y-%m-%d") == txn1.action_date
    assert award.certified_date.strftime("%Y-%m-%d") == txn6.action_date
    assert (
        award.period_of_performance_current_end_date.strftime("%Y-%m-%d")
        == txn10.period_of_performance_current_end_date
    )
