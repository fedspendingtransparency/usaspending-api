import pytest
from model_bakery import baker


@pytest.fixture
def spending_level_test_data():
    baker.make(
        "search.AwardSearch",
        award_id=1,
        date_signed="2019-01-01",
        action_date="2020-01-01",
        pop_country_code="USA",
        generated_pragmatic_obligation=500.0,
        total_outlays=600,
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        date_signed="2019-01-01",
        action_date="2020-01-01",
        pop_country_code="USA",
        generated_pragmatic_obligation=500.0,
        total_outlays=600,
    )
    baker.make(
        "search.AwardSearch",
        award_id=3,
        date_signed="2019-01-01",
        action_date="2020-01-01",
        pop_country_code="CAN",
        generated_pragmatic_obligation=300.0,
        total_outlays=500,
    )
    baker.make(
        "search.AwardSearch",
        award_id=4,
        date_signed="2019-01-01",
        action_date="2020-01-01",
        pop_country_code="CAN",
        generated_pragmatic_obligation=300.0,
        total_outlays=500,
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        action_date="2020-01-01",
        sub_action_date="2020-01-01",
        sub_place_of_perform_country_co="USA",
        subaward_amount=500.0,
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        action_date="2020-01-01",
        sub_action_date="2020-01-01",
        sub_place_of_perform_country_co="USA",
        subaward_amount=1000.0,
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        action_date="2020-01-01",
        sub_action_date="2020-01-01",
        sub_place_of_perform_country_co="CAN",
        subaward_amount=300.0,
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=4,
        action_date="2020-01-01",
        sub_action_date="2020-01-01",
        sub_place_of_perform_country_co="CAN",
        subaward_amount=100.0,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        action_date="2020-01-01",
        pop_country_code="USA",
        generated_pragmatic_obligation=250.0,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        action_date="2020-01-01",
        pop_country_code="USA",
        generated_pragmatic_obligation=250.0,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        action_date="2020-01-01",
        pop_country_code="CAN",
        generated_pragmatic_obligation=300.0,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        action_date="2020-01-01",
        pop_country_code="CAN",
        generated_pragmatic_obligation=200.0,
    )
    baker.make(
        "references.RefCountryCode",
        country_code="USA",
        country_name="Test United States",
        latest_population=500,
    )
    baker.make(
        "references.RefCountryCode",
        country_code="CAN",
        country_name="Test Canada",
        latest_population=200,
    )
