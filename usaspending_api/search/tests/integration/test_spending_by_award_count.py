import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def award_data_fixture(db):
    baker.make("search.TransactionSearch", transaction_id=210210210, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=321032103, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=432104321, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=543210543, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=654321065, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=765432107, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=876543210, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=987654321, action_date="2013-09-17")
    award1 = baker.make(
        "search.AwardSearch",
        category="loans",
        date_signed="2012-09-10",
        action_date="2012-09-12",
        fain="DECF0000058",
        generated_unique_award_id="ASST_NON_DECF0000058_8900",
        award_id=200,
        latest_transaction_id=210210210,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2012-09-10",
        piid=None,
        type="07",
        uri=None,
        program_activities=[{"code": "0123", "name": "PROGRAM_ACTIVITY_123"}],
    )
    award2 = baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2009-12-10",
        action_date="2009-12-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY2_8900",
        award_id=300,
        latest_transaction_id=321032103,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2014-09-10",
        piid="YUGGY2",
        type="IDV_B_A",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2015-05-10",
        action_date="2015-05-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY3_8900",
        award_id=400,
        latest_transaction_id=432104321,
        period_of_performance_current_end_date="2018-09-09",
        period_of_performance_start_date="2018-09-01",
        piid="YUGGY3",
        type="IDV_B",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2009-09-10",
        action_date="2009-09-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY_8900",
        award_id=500,
        latest_transaction_id=543210543,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2018-09-10",
        piid="YUGGY",
        type="IDV_B_C",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2009-09-10",
        action_date="2009-09-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY55_8900",
        award_id=600,
        latest_transaction_id=654321065,
        period_of_performance_current_end_date="2039-09-09",
        period_of_performance_start_date="2009-09-10",
        piid="YUGGY55",
        type="IDV_C",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2009-12-20",
        action_date="2009-12-20",
        fain=None,
        generated_unique_award_id="CONT_IDV_BEANS_8900",
        award_id=700,
        latest_transaction_id=765432107,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2009-12-20",
        piid="BEANS",
        type="IDV_C",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2011-09-10",
        action_date="2011-09-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_BEANS55_8900",
        award_id=800,
        latest_transaction_id=876543210,
        period_of_performance_current_end_date="2020-12-09",
        period_of_performance_start_date="2011-09-10",
        piid="BEANS55",
        type="IDV_B_A",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="contracts",
        date_signed="2011-09-10",
        action_date="2011-09-10",
        fain=None,
        generated_unique_award_id="CONT_AW_BEANS55_8900",
        award_id=1000,
        latest_transaction_id=876543210,
        period_of_performance_current_end_date="2020-12-09",
        period_of_performance_start_date="2011-09-10",
        piid="BEANS55",
        type="C",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="other",
        date_signed="2013-09-10",
        action_date="2013-09-10",
        fain=None,
        generated_unique_award_id="ASST_AGG_JHISUONSD_8900",
        award_id=900,
        latest_transaction_id=987654321,
        period_of_performance_current_end_date="2018-09-09",
        period_of_performance_start_date="2013-09-10",
        piid=None,
        type="11",
        uri="JHISUONSD",
    )
    baker.make(
        "search.AwardSearch",
        category="contracts",
        date_signed="2009-12-20",
        action_date="2009-12-20",
        fain=None,
        generated_unique_award_id="CONT_AW_BEANS_8900",
        award_id=1100,
        latest_transaction_id=765432107,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2009-12-20",
        piid="BEANS",
        type="A",
        uri=None,
    )
    baker.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=1, award_id=award1.award_id)

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award=award1,
        sub_action_date="2023-01-01",
        action_date="2023-01-01",
        prime_award_group="grant",
        prime_award_type="07",
        program_activities=[{"name": "PROGRAM_ACTIVITY_123", "code": "0123"}],
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        award=award1,
        sub_action_date="2023-01-01",
        action_date="2023-01-01",
        prime_award_group="procurement",
        prime_award_type="07",
        program_activities=[{"name": "PROGRAM_ACTIVITY_123", "code": "0123"}],
    )
    ref_program_activity1 = baker.make(
        "references.RefProgramActivity",
        id=1,
        program_activity_code=123,
        program_activity_name="PROGRAM_ACTIVITY_123",
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        financial_accounts_by_awards_id=1,
        award_id=award1.award_id,
        program_activity_id=ref_program_activity1.id,
    )

    ref_program_activity2 = baker.make(
        "references.RefProgramActivity",
        id=2,
        program_activity_code=2,
        program_activity_name="PROGRAM_ACTIVITY_2",
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        financial_accounts_by_awards_id=2,
        award_id=award2.award_id,
        program_activity_id=ref_program_activity2.id,
    )


def get_spending_by_award_count_url():
    return "/api/v2/search/spending_by_award_count/"


@pytest.mark.django_db
def test_spending_by_award_count(client, monkeypatch, elasticsearch_award_index, award_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_payload = {
        "subawards": False,
        "filters": {
            "time_period": [
                {"start_date": "2009-10-01", "end_date": "2017-09-30"},
                {"start_date": "2017-10-01", "end_date": "2018-09-30"},
            ]
        },
    }

    expected_response = {
        "results": {"contracts": 2, "idvs": 4, "loans": 1, "direct_payments": 0, "grants": 0, "other": 1},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_count_idvs(client, monkeypatch, elasticsearch_award_index, award_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_payload = {
        "subawards": False,
        "filters": {
            "award_type_codes": ["IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C"],
            "time_period": [{"start_date": "2009-10-01", "end_date": "2018-09-30"}],
        },
    }

    expected_response = {
        "results": {"contracts": 0, "idvs": 3, "loans": 0, "direct_payments": 0, "grants": 0, "other": 0},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_count_new_awards_only(client, monkeypatch, elasticsearch_award_index, award_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Tests new awards only where some awards are within the time period bounds
    test_payload = {
        "subawards": False,
        "filters": {
            "time_period": [
                {"date_type": "new_awards_only", "start_date": "2012-09-09", "end_date": "2012-09-11"},
            ]
        },
    }

    expected_response = {
        "results": {"contracts": 0, "direct_payments": 0, "grants": 0, "idvs": 0, "loans": 1, "other": 0},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # Tests new awards only where no awards are within the time period bounds
    test_payload = {
        "subawards": False,
        "filters": {
            "time_period": [
                {"date_type": "new_awards_only", "start_date": "2012-09-09", "end_date": "2012-09-09"},
            ]
        },
    }

    expected_response = {
        "results": {"contracts": 0, "direct_payments": 0, "grants": 0, "idvs": 0, "loans": 0, "other": 0},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_count_program_activity_subawards(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, award_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # Program Activites filter test
    test_payload = {
        "subawards": True,
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
        },
    }

    expected_response = {
        "results": {"subcontracts": 1, "subgrants": 1},
        "spending_level": "subawards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    test_payload = {
        "subawards": True,
        "filters": {
            "program_activities": [{"name": "program_activity_123"}, {"code": "123"}],
        },
    }

    expected_response = {
        "results": {"subcontracts": 1, "subgrants": 1},
        "spending_level": "subawards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    test_payload = {
        "subawards": True,
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
        },
    }

    expected_response = {
        "results": {"subcontracts": 0, "subgrants": 0},
        "spending_level": "subawards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_count_program_activity(client, monkeypatch, elasticsearch_award_index, award_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Program Activites filter test
    test_payload = {
        "subawards": False,
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
        },
    }

    expected_response = {
        "results": {"contracts": 0, "direct_payments": 0, "grants": 0, "idvs": 0, "loans": 1, "other": 0},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    test_payload = {
        "subawards": False,
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
        },
    }

    expected_response = {
        "results": {"contracts": 0, "direct_payments": 0, "grants": 0, "idvs": 0, "loans": 0, "other": 0},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    test_payload = {
        "subawards": False,
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "123"}],
        },
    }

    expected_response = {
        "results": {"contracts": 0, "direct_payments": 0, "grants": 0, "idvs": 0, "loans": 1, "other": 0},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    test_payload = {
        "subawards": False,
        "filters": {
            "program_activities": [{"name": "program_activity_123"}, {"code": "123"}],
        },
    }

    expected_response = {
        "results": {"contracts": 0, "direct_payments": 0, "grants": 0, "idvs": 0, "loans": 1, "other": 0},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_level_filter(client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    request = {
        "subawards": False,
        "spending_level": "awards",
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )

    expected_response = {
        "results": {"contracts": 0, "direct_payments": 0, "grants": 0, "idvs": 0, "loans": 0, "other": 0},
        "spending_level": "awards",
        "messages": [get_time_period_message()],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data

    request = {
        "spending_level": "subawards",
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )

    expected_response = {
        "results": {"subgrants": 0, "subcontracts": 0},
        "spending_level": "subawards",
        "messages": [get_time_period_message()],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data

    # Checks that subawards = true takes precedent over spending_level = awards
    request = {
        "subawards": True,
        "spending_level": "awards",
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )

    expected_response = {
        "results": {"subgrants": 0, "subcontracts": 0},
        "spending_level": "subawards",
        "messages": [get_time_period_message()],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data
