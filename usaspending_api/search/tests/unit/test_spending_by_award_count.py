import json
import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def award_data_fixture(db):
    mommy.make("awards.TransactionNormalized", id=210210210, action_date="2013-09-17")
    mommy.make("awards.TransactionNormalized", id=321032103, action_date="2013-09-17")
    mommy.make("awards.TransactionNormalized", id=432104321, action_date="2013-09-17")
    mommy.make("awards.TransactionNormalized", id=543210543, action_date="2013-09-17")
    mommy.make("awards.TransactionNormalized", id=654321065, action_date="2013-09-17")
    mommy.make("awards.TransactionNormalized", id=765432107, action_date="2013-09-17")
    mommy.make("awards.TransactionNormalized", id=876543210, action_date="2013-09-17")
    mommy.make("awards.TransactionNormalized", id=987654321, action_date="2013-09-17")
    mommy.make("references.LegalEntity", legal_entity_id=2022)
    mommy.make("references.LegalEntity", legal_entity_id=3022)
    mommy.make("references.LegalEntity", legal_entity_id=4022)
    mommy.make("references.LegalEntity", legal_entity_id=5022)
    mommy.make("references.LegalEntity", legal_entity_id=6022)
    mommy.make("references.LegalEntity", legal_entity_id=7022)
    mommy.make("references.LegalEntity", legal_entity_id=8022)
    mommy.make("references.LegalEntity", legal_entity_id=9022)
    mommy.make(
        "awards.Award",
        category="loans",
        date_signed="2012-09-10",
        fain="DECF0000058",
        generated_unique_award_id="ASST_NON_DECF0000058_8900",
        id=200,
        latest_transaction_id=210210210,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2012-09-10",
        piid=None,
        recipient_id=2022,
        type="07",
        uri=None,
    )
    mommy.make(
        "awards.Award",
        category="idvs",
        date_signed="2009-12-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY2_8900",
        id=300,
        latest_transaction_id=321032103,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2014-09-10",
        piid="YUGGY2",
        recipient_id=3022,
        type="IDV_B_A",
        uri=None,
    )
    mommy.make(
        "awards.Award",
        category="idvs",
        date_signed="2015-05-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY3_8900",
        id=400,
        latest_transaction_id=432104321,
        period_of_performance_current_end_date="2018-09-09",
        period_of_performance_start_date="2018-09-01",
        piid="YUGGY3",
        recipient_id=4022,
        type="IDV_B",
        uri=None,
    )
    mommy.make(
        "awards.Award",
        category="idvs",
        date_signed="2009-09-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY_8900",
        id=500,
        latest_transaction_id=543210543,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2018-09-10",
        piid="YUGGY",
        recipient_id=5022,
        type="IDV_B_C",
        uri=None,
    )
    mommy.make(
        "awards.Award",
        category="idvs",
        date_signed="2009-09-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY55_8900",
        id=600,
        latest_transaction_id=654321065,
        period_of_performance_current_end_date="2039-09-09",
        period_of_performance_start_date="2009-09-10",
        piid="YUGGY55",
        recipient_id=6022,
        type="IDV_C",
        uri=None,
    )
    mommy.make(
        "awards.Award",
        category="idvs",
        date_signed="2009-12-20",
        fain=None,
        generated_unique_award_id="CONT_AW_BEANS_8900",
        id=700,
        latest_transaction_id=765432107,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2009-12-20",
        piid="BEANS",
        recipient_id=7022,
        type="A",
        uri=None,
    )
    mommy.make(
        "awards.Award",
        category="idvs",
        date_signed="2011-09-10",
        fain=None,
        generated_unique_award_id="CONT_AW_BEANS55_8900",
        id=800,
        latest_transaction_id=876543210,
        period_of_performance_current_end_date="2020-12-09",
        period_of_performance_start_date="2011-09-10",
        piid="BEANS55",
        recipient_id=8022,
        type="C",
        uri=None,
    )
    mommy.make(
        "awards.Award",
        category="other",
        date_signed="2013-09-10",
        fain=None,
        generated_unique_award_id="ASST_AGG_JHISUONSD_8900",
        id=900,
        latest_transaction_id=987654321,
        period_of_performance_current_end_date="2018-09-09",
        period_of_performance_start_date="2013-09-10",
        piid=None,
        recipient_id=9022,
        type="11",
        uri="JHISUONSD",
    )


def get_spending_by_award_count_url():
    return "/api/v2/search/spending_by_award_count/"


@pytest.mark.django_db
def test_spending_by_award_count(client, db, award_data_fixture, refresh_matviews):
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
        "results": {"contracts": 2, "idvs": 4, "loans": 1, "direct_payments": 0, "grants": 0, "other": 1}
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_count_idvs(client, db, award_data_fixture, refresh_matviews):
    test_payload = {
        "subawards": False,
        "filters": {
            "award_type_codes": ["IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C"],
            "time_period": [{"start_date": "2009-10-01", "end_date": "2018-09-30"}],
        },
    }

    expected_response = {
        "results": {"contracts": 0, "idvs": 3, "loans": 0, "direct_payments": 0, "grants": 0, "other": 0}
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"
