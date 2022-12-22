import pytest
import json
import datetime

from model_bakery import baker
from rest_framework import status


@pytest.fixture
def cfda_transactions(db):
    award_1 = {"award_id": 1, "category": "grant", "generated_unique_award_id": "whatever"}
    trx_norm_1 = {
        "transaction_id": 1,
        "award_id": 1,
        "is_fpds": False,
        "transaction_unique_id": "Q25B9A1MQ0",
        "type": "02",
        "type_description": "Green",
        "action_date": datetime.date(2008, 11, 22),
        "action_type": None,
        "action_type_description": "grant",
        "modification_number": "0",
        "transaction_description": "ligula in lacus curabitur at ipsum ac tellus semper interdum mauris",
        "federal_action_obligation": "10.00",
        "face_value_loan_guarantee": "10.00",
        "original_loan_subsidy_cost": "10.00",
        "cfda_number": 12.345,
        "cfda_title": "Shiloh",
        "afa_generated_unique": "Q25B9A1MQ0",
    }

    baker.make("search.AwardSearch", **award_1)
    baker.make("search.TransactionSearch", **trx_norm_1)


def test_transaction_cfda(client, cfda_transactions):
    expected_response = {
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "id": "ASST_TX_Q25B9A1MQ0",
                "type": "02",
                "type_description": "Green",
                "action_date": "2008-11-22",
                "action_type": None,
                "action_type_description": "grant",
                "modification_number": "0",
                "description": "ligula in lacus curabitur at ipsum ac tellus semper interdum mauris",
                "federal_action_obligation": 10.00,
                "face_value_loan_guarantee": 10.00,
                "original_loan_subsidy_cost": 10.00,
                "cfda_number": "12.345",
            }
        ],
    }

    resp = client.post(
        "/api/v2/transactions/",
        content_type="application/json",
        data=json.dumps({"award_id": 1, "page": 1, "sort": "modification_number", "order": "asc", "limit": 15}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8")) == expected_response
