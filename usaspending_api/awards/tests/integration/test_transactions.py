import datetime
import json
from decimal import Decimal

# Stdlib imports
import pytest

# Third-party app imports
from model_bakery import baker
from rest_framework import status

# Imports from your apps
from usaspending_api.awards.v2.views.transactions import TransactionViewSet
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.search.models import AwardSearch


# Core Django imports


@pytest.mark.django_db
def test_transaction_endpoint_v2(client):
    """Test the transaction endpoint."""

    resp = client.post("/api/v2/transactions/", {"award_id": "1", "page": "1", "limit": "10", "order": "asc"})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data) > 1


@pytest.mark.django_db
def test_transaction_endpoint_v2_award_fk(client):
    """Test the transaction endpoint."""

    awd = baker.make(
        "search.AwardSearch",
        award_id=10,
        total_obligation="2000",
        latest_transaction_id=1,
        earliest_transaction_id=1,
        latest_transaction_search_id=1,
        earliest_transaction_search_id=1,
        _fill_optional=True,
        generated_unique_award_id="-TEST-",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        transaction_description="this should match",
        _fill_optional=True,
        award=awd,
    )

    resp = client.post("/api/v2/transactions/", {"award_id": 10})
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["description"] == "this should match"

    resp = client.post("/api/v2/transactions/", {"award_id": "-TEST-"})
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["description"] == "this should match"


@pytest.mark.django_db
def test_txn_total_grouped(client):
    """Test award total endpoint with a group parameter."""
    # add this test once the transaction backend is refactored
    pass


def format_response(api_dict):
    svs = TransactionViewSet()
    dec_fields = ["federal_action_obligation", "face_value_loan_guarantee", "original_loan_subsidy_cost"]
    resp = {k: v for k, v in api_dict.items() if k != "transaction_id"}
    resp["description"] = resp["transaction_description"]
    resp = svs._format_results([resp])[0]
    for k, v in resp.items():
        if k in dec_fields:
            resp[k] = Decimal(v)
        else:
            resp[k] = v
    return resp


@pytest.mark.django_db
def test_no_award_id():
    test_payload = {"page": 1, "limit": 10, "order": "asc"}

    svs = TransactionViewSet()
    parse = svs._parse_and_validate_request

    with pytest.raises(UnprocessableEntityException):
        parse(test_payload)


@pytest.mark.django_db
def test_specific_award():
    create_dummy_awards()
    baker.make("search.TransactionSearch", **transaction_1)
    baker.make("search.TransactionSearch", **transaction_2)
    baker.make("search.TransactionSearch", **transaction_3)
    test_payload = {"award_id": "2"}

    svs = TransactionViewSet()
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)

    expected_response = [format_response(transaction_2)]
    assert expected_response == subawards_logic

    test_payload["page"] = 2
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)
    assert [] == subawards_logic


@pytest.mark.django_db
def create_dummy_awards():
    baker.make("search.AwardSearch", **{"award_id": 1})
    dummy_award_1 = AwardSearch.objects.get(award_id=1)
    baker.make("search.AwardSearch", **{"award_id": 2})
    dummy_award_2 = AwardSearch.objects.get(award_id=2)
    baker.make("search.AwardSearch", **{"award_id": 3})
    dummy_award_3 = AwardSearch.objects.get(award_id=3)
    transaction_1["award"] = dummy_award_1
    transaction_2["award"] = dummy_award_2
    transaction_3["award"] = dummy_award_3


transaction_1 = {
    "transaction_id": 1,
    "is_fpds": False,
    "transaction_unique_id": "R02L2A1XL2",
    "type": "A",
    "type_description": "Mauv",
    "action_date": datetime.date(2008, 12, 4),
    "action_type": None,
    "action_type_description": "lobortis",
    "modification_number": "7",
    "transaction_description": "duis aliquam convallis nunc proin at turpis a pede posuere nonummy",
    "federal_action_obligation": "624678.18",
    "face_value_loan_guarantee": "118692.20",
    "original_loan_subsidy_cost": "801283.57",
}

transaction_2 = {
    "transaction_id": 2,
    "is_fpds": True,
    "transaction_unique_id": "O21E7S1PT6",
    "type": None,
    "type_description": "Crimson",
    "action_date": datetime.date(2012, 12, 11),
    "action_type": None,
    "action_type_description": "orci",
    "modification_number": "12",
    "transaction_description": "rhoncus dui vel sem sed sagittis nam",
    "federal_action_obligation": "528261.92",
    "face_value_loan_guarantee": "799182.50",
    "original_loan_subsidy_cost": "414172.86",
}
transaction_3 = {
    "transaction_id": 3,
    "is_fpds": False,
    "transaction_unique_id": "Q25B9A1MQ0",
    "type": "D",
    "type_description": "Green",
    "action_date": datetime.date(2008, 11, 22),
    "action_type": None,
    "action_type_description": "consequat",
    "modification_number": "4",
    "transaction_description": "ligula in lacus curabitur at ipsum ac tellus semper interdum mauris",
    "federal_action_obligation": "177682.30",
    "face_value_loan_guarantee": "418263.20",
    "original_loan_subsidy_cost": "279682.36",
}
