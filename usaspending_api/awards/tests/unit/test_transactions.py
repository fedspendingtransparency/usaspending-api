# Stdlib imports
import pytest
import datetime
from decimal import Decimal

# Core Django imports

# Third-party app imports
from model_mommy import mommy

# Imports from your apps
from usaspending_api.awards.v2.views.transactions import TransactionViewSet
from usaspending_api.awards.models import Award
from usaspending_api.common.exceptions import UnprocessableEntityException


def format_response(api_dict):
    svs = TransactionViewSet()
    dec_fields = ["federal_action_obligation", "face_value_loan_guarantee", "original_loan_subsidy_cost"]
    resp = {k: v for k, v in api_dict.items() if k != "id"}
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
    mommy.make("awards.TransactionNormalized", **transaction_1)
    mommy.make("awards.TransactionNormalized", **transaction_2)
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
    mommy.make("awards.Award", **{"id": 1})
    dummy_award_1 = Award.objects.get(id=1)
    mommy.make("awards.Award", **{"id": 2})
    dummy_award_2 = Award.objects.get(id=2)
    mommy.make("awards.Award", **{"id": 3})
    dummy_award_3 = Award.objects.get(id=3)
    transaction_1["award"] = dummy_award_1
    transaction_2["award"] = dummy_award_2
    transaction_3["award"] = dummy_award_3


transaction_1 = {
    "is_fpds": False,
    "transaction_unique_id": "R02L2A1XL2",
    "type": "A",
    "type_description": "Mauv",
    "action_date": datetime.date(2008, 12, 4),
    "action_type": None,
    "action_type_description": "lobortis",
    "modification_number": "7",
    "description": "duis aliquam convallis nunc proin at turpis a pede posuere nonummy",
    "federal_action_obligation": "624678.18",
    "face_value_loan_guarantee": "118692.20",
    "original_loan_subsidy_cost": "801283.57",
}

transaction_2 = {
    "is_fpds": True,
    "transaction_unique_id": "O21E7S1PT6",
    "type": None,
    "type_description": "Crimson",
    "action_date": datetime.date(2012, 12, 11),
    "action_type": None,
    "action_type_description": "orci",
    "modification_number": "12",
    "description": "rhoncus dui vel sem sed sagittis nam",
    "federal_action_obligation": "528261.92",
    "face_value_loan_guarantee": "799182.50",
    "original_loan_subsidy_cost": "414172.86",
}
transaction_3 = {
    "is_fpds": False,
    "transaction_unique_id": "Q25B9A1MQ0",
    "type": "D",
    "type_description": "Green",
    "action_date": datetime.date(2008, 11, 22),
    "action_type": None,
    "action_type_description": "consequat",
    "modification_number": "4",
    "description": "ligula in lacus curabitur at ipsum ac tellus semper interdum mauris",
    "federal_action_obligation": "177682.30",
    "face_value_loan_guarantee": "418263.20",
    "original_loan_subsidy_cost": "279682.36",
}
