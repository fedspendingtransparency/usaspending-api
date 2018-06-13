# Stdlib imports
import pytest
# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel


# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.awards.v2.views.transactions import TransactionViewSet


def strip_award_id(api_dict):
    return {k: v for k, v in api_dict.items() if k != 'award_id'}


@pytest.mark.django_db
def test_all_transactions(mock_matviews_qs):
    mock_model_1 = MockModel(**transaction_1)
    mock_model_2 = MockModel(**transaction_2)
    mock_model_3 = MockModel(**transaction_3)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3])

    test_payload = {
        "page": 1,
        "limit": 10,
        "order": "asc",
    }
    svs = TransactionViewSet()
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)

    expected_response = [strip_award_id(transaction_1), strip_award_id(transaction_2), strip_award_id(transaction_3)]

    assert expected_response == subawards_logic

    test_payload['page'] = 2
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)
    assert [] == subawards_logic

    test_payload = {
        "order": "desc",
    }
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)
    assert [strip_award_id(transaction_3), strip_award_id(transaction_2), strip_award_id(transaction_1)] == subawards_logic

@pytest.mark.django_db
def test_specific_award(mock_matviews_qs):
    mock_model_1 = MockModel(**transaction_10)
    mock_model_2 = MockModel(**transaction_9)
    mock_model_3 = MockModel(**transaction_8)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3])

    test_payload = {
        "award_id": 99
    }

    svs = TransactionViewSet()
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)

    expected_response = [strip_award_id(transaction_9), strip_award_id(transaction_10)]

    assert expected_response == subawards_logic


transaction_1 = {
    "transaction_unique_id": "R02L2A1XL2",
    "type": "A",
    "type_description": "Mauv",
    "action_date": "2008-12-04",
    "action_type": None,
    "action_type_description": "lobortis",
    "modification_number": 7,
    "description": "duis aliquam convallis nunc proin at turpis a pede posuere nonummy integer non velit donec diam neque vestibulum eget",
    "federal_action_obligation": 624678.18,
    "face_value_loan_guarantee": 118692.2,
    "original_loan_subsidy_cost": 801283.57
}

transaction_2 = {
    "transaction_unique_id": "O21E7S1PT6",
    "type": None,
    "type_description": "Crimson",
    "action_date": "2012-12-11",
    "action_type": None,
    "action_type_description": "orci",
    "modification_number": 12,
    "description": "rhoncus dui vel sem sed sagittis nam",
    "federal_action_obligation": 528261.92,
    "face_value_loan_guarantee": 799182.5,
    "original_loan_subsidy_cost": 414172.86
}
transaction_3 = {
    "transaction_unique_id": "Q25B9A1MQ0",
    "type": "D",
    "type_description": "Green",
    "action_date": "2008-11-22",
    "action_type": None,
    "action_type_description": "consequat",
    "modification_number": 4,
    "description": "ligula in lacus curabitur at ipsum ac tellus semper interdum mauris ullamcorper purus sit amet nulla",
    "federal_action_obligation": 177682.3,
    "face_value_loan_guarantee": 418263.2,
    "original_loan_subsidy_cost": 279682.36
}

transaction_4 = {
    "transaction_unique_id": "C93E0W3SH5",
    "type": "03",
    "type_description": "Maroon",
    "action_date": "2016-05-12",
    "action_type": None,
    "action_type_description": "sit",
    "modification_number": 15,
    "description": "convallis nulla neque libero convallis eget eleifend luctus ultricies",
    "federal_action_obligation": 359308.55,
    "face_value_loan_guarantee": 734027.87,
    "original_loan_subsidy_cost": 707114.19
}
transaction_5 = {
    "transaction_unique_id": "X10E2G3LI9",
    "type": None,
    "type_description": "Red",
    "action_date": "2011-11-14",
    "action_type": None,
    "action_type_description": "nascetur",
    "modification_number": 0,
    "description": "at turpis donec posuere metus vitae ipsum aliquam non mauris morbi non lectus aliquam sit amet diam in magna",
    "federal_action_obligation": 663019.49,
    "face_value_loan_guarantee": 702863.26,
    "original_loan_subsidy_cost": 886273.58
}
transaction_6 = {
    "transaction_unique_id": "R61J5A1PX7",
    "type": None,
    "type_description": "Turquoise",
    "action_date": "2016-07-31",
    "action_type": None,
    "action_type_description": "elit",
    "modification_number": 8,
    "description": "faucibus orci luctus et ultrices posuere cubilia curae donec pharetra magna vestibulum aliquet",
    "federal_action_obligation": 905914.17,
    "face_value_loan_guarantee": 26578.66,
    "original_loan_subsidy_cost": 556276.27
}
transaction_7 = {
    "transaction_unique_id": "M94V9T8GV2",
    "type": "C",
    "type_description": "Turquoise",
    "action_date": "2012-02-22",
    "action_type": None,
    "action_type_description": "ac",
    "modification_number": 4,
    "description": "consectetuer adipiscing elit proin interdum mauris",
    "federal_action_obligation": 456489.19,
    "face_value_loan_guarantee": 838244.57,
    "original_loan_subsidy_cost": 763710.46
}
transaction_8 = {
    "transaction_unique_id": "P75U1K9YM5",
    "type": "10",
    "type_description": "Violet",
    "action_date": "2013-04-18",
    "action_type": None,
    "action_type_description": "nibh",
    "modification_number": 4,
    "description": "nunc viverra dapibus nulla suscipit ligula in lacus curabitur at ipsum ac tellus semper interdum mauris ullamcorper purus",
    "federal_action_obligation": 256327.94,
    "face_value_loan_guarantee": 559112.87,
    "original_loan_subsidy_cost": 664865.52
}
transaction_9 = {
    "transaction_unique_id": "M19M1G6ZJ5",
    "type": None,
    "type_description": "Turquoise",
    "action_date": "2016-09-10",
    "action_type": None,
    "action_type_description": "eu",
    "modification_number": 12,
    "description": "luctus et ultrices posuere cubilia curae donec",
    "federal_action_obligation": 825169.0,
    "face_value_loan_guarantee": 411511.1,
    "original_loan_subsidy_cost": 527077.27
}
transaction_10 = {
    "transaction_unique_id": "M60E7H0IV2",
    "type": "00",
    "type_description": "Crimson",
    "action_date": "2014-11-02",
    "action_type": None,
    "action_type_description": "nulla",
    "modification_number": 6,
    "description": "nibh ligula nec sem",
    "federal_action_obligation": 899208.39,
    "face_value_loan_guarantee": 907561.53,
    "original_loan_subsidy_cost": 653592.37
}
