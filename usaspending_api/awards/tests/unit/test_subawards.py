# Stdlib imports

# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel


# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.awards.v2.views.subawards import SubawardsViewSet


def strip_award_id(api_dict):
    d = {k: v for k, v in api_dict.items() if k != 'award_id'}
    return remap_subaward_id(d)


def remap_subaward_id(api_dict):
    if "subaward_id" in api_dict:
        api_dict["id"] = api_dict.pop("subaward_id")
    else:
        api_dict["subaward_id"] = api_dict.pop("id")
    return api_dict


def test_all_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(**subaward_1)
    mock_model_2 = MockModel(**subaward_2)
    mock_model_3 = MockModel(**subaward_3)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3])

    test_payload = {
        "page": 1,
        "limit": 10,
        "order": "asc",
    }
    svs = SubawardsViewSet()
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)

    expected_response = [strip_award_id(subaward_1), strip_award_id(subaward_2), strip_award_id(subaward_3)]

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
    assert [strip_award_id(subaward_3), strip_award_id(subaward_2), strip_award_id(subaward_1)] == subawards_logic


def test_specific_award(mock_matviews_qs):
    mock_model_1 = MockModel(**subaward_10)
    mock_model_2 = MockModel(**subaward_11)
    mock_model_3 = MockModel(**subaward_12)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3])

    test_payload = {
        "award_id": 99
    }

    svs = SubawardsViewSet()
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)

    expected_response = [strip_award_id(subaward_11), strip_award_id(subaward_10)]

    assert expected_response == subawards_logic


subaward_1 = {
    "subaward_id": 2,
    "subaward_number": "000",
    "description":
        "Brunch chips craft direct fixie food gluten-free hoodie jean shorts keffiyeh lomo mumblecore readymade"
        " squid street stumptown thundercats viral wes you probably haven't heard of them. +1 bicycle biodiesel"
        " brunch carles chips direct diy ethical fixie gentrify keytar letterpress lomo mi mumblecore organic"
        " photo booth pour-over raw readymade salvia semiotics umami vinyl wes wolf. Beer biodiesel blog brooklyn"
        " chips cosby echo etsy forage future helvetica kale occupy salvia sartorial semiotics skateboard squid"
        " williamsburg yr. 8-bit banh beer before they sold out craft ethnic fingerstache fixie irony jean shorts"
        " life organic park photo booth retro salvia tattooed trade vhs williamsburg.",
    "action_date": "2017-09-30",
    "amount": "100",
    "recipient_name": "ACME",
    "award_id": 99,
}

subaward_2 = {
    "subaward_id": 1,
    "subaward_number": "001",
    "description":
        "Aesthetic bushwick chillwave chips cosby fanny pack four fund gentrify helvetica hoodie occupy pork raw"
        " salvia sartorial selvage stumptown sustainable tumblr vegan whatever wolf. American artisan authentic"
        " chambray cleanse cray direct freegan future hoodie kale lomo moon party portland readymade skateboard"
        " stumptown synth vice wes. +1 belly flexitarian forage helvetica kale marfa master photo booth pinterest"
        " seitan semiotics squid stumptown sweater trade vegan vhs vice. Aesthetic american beard chambray"
        " dreamcatcher echo gastropub hoodie next level pbr photo booth sartorial scenester terry thundercats"
        " truck trust typewriter you probably haven't heard of them.",
    "action_date": "2017-09-30",
    "amount": "200",
    "recipient_name": "Tools",
    "award_id": 99,
}

subaward_3 = {
    "subaward_id": 3,
    "subaward_number": "002",
    "description":
        "Ennui gluten-free keytar mixtape pitchfork selvage tattooed tofu viral yr. Austin banksy biodiesel"
        " carles fingerstache forage gentrify godard jean shorts kale sustainable terry vinyl. American bag"
        " bespoke bushwick cliche echo farm-to-table forage future gastropub gentrify keffiyeh life odd"
        " selvage semiotics thundercats twee williamsburg. Banksy brooklyn cred diy fixie forage locavore"
        " scenester skateboard sriracha. Art banh butcher chambray chillwave cred denim ennui farm-to-table"
        " kale life mumblecore park party pour-over raw sartorial seitan selvage single-origin coffee trade"
        " typewriter whatever williamsburg wolf. American banh before they sold out blog cray direct ethnic"
        " farm-to-table fingerstache food four freegan future gentrify kale life moon pour-over single-origin"
        " coffee small street trade twee umami wolf yr.",
    "action_date": "2010-04-03",
    "amount": "5000",
    "recipient_name": "Inc",
    "award_id": 99,
}

subaward_10 = {
    "subaward_id": 10,
    "subaward_number": "1234",
    "description": "Sub Award #10",
    "action_date": "2010-04-03",
    "amount": "5000",
    "recipient_name": "Big",
    "award_id": 99,
}

subaward_11 = {
    "subaward_id": 11,
    "subaward_number": "1235",
    "description": "Sub Award #11",
    "action_date": "2018-01-01",
    "amount": "400",
    "recipient_name": "Corp",
    "award_id": 99,
}

subaward_12 = {
    "subaward_id": 12,
    "subaward_number": "1236",
    "description": "subaward_11",
    "action_date": "2017-03-02",
    "amount": "444",
    "recipient_name": "First Tractor",
    "award_id": 88,
}
