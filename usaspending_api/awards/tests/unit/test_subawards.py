# Stdlib imports
import pytest
# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.awards.v2.views.subawards import SubawardsViewSet


def test_all_subawards(mock_matviews_qs):
    subaward_1 = {
        "id": 0,
        "subaward_number": "000",
        "description": """Brunch chips craft direct fixie food gluten-free hoodie jean shorts
keffiyeh lomo mumblecore readymade squid street stumptown thundercats viral wes you probably haven't heard of them. +1
bicycle biodiesel brunch carles chips direct diy ethical fixie gentrify keytar letterpress lomo mi mumblecore organic
photo booth pour-over raw readymade salvia semiotics umami vinyl wes wolf. Beer biodiesel blog brooklyn chips cosby echo
etsy forage future helvetica kale occupy salvia sartorial semiotics skateboard squid williamsburg yr. 8-bit banh
beer before they sold out craft ethnic fingerstache fixie irony jean shorts life organic park photo booth retro
salvia tattooed trade vhs williamsburg.""",
        "action_date": "2017-09-30",
        "amount": "100",
        "recipient_name": "ACME",
    }

    subaward_2 = {
        "id": 1,
        "subaward_number": "001",
        "description": """Aesthetic bushwick chillwave chips cosby fanny pack fap four fund gentrify helvetica hoodie occupy pork raw salvia
sartorial selvage stumptown sustainable tumblr vegan whatever wolf. American artisan authentic chambray cleanse cray
direct freegan future hoodie kale lomo moon party portland readymade skateboard stumptown synth vice wes. +1 belly
flexitarian forage helvetica kale marfa master photo booth pinterest seitan semiotics squid stumptown sweater trade
vegan vhs vice. Aesthetic american beard chambray dreamcatcher echo gastropub hoodie next level pbr photo booth
sartorial scenester terry thundercats truck trust typewriter you probably haven't heard of them.""",
        "action_date": "2017-09-30",
        "amount": "200",
        "recipient_name": "Tools",
    }
    mock_model_1 = MockModel(**subaward_1)
    mock_model_2 = MockModel(**subaward_2)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        "page": 1,
        "limit": 10,
        "sort": "id"
    }
    svs = SubawardsViewSet()
    test_params = svs._parse_and_validate_request(test_payload)

    subawards_logic = svs._business_logic(test_params)

    expected_response = {
        'page_metadata': {
            'page': 1,
            'next': None,
            'previous': None,
            'hasNext': False,
            'hasPrevious': False
        },
        'results': [subaward_1, subaward_2]
    }

    assert expected_response == subawards_logic
