import datetime
import pytest

from model_bakery import baker

from usaspending_api.awards.v2.views.subawards import SubawardsViewSet


def create_subaward_test_data(*subawards_data_list):
    baker.make("search.AwardSearch", award_id=88, generated_unique_award_id="generated_unique_award_id_for_88")
    baker.make("search.AwardSearch", award_id=99, generated_unique_award_id="generated_unique_award_id_for_99")

    for subaward in subawards_data_list:
        baker.make("search.SubawardSearch", **subaward)


def _strip_award_id(api_dict):
    field_mapping = {
        "broker_subaward_id": "id",
        "subaward_number": "subaward_number",
        "subaward_description": "description",
        "sub_action_date": "action_date",
        "subaward_amount": "amount",
        "sub_awardee_or_recipient_legal": "recipient_name",
    }
    return {field_mapping[k]: v for k, v in api_dict.items() if k not in ("award_id", "unique_award_key")}


@pytest.mark.django_db
def test_all_subawards():
    create_subaward_test_data(subaward_1, subaward_2, subaward_3)

    test_payload = {"page": 1, "limit": 10, "order": "asc"}
    svs = SubawardsViewSet()
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)

    expected_response = [_strip_award_id(subaward_1), _strip_award_id(subaward_2), _strip_award_id(subaward_3)]

    assert expected_response == subawards_logic

    test_payload["page"] = 2
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)
    assert [] == subawards_logic

    sub_1 = _strip_award_id(subaward_1)
    sub_2 = _strip_award_id(subaward_2)
    sub_3 = _strip_award_id(subaward_3)

    assert request_with_sort("id") == [sub_3, sub_1, sub_2]
    assert request_with_sort("amount") == [sub_3, sub_2, sub_1]
    assert request_with_sort("action_date") == [sub_2, sub_1, sub_3]
    assert request_with_sort("recipient_name") == [sub_2, sub_3, sub_1]


def request_with_sort(sort):
    svs = SubawardsViewSet()
    test_payload = {"page": 1, "limit": 4, "sort": sort, "order": "desc"}
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)
    return subawards_logic


@pytest.mark.django_db
def test_specific_award():
    create_subaward_test_data(subaward_10, subaward_11, subaward_12)

    test_payload = {"award_id": 99}

    svs = SubawardsViewSet()
    test_params = svs._parse_and_validate_request(test_payload)
    subawards_logic = svs._business_logic(test_params)

    expected_response = [_strip_award_id(subaward_11), _strip_award_id(subaward_10)]

    assert expected_response == subawards_logic


subaward_1 = {
    "broker_subaward_id": 2,
    "subaward_number": "000",
    "subaward_description": "Brunch chips craft direct fixie food gluten-free hoodie jean shorts keffiyeh lomo mumblecore"
    " readymade squid street stumptown thundercats viral wes you probably haven't heard of them. +1 bicycle biodiesel"
    " brunch carles chips direct diy ethical fixie gentrify keytar letterpress lomo mi mumblecore organic"
    " photo booth pour-over raw readymade salvia semiotics umami vinyl wes wolf. Beer biodiesel blog brooklyn"
    " chips cosby echo etsy forage future helvetica kale occupy salvia sartorial semiotics skateboard squid"
    " williamsburg yr. 8-bit banh beer before they sold out craft ethnic fingerstache fixie irony jean shorts"
    " life organic park photo booth retro salvia tattooed trade vhs williamsburg.",
    "sub_action_date": datetime.date(2017, 9, 29),
    "subaward_amount": 100.0,
    "sub_awardee_or_recipient_legal": "ACME",
    "award_id": 99,
    "unique_award_key": "generated_unique_award_id_for_99",
}

subaward_2 = {
    "broker_subaward_id": 1,
    "subaward_number": "001",
    "subaward_description": "Aesthetic bushwick chillwave chips cosby fanny pack four fund gentrify helvetica hoodie occupy pork"
    " raw salvia sartorial selvage stumptown sustainable tumblr vegan whatever wolf. American artisan authentic"
    " chambray cleanse cray direct freegan future hoodie kale lomo moon party portland readymade skateboard"
    " stumptown synth vice wes. +1 belly flexitarian forage helvetica kale marfa master photo booth pinterest"
    " seitan semiotics squid stumptown sweater trade vegan vhs vice. Aesthetic american beard chambray"
    " dreamcatcher echo gastropub hoodie next level pbr photo booth sartorial scenester terry thundercats"
    " truck trust typewriter you probably haven't heard of them.",
    "sub_action_date": datetime.date(2017, 9, 30),
    "subaward_amount": 200.0,
    "sub_awardee_or_recipient_legal": "TOOLS",
    "award_id": 99,
    "unique_award_key": "generated_unique_award_id_for_99",
}

subaward_3 = {
    "broker_subaward_id": 3,
    "subaward_number": "002",
    "subaward_description": "Ennui gluten-free keytar mixtape pitchfork selvage tattooed tofu viral yr. Austin banksy biodiesel"
    " carles fingerstache forage gentrify godard jean shorts kale sustainable terry vinyl. American bag"
    " bespoke bushwick cliche echo farm-to-table forage future gastropub gentrify keffiyeh life odd"
    " selvage semiotics thundercats twee williamsburg. Banksy brooklyn cred diy fixie forage locavore"
    " scenester skateboard sriracha. Art banh butcher chambray chillwave cred denim ennui farm-to-table"
    " kale life mumblecore park party pour-over raw sartorial seitan selvage single-origin coffee trade"
    " typewriter whatever williamsburg wolf. American banh before they sold out blog cray direct ethnic"
    " farm-to-table fingerstache food four freegan future gentrify kale life moon pour-over single-origin"
    " coffee small street trade twee umami wolf yr.",
    "sub_action_date": datetime.date(2010, 4, 3),
    "subaward_amount": 5000.0,
    "sub_awardee_or_recipient_legal": "INC",
    "award_id": 99,
    "unique_award_key": "generated_unique_award_id_for_99",
}

subaward_10 = {
    "broker_subaward_id": 10,
    "subaward_number": "1234",
    "subaward_description": "Sub Award #10",
    "sub_action_date": datetime.date(2010, 4, 3),
    "subaward_amount": 5000.0,
    "sub_awardee_or_recipient_legal": "BIG",
    "award_id": 99,
    "unique_award_key": "generated_unique_award_id_for_99",
}

subaward_11 = {
    "broker_subaward_id": 11,
    "subaward_number": "1235",
    "subaward_description": "Sub Award #11",
    "sub_action_date": datetime.date(2018, 1, 1),
    "subaward_amount": 400.0,
    "sub_awardee_or_recipient_legal": "CORP",
    "award_id": 99,
    "unique_award_key": "generated_unique_award_id_for_99",
}

subaward_12 = {
    "broker_subaward_id": 12,
    "subaward_number": "1236",
    "subaward_description": "subaward_11",
    "sub_action_date": datetime.date(2017, 3, 2),
    "subaward_amount": 444.0,
    "sub_awardee_or_recipient_legal": "FIRST TRACTOR",
    "award_id": 88,
    "unique_award_key": "generated_unique_award_id_for_88",
}
