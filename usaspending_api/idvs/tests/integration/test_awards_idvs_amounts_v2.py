import json

import pytest
from model_bakery import baker
from rest_framework import status
from django.template.library import InvalidTemplateLibrary

ENDPOINT = "/api/v2/idvs/amounts/"


EXPECTED_GOOD_OUTPUT = {
    "award_id": 1,
    "generated_unique_award_id": "CONT_IDV_2",
    "child_idv_count": 3,
    "child_award_count": 4,
    "child_award_total_obligation": 5.01,
    "child_award_base_and_all_options_value": 6.02,
    "child_award_base_exercised_options_val": 7.03,
    "child_total_account_outlay": 0,
    "child_total_account_obligation": 0,
    "child_account_outlays_by_defc": [],
    "child_account_obligations_by_defc": [],
    "child_award_total_outlay": None,
    "grandchild_award_count": 5,
    "grandchild_award_total_obligation": 5.03,
    "grandchild_award_base_and_all_options_value": 5.03,
    "grandchild_award_base_exercised_options_val": 5.03,
    "grandchild_total_account_outlay": 0,
    "grandchild_total_account_obligation": 0,
    "grandchild_account_outlays_by_defc": [],
    "grandchild_account_obligations_by_defc": [],
    "grandchild_award_total_outlay": None,
}


@pytest.fixture
def _test_data(db):
    baker.make("search.AwardSearch", award_id=1)
    baker.make(
        "awards.ParentAward",
        award_id=1,
        generated_unique_award_id="CONT_IDV_2",
        direct_idv_count=3,
        direct_contract_count=4,
        direct_total_obligation="5.01",
        direct_base_and_all_options_value="6.02",
        direct_base_exercised_options_val="7.03",
        rollup_idv_count=8,
        rollup_contract_count=9,
        rollup_total_obligation="10.04",
        rollup_base_and_all_options_value="11.05",
        rollup_base_exercised_options_val="12.06",
    )


def _test_get(client, _id, expected_response=None, expected_status_code=status.HTTP_200_OK):
    endpoint = ENDPOINT + str(_id) + "/"
    response = client.get(endpoint)
    assert response.status_code == expected_status_code
    if expected_response is not None:
        assert json.loads(response.content.decode("utf-8")) == expected_response


@pytest.mark.django_db
def test_awards_idvs_amounts_v2(client, _test_data):
    _test_get(client, 1, EXPECTED_GOOD_OUTPUT)
    _test_get(client, "CONT_IDV_2", EXPECTED_GOOD_OUTPUT)
    try:
        response = client.get("/api/v2/idvs/amounts/3/")
        assert json.loads(response.content.decode("utf-8"))["detail"] == "No IDV award found with this id"
        assert response.status_code == status.HTTP_404_NOT_FOUND
    except InvalidTemplateLibrary:
        assert json.loads(response.content.decode("utf-8")) == {"detail": "No IDV award found with this id"}
        assert response.status_code == {"detail": "No IDV award found with this id"}
    try:
        response = client.get("/api/v2/idvs/amounts/BOGUS_ID/")
        assert response.status_code == status.HTTP_404_NOT_FOUND
    except InvalidTemplateLibrary:
        assert response.status_code == status.HTTP_404_NOT_FOUND
    try:
        response = client.get("/api/v2/idvs/amounts/INVALID_ID_12345/")
        assert response.status_code == status.HTTP_404_NOT_FOUND
    except InvalidTemplateLibrary:
        assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
def test_special_characters(client):
    baker.make("search.AwardSearch", award_id=100, generated_unique_award_id="CONT_IDV_:~$@*\"()#/,^&+=`!'%/_. -_9700")
    baker.make("awards.ParentAward", award_id=100, generated_unique_award_id="CONT_IDV_:~$@*\"()#/,^&+=`!'%/_. -_9700")
    response = client.get("/api/v2/idvs/amounts/CONT_IDV_:~$@*\"()%23/,^&+=`!'%/_. -_9700/")
    assert response.status_code == status.HTTP_200_OK
