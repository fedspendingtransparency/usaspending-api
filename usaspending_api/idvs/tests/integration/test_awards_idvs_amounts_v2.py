import json

import pytest
from django.template.library import InvalidTemplateLibrary
from django.test import Client
from model_bakery import baker
from rest_framework import status

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
def _test_data(db: None) -> None:
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


def _test_get(
    client: Client,
    _id: int | str,
    expected_response: dict | None = None,
    expected_status_code: int = status.HTTP_200_OK,
) -> None:
    endpoint = ENDPOINT + str(_id) + "/"
    response = client.get(endpoint)
    assert response.status_code == expected_status_code
    if expected_response is not None:
        assert json.loads(response.content.decode("utf-8")) == expected_response


@pytest.mark.django_db
def test_awards_idvs_amounts_v2(client: Client, _test_data: None) -> None:
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
def test_special_characters(client: Client) -> None:
    baker.make("search.AwardSearch", award_id=100, generated_unique_award_id="CONT_IDV_:~$@*\"()#/,^&+=`!'%/. -_9700")
    baker.make("awards.ParentAward", award_id=100, generated_unique_award_id="CONT_IDV_:~$@*\"()#/,^&+=`!'%/. -_9700")
    response = client.get("/api/v2/idvs/amounts/CONT_IDV_:~$@*\"()%23/,^&+=`!'%/. -_9700/")
    assert response.status_code == status.HTTP_200_OK


@pytest.fixture
def idv_tree(db: None) -> None:
    """An IDV with two child contracts and a child IDV that has two contracts of its own.

    A third contract names the child IDV's PIID under another agency and so belongs to neither.
    """
    defc = baker.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19")
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        reporting_fiscal_year=2020,
        reporting_fiscal_period=12,
        is_final_balances_for_fy=True,
    )
    contract = {"type": "A", "category": "contract"}
    baker.make(
        "search.AwardSearch",
        award_id=10,
        generated_unique_award_id="CONT_IDV_ROOT_9700",
        type="IDV_B",
        category="idv",
        piid="ROOT",
        fpds_agency_id="9700",
    )
    baker.make(
        "search.AwardSearch",
        award_id=11,
        parent_award_piid="ROOT",
        fpds_parent_agency_id="9700",
        total_outlays=10,
        **contract,
    )
    baker.make(
        "search.AwardSearch",
        award_id=12,
        parent_award_piid="ROOT",
        fpds_parent_agency_id="9700",
        total_outlays=5,
        **contract,
    )
    baker.make(
        "search.AwardSearch",
        award_id=13,
        generated_unique_award_id="CONT_IDV_BPA1_9700",
        type="IDV_E",
        category="idv",
        piid="BPA1",
        fpds_agency_id="9700",
        parent_award_piid="ROOT",
        fpds_parent_agency_id="9700",
    )
    baker.make(
        "search.AwardSearch",
        award_id=14,
        parent_award_piid="BPA1",
        fpds_parent_agency_id="9700",
        total_outlays=1,
        **contract,
    )
    baker.make(
        "search.AwardSearch",
        award_id=15,
        parent_award_piid="BPA1",
        fpds_parent_agency_id="9700",
        total_outlays=2,
        **contract,
    )
    baker.make(
        "search.AwardSearch",
        award_id=16,
        parent_award_piid="BPA1",
        fpds_parent_agency_id="4700",
        total_outlays=1000,
        **contract,
    )
    baker.make("awards.ParentAward", award_id=10, generated_unique_award_id="CONT_IDV_ROOT_9700")
    baker.make("awards.ParentAward", award_id=13, generated_unique_award_id="CONT_IDV_BPA1_9700", parent_award_id=10)
    for award_id, obligated, outlay in ((11, 100, 50), (14, 20, 7), (16, 999, 999)):
        baker.make(
            "awards.FinancialAccountsByAwards",
            award_id=award_id,
            submission_id=1,
            disaster_emergency_fund=defc,
            transaction_obligated_amount=obligated,
            gross_outlay_amount_by_award_cpe=outlay,
        )


@pytest.mark.django_db
@pytest.mark.parametrize("award_id", [10, "CONT_IDV_ROOT_9700"])
def test_child_and_grandchild_outlays_and_account_amounts(client: Client, idv_tree: None, award_id: int | str) -> None:
    response = client.get(f"{ENDPOINT}{award_id}/")
    assert response.status_code == status.HTTP_200_OK
    result = json.loads(response.content.decode("utf-8"))
    assert result["child_award_total_outlay"] == 15
    assert result["grandchild_award_total_outlay"] == 3
    assert result["child_total_account_obligation"] == 100
    assert result["child_total_account_outlay"] == 50
    assert result["child_account_obligations_by_defc"] == [{"code": "L", "amount": 100}]
    assert result["grandchild_total_account_obligation"] == 20
    assert result["grandchild_total_account_outlay"] == 7
    assert result["grandchild_account_outlays_by_defc"] == [{"code": "L", "amount": 7}]
