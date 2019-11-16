import json

from django.test import TestCase
from model_mommy import mommy
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
    "grandchild_award_count": 5,
    "grandchild_award_total_obligation": 5.03,
    "grandchild_award_base_and_all_options_value": 5.03,
    "grandchild_award_base_exercised_options_val": 5.03,
}


class IDVAmountsTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        mommy.make("awards.Award", pk=1)
        mommy.make(
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

    def _test_get(self, _id, expected_response=None, expected_status_code=status.HTTP_200_OK):
        endpoint = ENDPOINT + str(_id) + "/"
        response = self.client.get(endpoint)
        assert response.status_code == expected_status_code
        if expected_response is not None:
            assert json.loads(response.content.decode("utf-8")) == expected_response

    def test_awards_idvs_amounts_v2(self):
        self._test_get(1, EXPECTED_GOOD_OUTPUT)
        self._test_get("CONT_IDV_2", EXPECTED_GOOD_OUTPUT)
        try:
            response = self.client.get("/api/v2/idvs/amounts/3/")
            assert json.loads(response.content.decode("utf-8"))["detail"] == "No IDV award found with this id"
            assert response.status_code == status.HTTP_404_NOT_FOUND
        except InvalidTemplateLibrary:
            assert json.loads(response.content.decode("utf-8")) == {"detail": "No IDV award found with this id"}
            assert response.status_code == {"detail": "No IDV award found with this id"}
        try:
            response = self.client.get("/api/v2/idvs/amounts/BOGUS_ID/")
            assert response.status_code == status.HTTP_404_NOT_FOUND
        except InvalidTemplateLibrary:
            assert response.status_code == status.HTTP_404_NOT_FOUND
        try:
            response = self.client.get("/api/v2/idvs/amounts/INVALID_ID_12345/")
            assert response.status_code == status.HTTP_404_NOT_FOUND
        except InvalidTemplateLibrary:
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_special_characters(self):
        mommy.make("awards.Award", pk=100, generated_unique_award_id="CONT_IDV_:~$@*\"()#/,^&+=`!'%/_. -_9700")
        mommy.make(
            "awards.ParentAward", award_id=100, generated_unique_award_id="CONT_IDV_:~$@*\"()#/,^&+=`!'%/_. -_9700"
        )
        response = self.client.get("/api/v2/idvs/amounts/CONT_IDV_:~$@*\"()%23/,^&+=`!'%/_. -_9700/")
        assert response.status_code == status.HTTP_200_OK
