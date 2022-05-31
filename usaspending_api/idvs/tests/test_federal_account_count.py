from django.test import TestCase
from rest_framework import status
from usaspending_api.idvs.tests.data.idv_test_data import create_idv_test_data
from model_bakery import baker


class IDVFundingTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        create_idv_test_data()

    def test_defaults(self):
        response = self.client.get("/api/v2/idvs/count/federal_account/1/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 5
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 1})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/2/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 9
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 2})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/3/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 3})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/4/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 4})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/5/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 5})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/7/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 3
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 7})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/8/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 3
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 8})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        # test with generated id
        response = self.client.get("/api/v2/idvs/count/federal_account/CONT_IDV_001/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 5

    def test_with_nonexistent_id(self):
        response = self.client.get("/api/v2/idvs/count/federal_account/0/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0
        # invalid id
        response = self.client.get("/api/v2/idvs/count/federal_account/CONT_IDV_000/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0

    def test_with_piid(self):
        # returns federal accounts for idvs
        response = self.client.get("/api/v2/idvs/count/federal_account/2/?piid=piid_002")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 2, "piid": "piid_002"})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        # returns results for child awards
        response = self.client.get("/api/v2/idvs/count/federal_account/2/?piid=piid_011")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 2, "piid": "piid_011"})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

    def test_special_characters(self):
        baker.make("awards.Award", id=100, generated_unique_award_id="CONT_IDV_:~$@*\"()#/,^&+=`!'%/_. -_9700")
        response = self.client.get("/api/v2/idvs/count/federal_account/CONT_IDV_:~$@*\"()%23/,^&+=`!'%/_. -_9700/")
        assert response.status_code == status.HTTP_200_OK

        response = self.client.get(
            "/api/v2/idvs/count/federal_account/CONT_IDV_:~$@*\"()%23/,^&+=`!'%/_. -_9700/?piid=:~$@*\"()%23/,^&+=`!'%/_. -"
        )
        assert response.status_code == status.HTTP_200_OK
