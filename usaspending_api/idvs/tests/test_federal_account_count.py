from django.test import TestCase
from rest_framework import status
from usaspending_api.idvs.tests.data.idv_test_data import create_idv_test_data


class IDVFundingTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        create_idv_test_data()

    def test_defaults(self):
        response = self.client.get("/api/v2/idvs/count/federal_account/1/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 1})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/2/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 6
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 2})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/3/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 3})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/4/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 4})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/5/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 5})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/7/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 2
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 7})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]

        response = self.client.get("/api/v2/idvs/count/federal_account/8/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 2
        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 8})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == response.data["count"]


        # test with generated id
        response = self.client.get("/api/v2/idvs/count/federal_account/GENERATED_UNIQUE_AWARD_ID_001/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1

    def test_with_nonexistent_id(self):
        response = self.client.get("/api/v2/idvs/count/federal_account/0/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0
        #
        response = self.client.get("/api/v2/idvs/count/federal_account/GENERATED_UNIQUE_AWARD_ID_000/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0

    def test_with_piid(self):
        # doesn't return federal accounts for idvs
        response = self.client.get("/api/v2/idvs/count/federal_account/2/?piid=piid_002")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0
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
