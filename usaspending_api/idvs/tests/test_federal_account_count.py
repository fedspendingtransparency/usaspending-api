import json

from django.test import TestCase
from rest_framework import status
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.idvs.v2.views.funding import SORTABLE_COLUMNS
from usaspending_api.idvs.tests.data.idv_test_data import create_idv_test_data
from usaspending_api.awards.models import ParentAward

class IDVFundingTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        create_idv_test_data()

    def test_defaults(self):
        pa = ParentAward.objects.all()
        for award in pa:
            print("award_id: {}, parent_award: {}".format(award.award_id, award.parent_award_id))

        response = self.client.get("/api/v2/idvs/count/federal_account/1/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 5

        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 1})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == 5

        response = self.client.get("/api/v2/idvs/count/federal_account/2/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 10

        response2 = self.client.post("/api/v2/idvs/funding/", {"award_id": 2})
        assert response2.status_code == status.HTTP_200_OK
        assert len(response2.data["results"]) == 10

        response = self.client.get("/api/v2/idvs/count/federal_account/3/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1

        response = self.client.get("/api/v2/idvs/count/federal_account/4/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1

        response = self.client.get("/api/v2/idvs/count/federal_account/5/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1

        response = self.client.get("/api/v2/idvs/count/federal_account/7/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 3

        response = self.client.get("/api/v2/idvs/count/federal_account/8/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 3

        response = self.client.get("/api/v2/idvs/count/federal_account/9/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 2

        response = self.client.get("/api/v2/idvs/count/federal_account/GENERATED_UNIQUE_AWARD_ID_001/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 5

    def test_with_nonexistent_id(self):
        response = self.client.get("/api/v2/idvs/count/federal_account/0/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0
        #
        response = self.client.get("/api/v2/idvs/count/federal_account/GENERATED_UNIQUE_AWARD_ID_000/")
        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 0

    # def test_with_bogus_id(self):

        # self._test_post({"award_id": None}, (None, None, 1, False, False))