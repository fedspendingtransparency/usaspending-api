import json

from django.test import TestCase
from rest_framework import status
from usaspending_api.idvs.tests.data.idv_test_data import create_idv_test_data, PARENTS, RECIPIENT_HASH_PREFIX


ENDPOINT = "/api/v2/idvs/activity/"


class IDVAwardsTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        create_idv_test_data()

    @staticmethod
    def _generate_expected_response(total, limit, page, *award_ids):
        """
        Rather than manually generate an insane number of potential responses
        to test the various parameter combinations, we're going to procedurally
        generate them.  award_ids is the list of ids we expect back from the
        request in the order we expect them.  Unfortunately, for this to work,
        test data had to be generated in a specific way.  If you change how
        test data is generated you will probably also have to change this.
        """
        results = []
        for award_id in award_ids:
            string_award_id = str(award_id).zfill(3)
            parent_award_id = PARENTS.get(award_id)
            string_parent_award_id = str(parent_award_id).zfill(3)
            results.append(
                {
                    "award_id": award_id,
                    "awarding_agency": "toptier_awarding_agency_name_%s" % (8500 + award_id),
                    "awarding_agency_id": 8000 + award_id,
                    "generated_unique_award_id": "CONT_IDV_%s" % string_award_id,
                    "period_of_performance_potential_end_date": "2018-08-%02d" % award_id,
                    "parent_award_id": parent_award_id,
                    "parent_generated_unique_award_id": "CONT_IDV_%s" % string_parent_award_id,
                    "parent_award_piid": ("piid_%s" % string_parent_award_id) if parent_award_id else None,
                    "obligated_amount": 100000.0 + award_id,
                    "awarded_amount": 500000.0 + award_id,
                    "period_of_performance_start_date": "2018-02-%02d" % award_id,
                    "piid": "piid_%s" % string_award_id,
                    "recipient_name": "recipient_name_%s" % (7000 + award_id),
                    "recipient_id": "%s%s-%s" % (RECIPIENT_HASH_PREFIX, 7000 + award_id, "R"),
                    "grandchild": award_id in (11, 12, 13, 14),  # based on picture in idv_test_data
                }
            )

        page_metadata = {
            "hasNext": (limit * page < total),
            "hasPrevious": page > 1 and limit * (page - 2) < total,
            "limit": limit,
            "next": page + 1 if (limit * page < total) else None,
            "page": page,
            "previous": page - 1 if (page > 1 and limit * (page - 2) < total) else None,
            "total": total,
        }

        return {"results": results, "page_metadata": page_metadata}

    def _test_post(self, request, expected_response_parameters_tuple=None, expected_status_code=status.HTTP_200_OK):
        """
        Perform the actual request and interrogates the results.

        request is the Python dictionary that will be posted to the endpoint.
        expected_response_parameters are the values that you would normally
            pass into _generate_expected_response but we're going to do that
            for you so just pass the parameters as a tuple or list.
        expected_status_code is the HTTP status we expect to be returned from
            the call to the endpoint.

        Returns... nothing useful.
        """
        response = self.client.post(ENDPOINT, request)
        assert response.status_code == expected_status_code
        if expected_response_parameters_tuple is not None:
            expected_response = self._generate_expected_response(*expected_response_parameters_tuple)
            assert json.loads(response.content.decode("utf-8")) == expected_response

    def test_defaults(self):

        self._test_post({"award_id": 2}, (400002, 10, 1, 14, 13, 12, 11, 10, 9))

        self._test_post({"award_id": "CONT_IDV_002"}, (400002, 10, 1, 14, 13, 12, 11, 10, 9))

    def test_with_nonexistent_id(self):

        self._test_post({"award_id": 0}, (0, 10, 1))

        self._test_post({"award_id": "CONT_IDV_000"}, (0, 10, 1))

    def test_with_bogus_id(self):

        self._test_post({"award_id": None}, (0, 10, 1))

    def test_limit_values(self):

        self._test_post({"award_id": 2, "limit": 1}, (400002, 1, 1, 14))

        self._test_post({"award_id": 2, "limit": 5}, (400002, 5, 1, 14, 13, 12, 11, 10))

        self._test_post({"award_id": 2, "limit": 0}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)

        self._test_post({"award_id": 2, "limit": 2000000000}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)

        self._test_post({"award_id": 2, "limit": {"BOGUS": "LIMIT"}}, expected_status_code=status.HTTP_400_BAD_REQUEST)

    def test_page_values(self):

        self._test_post({"award_id": 2, "limit": 1, "page": 2}, (400002, 1, 2, 13))

        self._test_post({"award_id": 2, "limit": 1, "page": 3}, (400002, 1, 3, 12))

        self._test_post({"award_id": 2, "limit": 1, "page": 10}, (400002, 1, 10))

        self._test_post(
            {"award_id": 2, "limit": 1, "page": 0}, expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

        self._test_post(
            {"award_id": 2, "limit": 1, "page": "BOGUS PAGE"}, expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_hide_edges(self):
        self._test_post({"award_id": 2, "limit": 1, "hide_edge_cases": True}, (6, 1, 1, 14))
        self._test_post({"award_id": 2, "limit": 1, "hide_edge_cases": False}, (400002, 1, 1, 14))
        self._test_post({"award_id": "CONT_IDV_002", "hide_edge_cases": True}, (6, 10, 1, 14, 13, 12, 11, 10, 9))

        self._test_post({"award_id": "CONT_IDV_002", "hide_edge_cases": False}, (400002, 10, 1, 14, 13, 12, 11, 10, 9))
