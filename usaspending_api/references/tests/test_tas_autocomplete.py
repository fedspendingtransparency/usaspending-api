import json

from django.test import TestCase
from model_mommy import mommy


BASE_ENDPOINT = "/api/v2/autocomplete/accounts/"


class TASAutocompleteTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        mommy.make("references.CGAC", cgac_code="000", agency_name="Agency 000", agency_abbreviation="A000")
        mommy.make("references.CGAC", cgac_code="002", agency_name="Agency 002", agency_abbreviation="A002")

        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            allocation_transfer_agency_id=None,
            agency_id="000",
            beginning_period_of_availability=None,
            ending_period_of_availability=None,
            availability_type_code=None,
            main_account_code="2121",
            sub_account_code="212",
        )

        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            allocation_transfer_agency_id="000",
            agency_id="001",
            beginning_period_of_availability="123456",
            ending_period_of_availability="234567",
            availability_type_code=None,
            main_account_code="1234",
            sub_account_code="321",
        )

        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            allocation_transfer_agency_id="001",
            agency_id="002",
            beginning_period_of_availability="923456",
            ending_period_of_availability="934567",
            availability_type_code="X",
            main_account_code="9234",
            sub_account_code="921",
        )

        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            allocation_transfer_agency_id="001",
            agency_id="002",
            beginning_period_of_availability="923456",
            ending_period_of_availability="934567",
            availability_type_code="X",
            main_account_code="9234",
            sub_account_code="921",
        )

    def _post(self, endpoint, request, expected_response):
        response = self.client.post(endpoint, json.dumps(request), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content.decode("utf-8")), expected_response)

    def test_autocomplete_filters(self):
        """
        We're going to do one thoroughish set of tests for ata then a bunch of
        happy path tests for the other individual components.
        """
        endpoint = BASE_ENDPOINT + "ata"

        # Test with no filter.
        self._post(
            endpoint,
            {},
            {"results": [
                {"ata": "000", "agency_name": "Agency 000", "agency_abbreviation": "A000"},
                {"ata": "001", "agency_name": None, "agency_abbreviation": None},
                {"ata": None, "agency_name": None, "agency_abbreviation": None},
            ]},
        )

        # Test with limit.
        self._post(
            endpoint,
            {"limit": 1},
            {"results": [{"ata": "000", "agency_name": "Agency 000", "agency_abbreviation": "A000"}]}
        )

        # Test with filter on component of interest.
        self._post(
            endpoint,
            {"filters": {"ata": "0"}},
            {"results": [
                {"ata": "000", "agency_name": "Agency 000", "agency_abbreviation": "A000"},
                {"ata": "001", "agency_name": None, "agency_abbreviation": None},
            ]},
        )

        # Test with null filter on component of interest.
        self._post(
            endpoint,
            {"filters": {"ata": None}},
            {"results": [{"ata": None, "agency_name": None, "agency_abbreviation": None}]}
        )

        # Test with a bunch of filters.
        self._post(
            endpoint,
            {
                "filters": {
                    "ata": "001",
                    "aid": "002",
                    "bpoa": "923456",
                    "epoa": "934567",
                    "a": "X",
                    "main": "9234",
                    "sub": "921",
                }
            },
            {"results": [{"ata": "001", "agency_name": None, "agency_abbreviation": None}]},
        )

    def test_autocomplete_aid(self):

        self._post(
            BASE_ENDPOINT + "aid",
            {"filters": {"aid": "002"}},
            {"results": [{"aid": "002", "agency_name": "Agency 002", "agency_abbreviation": "A002"}]},
        )
        self._post(BASE_ENDPOINT + "aid", {"filters": {"aid": "2"}}, {"results": []})

    def test_autocomplete_bpoa(self):

        self._post(BASE_ENDPOINT + "bpoa", {"filters": {"bpoa": "9"}}, {"results": ["923456"]})
        self._post(BASE_ENDPOINT + "bpoa", {"filters": {"bpoa": "6"}}, {"results": []})

    def test_autocomplete_epoa(self):

        self._post(BASE_ENDPOINT + "epoa", {"filters": {"epoa": "9"}}, {"results": ["934567"]})
        self._post(BASE_ENDPOINT + "epoa", {"filters": {"epoa": "7"}}, {"results": []})

    def test_autocomplete_a(self):

        self._post(BASE_ENDPOINT + "a", {"filters": {"a": "X"}}, {"results": ["X"]})
        self._post(BASE_ENDPOINT + "a", {"filters": {"a": "Z"}}, {"results": []})

    def test_autocomplete_main(self):

        self._post(BASE_ENDPOINT + "main", {"filters": {"main": "9"}}, {"results": ["9234"]})
        self._post(BASE_ENDPOINT + "main", {"filters": {"main": "4"}}, {"results": []})

    def test_autocomplete_sub(self):

        self._post(BASE_ENDPOINT + "sub", {"filters": {"sub": "9"}}, {"results": ["921"]})
        self._post(BASE_ENDPOINT + "sub", {"filters": {"sub": "1"}}, {"results": []})
