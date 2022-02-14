from unittest import TestCase
from usaspending_api.recipient.v2.views import list_recipients


class TestListRecipients(TestCase):
    def test_build_recipient_identifier_base_query_happy_path(self):
        """Ensure query builder returns with uei entry"""

        expected_uei = "okmju7ygv"
        filters = {
            "keyword": "okmju7ygv",
            "award_type": "all",
            "page": 1,
            "order": "desc",
            "sort": "amount",
            "limit": 50,
        }
        response = list_recipients.build_recipient_identifier_base_query(filters)

        calculated_uei = None
        for child in response.children:
            if child[1] == "okmju7ygv":
                calculated_uei = child[1]

        self.assertIsNotNone(calculated_uei)
        self.assertEqual(3, len(response.children))
        self.assertEqual(expected_uei, calculated_uei)
