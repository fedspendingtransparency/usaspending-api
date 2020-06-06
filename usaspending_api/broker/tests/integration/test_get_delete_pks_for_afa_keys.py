import pytest

from django.db import connections
from django.test import TestCase
from usaspending_api.broker.helpers.delete_fabs_transactions import get_delete_pks_for_afa_keys


@pytest.mark.usefixtures("broker_db_setup")
class TestThingWithMultipleDatabases(TestCase):
    databases = "__all__"

    @classmethod
    def setUpTestData(cls):
        connection = connections["data_broker"]
        with connection.cursor() as cursor:

            cursor.execute("select count(*) from published_award_financial_assistance")
            assert cursor.fetchone()[0] == 0, "Another test somewhere is leaking data"

            cursor.execute(
                """
                insert into published_award_financial_assistance (
                    published_award_financial_assistance_id, afa_generated_unique, is_active
                ) (values
                    (1, 'abc', false),
                    (2, 'aBc', false),
                    (3, 'ABC', true),
                    (4, 'xyz', false),
                    (5, 'xYz', false),
                    (6, 'XYZ', false),
                    (7, 'lmn', false),
                    (8, 'opq', false)
                )
                """
            )

    def test_get_delete_pks_for_afa_keys(self):
        assert get_delete_pks_for_afa_keys(None) == []
        assert set(get_delete_pks_for_afa_keys(["abc", "xyZ"])) == {1, 2, 4, 5, 6}
