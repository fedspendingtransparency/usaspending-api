import pytest

from django.conf import settings
from django.db import connections
from django.test import TestCase

from usaspending_api.broker.helpers.delete_fabs_transactions import get_delete_pks_for_afa_keys


@pytest.mark.usefixtures("broker_db_setup")
class TestThingWithMultipleDatabases(TestCase):
    databases = "__all__"

    @classmethod
    def setUpTestData(cls):
        connection = connections[settings.DATA_BROKER_DB_ALIAS]
        with connection.cursor() as cursor:

            cursor.execute("select count(*) from published_fabs")
            assert cursor.fetchone()[0] == 0, "Another test somewhere is leaking data"

            cursor.execute(
                """
                insert into published_fabs (
                    published_fabs_id, afa_generated_unique, is_active
                ) (values
                    (1, 'abc', false),
                    (2, 'aBc', false),
                    (3, 'ABC', true),
                    (4, 'xyz', false),
                    (5, 'xYz', false),
                    (6, 'XYZ', false),
                    (7, 'lmn', false),
                    (8, 'opq', true)
                )
                """
            )

    def test_get_delete_pks_for_afa_keys(self):
        assert get_delete_pks_for_afa_keys(None) == []
        assert get_delete_pks_for_afa_keys([]) == []
        assert set(get_delete_pks_for_afa_keys(["abc", "xyZ"])) == {1, 2, 4, 5, 6}
