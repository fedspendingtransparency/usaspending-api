import pytest

from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connections

from usaspending_api.references.models import ProgramActivityPark
from usaspending_api.settings import DATA_BROKER_DB_ALIAS


@pytest.fixture
def setup_broker_data():
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO program_activity_park (program_activity_park_id, fiscal_year, period, agency_id, allocation_transfer_id, main_account_number, sub_account_number, park_code, park_name)
            VALUES
                (1, 2025, 6, '000', NULL, '0000', NULL, '5ZC3H17Q8NJ', 'Acquisition Workforce Development'),
                (2, 2025, 4, '000', NULL, '0000', NULL, '60HXXG853PV', 'Administrative Expenses');
        """
        )
    yield
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute("TRUNCATE program_activity_park;")


@pytest.mark.django_db(databases=[DATA_BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_load_park(setup_broker_data):
    actual_park_count = ProgramActivityPark.objects.count()
    assert actual_park_count == 0

    call_command("load_park")

    expected_results = [
        ("0000", "UNKNOWN/OTHER"),
        ("5ZC3H17Q8NJ", "ACQUISITION WORKFORCE DEVELOPMENT"),
        ("60HXXG853PV", "ADMINISTRATIVE EXPENSES"),
    ]
    actual_park = list(ProgramActivityPark.objects.order_by("code").values_list("code", "name"))
    assert actual_park == expected_results

    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO program_activity_park (program_activity_park_id, fiscal_year, period, agency_id, allocation_transfer_id, main_account_number, sub_account_number, park_code, park_name)
            VALUES (3, 2025, 6, '000', NULL, '0000', NULL, '61TX9TDQEK7', 'AG IN THE CLASSROOM')
        """
        )
    call_command("load_park")
    expected_results.append(("61TX9TDQEK7", "AG IN THE CLASSROOM"))
    actual_park = list(ProgramActivityPark.objects.order_by("code").values_list("code", "name"))
    assert actual_park == expected_results
