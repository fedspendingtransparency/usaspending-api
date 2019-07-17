from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader, cleanse_values
from django.conf import settings
import os
import pytest


@pytest.mark.django_db(transaction=True)
def test_threaded_data_loader():
    """
    Test the threaded data loader to ensure full coverage and the testing of all collision states
    """
    # Create the field map, value map, and threaded data loader object
    # The field map is truncated because we don't care about most fields actually getting loaded
    field_map = {"treasury_account_identifier": "ACCT_NUM", "account_title": "GWA_TAS_NAME"}

    loader = ThreadedDataLoader(
        model_class=TreasuryAppropriationAccount,
        field_map=field_map,
        collision_field="treasury_account_identifier",
        collision_behavior="update",
    )

    # We'll be using the tas_list.csv, modified to have fewer lines
    file_path_1 = os.path.join(settings.BASE_DIR, "usaspending_api/data/testing_data/tas_list_1.csv")
    file_1_account_title = "Compensation of Members and Related Administrative Expenses, Senat"

    file_path_2 = os.path.join(settings.BASE_DIR, "usaspending_api/data/testing_data/tas_list_2.csv")
    file_2_account_title = "Update Test Name"

    # Load it once
    loader.load_from_file(file_path_1)
    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="45736")

    # Check that we loaded successfully
    assert gwa_tas.account_title == file_1_account_title

    # Now load again, but file 2. Collision behavior of "update" should update the name without deleting the record
    gwa_tas.beginning_period_of_availability = 2004
    gwa_tas.save()

    loader.load_from_file(file_path_2)
    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="45736")

    assert gwa_tas.account_title == file_2_account_title
    assert gwa_tas.beginning_period_of_availability == "2004"
    # If this passes, the update collision works

    # Let's test delete!
    loader.collision_behavior = "delete"
    loader.load_from_file(file_path_1)

    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="45736")
    assert gwa_tas.beginning_period_of_availability is None
    assert gwa_tas.account_title == file_1_account_title

    # Now to test skip
    loader.collision_behavior = "skip"
    loader.load_from_file(file_path_2)

    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="45736")
    assert gwa_tas.account_title == file_1_account_title

    # Now test skip and complain
    loader.collision_behavior = "skip_and_complain"
    loader.load_from_file(file_path_2)

    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="45736")
    assert gwa_tas.account_title == file_1_account_title


def test_cleanse_values():
    """Test that sloppy values in CSV are cleaned before use"""

    row = {"a": "  15", "b": "abcde", "c": "null", "d": "Null", "e": "   ", "f": " abc def "}
    result = cleanse_values(row)
    expected = {"a": "15", "b": "abcde", "c": None, "d": None, "e": "", "f": "abc def"}
    assert result == expected
