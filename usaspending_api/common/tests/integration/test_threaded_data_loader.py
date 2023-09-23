import pytest

from django.conf import settings
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader


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
    file_path_1 = str(settings.APP_DIR / "data" / "testing_data" / "tas_list_1.csv")
    file_1_account_title = "Compensation of Members and Related Administrative Expenses, Senat"

    file_path_2 = str(settings.APP_DIR / "data" / "testing_data" / "tas_list_2.csv")
    file_2_account_title = "Update Test Name"

    # Load it once
    loader.load_from_file(file_path_1)
    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="53021")

    # Check that we loaded successfully
    assert gwa_tas.account_title == file_1_account_title

    # Now load again, but file 2. Collision behavior of "update" should update the name without deleting the record
    gwa_tas.beginning_period_of_availability = 2004
    gwa_tas.save()

    loader.load_from_file(file_path_2)
    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="53021")

    assert gwa_tas.account_title == file_2_account_title
    assert gwa_tas.beginning_period_of_availability == "2004"
    # If this passes, the update collision works

    # Let's test delete!
    loader.collision_behavior = "delete"
    loader.load_from_file(file_path_1)

    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="53021")
    assert gwa_tas.beginning_period_of_availability is None
    assert gwa_tas.account_title == file_1_account_title

    # Now to test skip
    loader.collision_behavior = "skip"
    loader.load_from_file(file_path_2)

    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="53021")
    assert gwa_tas.account_title == file_1_account_title

    # Now test skip and complain
    loader.collision_behavior = "skip_and_complain"
    loader.load_from_file(file_path_2)

    gwa_tas = TreasuryAppropriationAccount.objects.get(treasury_account_identifier="53021")
    assert gwa_tas.account_title == file_1_account_title
