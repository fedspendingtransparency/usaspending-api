from django.test import TransactionTestCase, Client
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from django.core.management import call_command
from django.conf import settings
import os
import pytest


class ThreadedDataLoaderTests(TransactionTestCase):

    @pytest.mark.django_db
    def test_threaded_data_loader(self):
        """
        Test the threaded data loader to ensure full coverage and the testing of
        all collision states
        """
        # Create the field map, value map, and threaded data loader object
        # The field map is truncated because we don't care about most fields
        # actually getting loaded
        field_map = {
            "treasury_account_identifier": "ACCT_NUM",
            "gwa_tas": "GWA_TAS",
            "gwa_tas_name": "GWA_TAS NAME"
        }

        loader = ThreadedDataLoader(model_class=TreasuryAppropriationAccount, field_map=field_map, collision_field='treasury_account_identifier', collision_behavior='update')

        # We'll be using the tas_list.csv, modified to have fewer lines
        file_path_1 = os.path.join(settings.BASE_DIR, 'usaspending_api/data/testing_data/tas_list_1.csv')
        file_1_gwa_tas_name = "Compensation of Members and Related Administrative Expenses, Senat"

        file_path_2 = os.path.join(settings.BASE_DIR, 'usaspending_api/data/testing_data/tas_list_2.csv')
        file_2_gwa_tas_name = "Update Test Name"

        # Load it once
        loader.load_from_file(file_path_1)
        gwa_tas = TreasuryAppropriationAccount.objects.get(gwa_tas='110100')

        # Check that we loaded successfully
        self.assertEqual(gwa_tas.gwa_tas_name, file_1_gwa_tas_name)

        # Now load again, but file 2. Collision behavior of "update" should update the name
        # without deleting the record
        gwa_tas.beginning_period_of_availability = 2004
        gwa_tas.save()

        loader.load_from_file(file_path_2)
        gwa_tas = TreasuryAppropriationAccount.objects.get(gwa_tas='110100')

        self.assertEqual(gwa_tas.gwa_tas_name, file_2_gwa_tas_name)
        self.assertEqual(gwa_tas.beginning_period_of_availability, '2004')
        # If this passes, the update collision works

        # Let's test delete!
        loader.collision_behavior = 'delete'
        loader.load_from_file(file_path_1)

        gwa_tas = TreasuryAppropriationAccount.objects.get(gwa_tas='110100')
        self.assertEqual(gwa_tas.beginning_period_of_availability, None)
        self.assertEqual(gwa_tas.gwa_tas_name, file_1_gwa_tas_name)

        # Now to test skip
        loader.collision_behavior = 'skip'
        loader.load_from_file(file_path_2)

        gwa_tas = TreasuryAppropriationAccount.objects.get(gwa_tas='110100')
        self.assertEqual(gwa_tas.gwa_tas_name, file_1_gwa_tas_name)

        # Now test skip and complain
        loader.collision_behavior = 'skip_and_complain'
        loader.load_from_file(file_path_2)

        gwa_tas = TreasuryAppropriationAccount.objects.get(gwa_tas='110100')
        self.assertEqual(gwa_tas.gwa_tas_name, file_1_gwa_tas_name)
