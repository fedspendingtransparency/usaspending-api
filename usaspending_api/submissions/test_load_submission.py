from django.test import TestCase, Client
from django.core.management import call_command
from usaspending_api.accounts.models import *
from usaspending_api.awards.models import *
from usaspending_api.financial_activities.models import *
from usaspending_api.submissions.models import *

import pytest
import os

from model_mommy import mommy


class LoadSubmissionTest(TestCase):

    fixtures = ['endpoint_fixture_db']

    def setUp(self):
        Procurement.objects.all().delete()
        SubmissionAttributes.objects.all().delete()
        AppropriationAccountBalances.objects.all().delete()
        FinancialAccountsByProgramActivityObjectClass.objects.all().delete()
        FinancialAccountsByAwards.objects.all().delete()
        FinancialAccountsByAwardsTransactionObligations.objects.all().delete()
        FinancialAssistanceAward.objects.all().delete()
        Location.objects.all().delete()
        LegalEntity.objects.all().delete()
        Award.objects.all().delete()

    @pytest.mark.django_db
    def test_load_submission_command(self):
        """
        Test the submission loader to validate the ETL process
        """
        # Load the RefObjClass and ProgramActivityCode data
        call_command('load_submission', '-1', '--delete', '--test')
        self.assertEqual(SubmissionAttributes.objects.all().count(), 1)
        self.assertEqual(AppropriationAccountBalances.objects.all().count(), 1)
        self.assertEqual(FinancialAccountsByProgramActivityObjectClass.objects.all().count(), 10)
        self.assertEqual(FinancialAccountsByAwards.objects.all().count(), 11)
        self.assertEqual(FinancialAccountsByAwardsTransactionObligations.objects.all().count(), 11)
        self.assertEqual(Location.objects.all().count(), 4)
        self.assertEqual(LegalEntity.objects.all().count(), 2)
        self.assertEqual(Award.objects.all().count(), 7)
        self.assertEqual(Procurement.objects.all().count(), 1)
        self.assertEqual(FinancialAssistanceAward.objects.all().count(), 1)
