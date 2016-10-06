from django.test import TestCase, Client
from usaspending_api.references.models import Agency
from django.core.management import call_command
from django.conf import settings
import os
import pytest


class AgencyLoadTests(TestCase):

    fixtures = ['agencies']

    @pytest.mark.django_db
    def test_contract_load(self):
        """
        Ensure agencies can be loaded from source file
        """
        call_command('loadagencies')

    def test_department(self):
        """
        Make sure an instance of a department is properly created
        """

        department = Agency.objects.get(cgac_code='002', fpds_code='0000', subtier_code='0000')

    def test_subtier(self):
        """
        Make sure a subtier is properly mapped to its parent department
        """

        subtier = Agency.objects.get(cgac_code='002', fpds_code='0000', subtier_code='0001')
        department = Agency.objects.get(cgac_code='002', fpds_code='0000', subtier_code='0000')
        assert(subtier.department == department)
