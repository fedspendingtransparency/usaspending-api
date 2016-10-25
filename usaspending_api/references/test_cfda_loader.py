from django.test import TestCase, Client
from usaspending_api.references.models import CFDAProgram
from django.core.management import call_command
from django.conf import settings
import os
import pytest


class CDFALoadTests(TestCase):

#    fixtures = ['cfda_program']

    @pytest.mark.django_db
    def test_cfda_load(self):
        """
        Ensure cfda data can can be loaded from source file
        """

    def test_program_number(self):
        """
        Make sure an instance of a program number is properly created
        """


    def test_account_identification(self):
        """
        Make sure a account identication is properly mapped to program_number
        """

#        assert(subtier.department == department)
