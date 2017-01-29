from usaspending_api.references.models import CFDAProgram
from django.core.management import call_command
from django.conf import settings
import os
import pytest


@pytest.mark.django_db
def test_cfda_load():
    """
    Ensure cfda data can can be loaded from source file
    """
    call_command('loadcfda')


@pytest.mark.django_db
def test_program_number():
    """
    Make sure an instance of a program number is properly created
    """
    call_command('loadcfda')
    program_number = CFDAProgram.objects.get(
        program_number='98.011', program_title='Global Development Alliance')


@pytest.mark.django_db
def test_account_identification():
    """
    Make sure a account identication is properly mapped to program_number
    """
    call_command('loadcfda')
    account_identification = CFDAProgram.objects.get(
        program_number='98.009', account_identification='12-2278-0-1-151.')
    #        assert(subtier.department == department)


# TESTING TODO: put these call_commands in a fixture so they get cleaned up?
