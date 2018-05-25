from usaspending_api.references.models import Cfda
from usaspending_api.references.management.commands.loadcfda import load_cfda
import os
import pytest
from django.conf import settings


# Scoping to module would save time, but db object is function-scoped
@pytest.fixture(scope='function')
def cfda_data(db):
    "Load from small test CSV to test database"
    path = 'usaspending_api/references/management/commands/programs-01pct-usaspending.csv'
    path = os.path.normpath(path)
    fullpath = os.path.join(settings.BASE_DIR, path)
    load_cfda(fullpath)


# @pytest.mark.django_db
def test_cfda_load(cfda_data):
    """
    Ensure cfda data can can be loaded from source file
    """

    pass


# @pytest.mark.django_db
def test_program_number(cfda_data):
    """
    Make sure an instance of a program number is properly created
    """

    Cfda.objects.get(program_number='10.051', program_title='Commodity Loans and Loan Deficiency Payments')


# @pytest.mark.django_db
def test_account_identification(cfda_data):
    """
    Make sure a account identication is properly mapped to program_number
    """
    Cfda.objects.get(program_number='10.066', account_identification='12-4336-0-3-351;')

    #        assert(subtier.department == department)
