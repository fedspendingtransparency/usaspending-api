import pytest

from django.conf import settings
from django.core.management import call_command

from usaspending_api.references.models import Cfda


@pytest.fixture(scope="function")
def cfda_data(db):
    "Load from small test CSV to test database"
    file_path = settings.APP_DIR / "references" / "management" / "commands" / "cfda_sample.csv"
    fullpath = "file://{}".format(file_path)

    call_command("loadcfda", fullpath)


@pytest.mark.django_db
def test_cfda_load(cfda_data):
    """
    Ensure cfda data can can be loaded from source file
    """

    pass


@pytest.mark.django_db
def test_program_number(cfda_data):
    """
    Make sure an instance of a program number is properly created
    """

    Cfda.objects.get(program_number="10.054", program_title="Emergency Conservation Program")


@pytest.mark.django_db
def test_account_identification(cfda_data):
    """
    Make sure a account identification is properly mapped to program_number
    """
    Cfda.objects.get(program_number="10.03", account_identification="12-1600-0-1-352;")
