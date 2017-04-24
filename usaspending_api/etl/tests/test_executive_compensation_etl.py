import pytest
import json

from django.core.management import call_command
from usaspending_api.references.models import LegalEntityOfficers

from model_mommy import mommy


@pytest.fixture
def test_exec_etl_fixture():
    mommy.make('references.LegalEntity', recipient_unique_id="061530416")
    mommy.make('references.LegalEntity', recipient_unique_id="555498187")
    mommy.make('references.LegalEntity', recipient_unique_id="966833766")


@pytest.mark.django_db
def test_exec_compensation_all_duns(test_exec_etl_fixture):
    # First, run the command
    call_command("load_executive_compensation", "-a", "--test")

    assert LegalEntityOfficers.objects.filter(officer_1_amount__isnull=True).count() == 0

    leo = LegalEntityOfficers.objects.get(legal_entity__recipient_unique_id="966833766")
    assert leo.officer_1_amount == 217000.00

    leo = LegalEntityOfficers.objects.get(legal_entity__recipient_unique_id="555498187")
    assert leo.officer_1_amount == 842224.00

    leo = LegalEntityOfficers.objects.get(legal_entity__recipient_unique_id="061530416")
    assert leo.officer_1_amount == 3.50
