from os.path import join

from django.core.management import call_command

from usaspending_api.accounts.models import BudgetAuthority
from usaspending_api.references.models import OverallTotals

import pytest


@pytest.fixture()
@pytest.mark.django_db
def flushed():
    call_command("flush", "--noinput")
    call_command("loaddata", "endpoint_fixture_db")
    BudgetAuthority.objects.all().delete()
    OverallTotals.objects.all().delete()


@pytest.mark.django_db
def test_load_budget_authority(flushed):
    """
    Verify successful load of historical budget authority info
    """
    directory_path = join("usaspending_api", "data", "budget_authority")
    assert not OverallTotals.objects.exists()
    assert not BudgetAuthority.objects.exists()
    call_command("load_budget_authority", "--directory", directory_path, "-q", "2")
    assert OverallTotals.objects.exists()
    assert BudgetAuthority.objects.exists()
    BudgetAuthority.objects.filter(fr_entity_code__isnull=False).exists()
