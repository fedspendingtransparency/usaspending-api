import pytest

from django.core.management import call_command
from os.path import join
from usaspending_api.accounts.models import BudgetAuthority
from usaspending_api.references.models import OverallTotals


@pytest.mark.django_db
def test_load_budget_authority():
    """
    Verify successful load of historical budget authority info
    """
    directory_path = join("usaspending_api", "data", "testing_data", "budget_authority")
    assert not OverallTotals.objects.exists()
    assert not BudgetAuthority.objects.exists()
    call_command("load_budget_authority", "--directory", directory_path, "-q", "2")
    assert OverallTotals.objects.exists()
    assert BudgetAuthority.objects.exists()
    assert BudgetAuthority.objects.filter(fr_entity_code__isnull=False).exists()
