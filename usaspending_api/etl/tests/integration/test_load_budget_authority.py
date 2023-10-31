import pytest

from django.core.management import call_command
from usaspending_api.accounts.models import BudgetAuthority
from usaspending_api.references.models import OverallTotals
from usaspending_api import settings

BUDGET_AUTHORITY_DATA = settings.APP_DIR / "data" / "budget_authority"


@pytest.mark.django_db
def test_load_budget_authority():
    """
    Verify successful load of historical budget authority info
    """
    assert not OverallTotals.objects.exists()
    assert not BudgetAuthority.objects.exists()
    call_command("load_budget_authority", "--directory", str(BUDGET_AUTHORITY_DATA), "-q", "2")
    assert OverallTotals.objects.exists()
    assert BudgetAuthority.objects.exists()
    assert BudgetAuthority.objects.filter(fr_entity_code__isnull=False).exists()
