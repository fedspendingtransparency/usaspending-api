# Stdlib imports

# Core Django imports
from django.core.management import call_command

# Third-party app imports
import pytest

# Imports from your apps
from usaspending_api.references.models import RefProgramActivity


@pytest.mark.django_db
def test_program_activity_fresh_load():
    """
    Test the program activity load to ensure data is loaded with the following expectations:
        1. Program activity name is uppercased
        2. Duplicate entries are not loaded

    TODO: future rework will redefine what defines a duplicate by removing 'budget_year' from the definition

    A unique set is defined by the following columns:
        program_activity_code
        program_activity_name
        responsible_agency_id
        allocation_transfer_agency_id
        main_account_code
        budget_year'
    """

    call_command("load_program_activity", "usaspending_api/references/tests/data/program_activity.csv")

    expected_results = {"count": 6, "program_activity_name_lowercase_found": False}

    actual_results = {
        "count": RefProgramActivity.objects.count(),
        "program_activity_name_lowercase_found": all(
            name.islower() for name in RefProgramActivity.objects.values_list("program_activity_name", flat=True)
        ),
    }

    assert expected_results == actual_results
