# Stdlib imports
import os

# Core Django imports
from django.core.management import call_command

# Third-party app imports
import pytest

# Imports from your apps
from usaspending_api.references.models import Rosetta


@pytest.mark.django_db
def test_rosetta_fresh_load():
    test_file_path = os.path.abspath("usaspending_api/references/tests/data/20181219rosetta-test-file.xlsx")
    all_rows = Rosetta.objects.count()
    assert all_rows == 0, "Table is not empty before testing the loader script. Results will be unexpected"

    call_command("load_rosetta", path=test_file_path)

    all_rows = Rosetta.objects.count()
    assert all_rows == 1, "Loader did not populate the table"

    expected_result = {
        "rows": [
            [
                "1862 Land Grant College",
                "https://www.sam.gov",
                "1862 Land Grant College",
                "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
                "1862_land_grant_college",
                None,
                None,
                None,
                None,
                "Contracts",
                "is1862landgrantcollege",
                None,
            ]
        ],
        "headers": [
            {"raw": "element", "display": "Element"},
            {"raw": "definition", "display": "Definition"},
            {"raw": "fpds_element", "display": "FPDS Data Dictionary Element"},
            {"raw": "award_file", "display": "Award File"},
            {"raw": "award_element", "display": "Award Element"},
            {"raw": "subaward_file", "display": "Subaward File"},
            {"raw": "subaward_element", "display": "Subaward Element"},
            {"raw": "account_file", "display": "Account File"},
            {"raw": "account_element", "display": "Account Element"},
            {"raw": "legacy_award_file", "display": "Award File"},
            {"raw": "legacy_award_element", "display": "Award Element"},
            {"raw": "legacy_subaward_element", "display": "Subaward Element"},
        ],
        "metadata": {"total_rows": 1, "total_size": "10.80KB", "total_columns": 12, "download_location": None},
        "sections": [
            {"colspan": 3, "section": "Schema Data Label & Description"},
            {"colspan": 6, "section": "USA Spending Downloads"},
            {"colspan": 3, "section": "Legacy USA Spending"},
        ],
    }

    actual_result = Rosetta.objects.filter(document_name="api_response").values("document")
    assert expected_result == actual_result[0]["document"]
