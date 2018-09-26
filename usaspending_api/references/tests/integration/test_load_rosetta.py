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
    """
    """
    test_file_path = os.path.abspath("usaspending_api/references/tests/data/data_transparency_crosswalk_test_file.xlsx")
    all_rows = Rosetta.objects.count()
    assert all_rows == 0, "Table is not empty before testing the loader script. Results will be unexpected"

    call_command("load_rosetta", path=test_file_path)

    all_rows = Rosetta.objects.count()
    assert all_rows == 1, "Loader did not populate the table"

    # import pdb; pdb.set_trace()

    expected_result = {
        "rows": [
            [
                "1862 Land Grant College",
                "https://www.sam.gov",
                "1862 Land Grant College",
                "D1",
                "all_contracts_prime_awards_1.csv,\nall_contracts_prime_transactions_1.csv",
                "1862_land_grant_college",
                None,
                None,
                None,
                None,
                "is1862landgrantcollege",
                None,
            ]
        ],
        "headers": [
            {"raw": "element", "display": "Element"},
            {"raw": "definition", "display": "Definition"},
            {"raw": "fpds_element", "display": "FPDS Element"},
            {"raw": "file_a_f", "display": "File\nA-F"},
            {"raw": "award_file", "display": "Award File"},
            {"raw": "award_element", "display": "Award Element"},
            {"raw": "subaward_file", "display": "Subaward File"},
            {"raw": "subaward_element", "display": "Subaward Element"},
            {"raw": "account_file", "display": "Account File"},
            {"raw": "account_element", "display": "Account Element"},
            {"raw": "legacy_award_element", "display": "Award Element"},
            {"raw": "legacy_subaward_element", "display": "Subaward Element"},
        ],
        "metadata": {
            "file_name": "data_transparency_crosswalk_test_file.xlsx",
            "total_rows": 1,
            "total_size": "94.61KB",
            "total_columns": 12,
            "download_location": None,
        },
        "sections": [
            {"colspan": 3, "section": "Schema Data Label & Description"},
            {"colspan": 1, "section": "File"},
            {"colspan": 6, "section": "USA Spending Downloads"},
            {"colspan": 2, "section": "Legacy USA Spending"},
        ],
    }

    actual_result = Rosetta.objects.filter(document_name="api_response").values("document")
    assert expected_result == actual_result[0]["document"]
