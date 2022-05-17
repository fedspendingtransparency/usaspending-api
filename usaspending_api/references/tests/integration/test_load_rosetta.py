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
    test_file_path = os.path.abspath("usaspending_api/references/tests/data/20220517rosetta-test-file.xlsx")
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
                "Award Recipient",
                "F = False\nT = True",
                None,
                "Contracts_PrimeAwardSummaries.csv,\nContracts_PrimeTransactions.csv",
                "1862_land_grant_college",
                None,
                None,
                None,
                None,
                "transaction_fpds",
                "c1862_land_grant_college",
                "Contracts",
                "is1862landgrantcollege",
                None,
            ]
        ],
        "headers": [
            {"raw": "A:element", "display": "Element"},
            {"raw": "B:definition", "display": "Definition"},
            {"raw": "C:fpds_data_dictionary_element", "display": "FPDS Data Dictionary Element"},
            {"raw": "D:grouping", "display": "Grouping"},
            {"raw": "E:domain_values", "display": "Domain Values"},
            {"raw": "F:domain_values_code_description", "display": "Domain Values Code Description"},
            {"raw": "G:award_file", "display": "Award File"},
            {"raw": "H:award_element", "display": "Award Element"},
            {"raw": "I:subaward_file", "display": "Subaward File"},
            {"raw": "J:subaward_element", "display": "Subaward Element"},
            {"raw": "K:account_file", "display": "Account File"},
            {"raw": "L:account_element", "display": "Account Element"},
            {"raw": "M:table", "display": "Table"},
            {"raw": "N:element", "display": "Element"},
            {"raw": "O:award_file", "display": "Award File"},
            {"raw": "P:award_element", "display": "Award Element"},
            {"raw": "Q:subaward_element", "display": "Subaward Element"},
        ],
        "metadata": {
            "total_rows": 1,
            "total_size": "61.22KB",
            "total_columns": 17,
            "download_location": test_file_path,
        },
        "sections": [
            {"colspan": 6, "section": "Schema Data Label & Description"},
            {"colspan": 6, "section": "USA Spending Downloads"},
            {"colspan": 2, "section": "Database Download"},
            {"colspan": 3, "section": "Legacy USA Spending"},
        ],
    }

    actual_result = Rosetta.objects.filter(document_name="api_response").values("document")

    assert expected_result == actual_result[0]["document"]
