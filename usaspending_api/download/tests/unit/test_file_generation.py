from unittest.mock import MagicMock

from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import VALUE_MAPPINGS


def test_get_account_balances_csv_sources():
    original = VALUE_MAPPINGS["account_balances"]["filter_function"]
    VALUE_MAPPINGS["account_balances"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {"download_types": ["account_balances"], "account_level": "treasury_account", "filters": {}}
    )
    VALUE_MAPPINGS["account_balances"]["filter_function"] = original
    assert len(csv_sources) == 1
    assert csv_sources[0].file_type == "treasury_account"
    assert csv_sources[0].source_type == "account_balances"


def test_get_object_class_program_activity_csv_sources():
    original = VALUE_MAPPINGS["object_class_program_activity"]["filter_function"]
    VALUE_MAPPINGS["object_class_program_activity"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {"download_types": ["object_class_program_activity"], "account_level": "treasury_account", "filters": {}}
    )
    VALUE_MAPPINGS["object_class_program_activity"]["filter_function"] = original
    assert len(csv_sources) == 1
    assert csv_sources[0].file_type == "treasury_account"
    assert csv_sources[0].source_type == "object_class_program_activity"


def test_get_award_financial_csv_sources():
    original = VALUE_MAPPINGS["award_financial"]["filter_function"]
    VALUE_MAPPINGS["award_financial"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {"download_types": ["award_financial"], "account_level": "treasury_account", "filters": {}}
    )
    VALUE_MAPPINGS["award_financial"]["filter_function"] = original
    assert len(csv_sources) == 3
    assert csv_sources[0].file_type == "treasury_account"
    assert csv_sources[0].source_type == "award_financial"
    assert csv_sources[0].extra_file_type == "Contracts_"
    assert csv_sources[1].file_type == "treasury_account"
    assert csv_sources[1].source_type == "award_financial"
    assert csv_sources[1].extra_file_type == "Assistance_"


def test_idv_treasury_account_funding_csv_sources():
    original = VALUE_MAPPINGS["idv_federal_account_funding"]["filter_function"]
    VALUE_MAPPINGS["idv_federal_account_funding"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {
            "download_types": ["idv_federal_account_funding"],
            "account_level": "treasury_account",
            "filters": {"award_id": 0},
        }
    )
    assert len(csv_sources) == 1
    VALUE_MAPPINGS["idv_federal_account_funding"]["filter_function"] = original
    assert csv_sources[0].file_type == "treasury_account"
    assert csv_sources[0].source_type == "idv_federal_account_funding"
