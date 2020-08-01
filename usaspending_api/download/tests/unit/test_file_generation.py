from unittest.mock import MagicMock

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping, contract_type_mapping, idv_type_mapping
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import VALUE_MAPPINGS


def test_get_awards_csv_sources(db):
    original = VALUE_MAPPINGS["awards"]["filter_function"]
    VALUE_MAPPINGS["awards"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {"download_types": ["awards"], "filters": {"award_type_codes": list(award_type_mapping.keys())}}
    )
    assert len(csv_sources) == 2
    VALUE_MAPPINGS["awards"]["filter_function"] = original
    assert csv_sources[0].file_type == "d1"
    assert csv_sources[0].source_type == "awards"
    assert csv_sources[1].file_type == "d2"
    assert csv_sources[1].source_type == "awards"


def test_get_transactions_csv_sources(db):
    original = VALUE_MAPPINGS["transactions"]["filter_function"]
    VALUE_MAPPINGS["transactions"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {"download_types": ["transactions"], "filters": {"award_type_codes": list(award_type_mapping.keys())}}
    )
    assert len(csv_sources) == 2
    VALUE_MAPPINGS["transactions"]["filter_function"] = original
    assert csv_sources[0].file_type == "d1"
    assert csv_sources[0].source_type == "transactions"
    assert csv_sources[1].file_type == "d2"
    assert csv_sources[1].source_type == "transactions"


def test_get_sub_awards_csv_sources(db):
    original = VALUE_MAPPINGS["sub_awards"]["filter_function"]
    VALUE_MAPPINGS["sub_awards"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {"download_types": ["sub_awards"], "filters": {"award_type_codes": list(award_type_mapping.keys())}}
    )
    assert len(csv_sources) == 2
    VALUE_MAPPINGS["sub_awards"]["filter_function"] = original
    assert csv_sources[0].file_type == "d1"
    assert csv_sources[0].source_type == "sub_awards"
    assert csv_sources[1].file_type == "d2"
    assert csv_sources[1].source_type == "sub_awards"


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
    assert len(csv_sources) == 1
    assert csv_sources[0].file_type == "treasury_account"
    assert csv_sources[0].source_type == "award_financial"


def test_idv_orders_csv_sources(db):
    original = VALUE_MAPPINGS["idv_orders"]["filter_function"]
    VALUE_MAPPINGS["idv_orders"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {
            "download_types": ["idv_orders"],
            "filters": {"award_id": 0, "award_type_codes": tuple(set(contract_type_mapping) | set(idv_type_mapping))},
        }
    )
    assert len(csv_sources) == 1
    VALUE_MAPPINGS["idv_orders"]["filter_function"] = original
    assert csv_sources[0].file_type == "d1"
    assert csv_sources[0].source_type == "idv_orders"


def test_idv_transactions_csv_sources(db):
    original = VALUE_MAPPINGS["idv_transaction_history"]["filter_function"]
    VALUE_MAPPINGS["idv_transaction_history"]["filter_function"] = MagicMock(returned_value="")
    csv_sources = download_generation.get_download_sources(
        {
            "download_types": ["idv_transaction_history"],
            "filters": {"award_id": 0, "award_type_codes": tuple(set(contract_type_mapping) | set(idv_type_mapping))},
        }
    )
    assert len(csv_sources) == 1
    VALUE_MAPPINGS["idv_transaction_history"]["filter_function"] = original
    assert csv_sources[0].file_type == "d1"
    assert csv_sources[0].source_type == "idv_transaction_history"


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
