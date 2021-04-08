import json
import pytest
import random

from model_mommy import mommy
from rest_framework import status
from unittest.mock import Mock
from itertools import chain, combinations

from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.awards.models import (
    TransactionNormalized,
    TransactionFABS,
    TransactionFPDS,
    FinancialAccountsByAwards,
)
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.common.helpers.generic_helper import generate_test_db_connection_string
from usaspending_api.download.lookups import JOB_STATUS, VALID_ACCOUNT_SUBMISSION_TYPES
from usaspending_api.etl.award_helpers import update_awards


@pytest.fixture
def download_test_data(db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        mommy.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Create Awarding Top Agency
    ata1 = mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=100,
        name="Bureau of Things",
        toptier_code="100",
        website="http://test0.com",
        mission="test0",
        icon_filename="test0",
    )
    ata2 = mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=101,
        name="Bureau of Stuff",
        toptier_code="101",
        website="http://test1.com",
        mission="test1",
        icon_filename="test1",
    )

    # Create Awarding Agencies
    aa1 = mommy.make("references.Agency", id=1, toptier_agency=ata1, toptier_flag=False)
    aa2 = mommy.make("references.Agency", id=2, toptier_agency=ata2, toptier_flag=False)

    # Create Funding Top Agency
    ata3 = mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=102,
        name="Bureau of Money",
        toptier_code="102",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Funding Agency
    mommy.make("references.Agency", id=3, toptier_agency=ata3, toptier_flag=False)

    # Create Awards
    award1 = mommy.make("awards.Award", id=123, category="idv", generated_unique_award_id="CONT_IDV_1")
    award2 = mommy.make("awards.Award", id=456, category="contracts", generated_unique_award_id="CONT_AWD_1")
    award3 = mommy.make("awards.Award", id=789, category="assistance", generated_unique_award_id="ASST_NON_1")

    # Create Transactions
    trann1 = mommy.make(
        TransactionNormalized,
        award=award1,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa1,
        unique_award_key="CONT_IDV_1",
    )
    trann2 = mommy.make(
        TransactionNormalized,
        award=award2,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="CONT_AWD_1",
    )
    trann3 = mommy.make(
        TransactionNormalized,
        award=award3,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2,
        unique_award_key="ASST_NON_1",
    )

    # Create TransactionContract
    mommy.make(TransactionFPDS, transaction=trann1, piid="tc1piid", unique_award_key="CONT_IDV_1")
    mommy.make(TransactionFPDS, transaction=trann2, piid="tc2piid", unique_award_key="CONT_AWD_1")

    # Create TransactionAssistance
    mommy.make(TransactionFABS, transaction=trann3, fain="ta1fain", unique_award_key="ASST_NON_1")

    # Create FederalAccount
    fa1 = mommy.make(FederalAccount, id=10)

    # Create TreasuryAppropriationAccount
    taa1 = mommy.make(TreasuryAppropriationAccount, treasury_account_identifier=100, federal_account=fa1)

    # Create FinancialAccountsByAwards
    mommy.make(FinancialAccountsByAwards, financial_accounts_by_awards_id=1000, award=award1, treasury_account=taa1)

    # Set latest_award for each award
    update_awards()


def test_tas_a_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_types": ["account_balances"], "fy": "2017", "quarter": "3"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_tas_b_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_types": ["object_class_program_activity"], "fy": "2018", "quarter": "1"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_tas_c_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_types": ["award_financial"], "fy": "2016", "quarter": "4"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_federal_account_a_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_types": ["account_balances"], "fy": "2017", "quarter": "3"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_federal_account_b_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_types": ["object_class_program_activity"], "fy": "2018", "quarter": "1"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_federal_account_c_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_types": ["award_financial"], "fy": "2016", "quarter": "4"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["file_url"]


def test_agency_filter_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_types": ["account_balances"], "fy": "2017", "quarter": "4", "agency": "100"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK


def test_agency_filter_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {
                    "submission_types": ["object_class_program_activity"],
                    "fy": "2017",
                    "quarter": "4",
                    "agency": "-2",
                },
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


def test_federal_account_filter_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {
                    "submission_types": ["award_financial"],
                    "fy": "2017",
                    "quarter": "4",
                    "federal_account": "10",
                },
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK


def test_federal_account_filter_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {
                    "submission_types": ["account_balances"],
                    "fy": "2017",
                    "quarter": "4",
                    "federal_account": "-1",
                },
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


def test_account_level_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "not_tas_or_fa",
                "filters": {"submission_types": ["account_balances"], "fy": "2017", "quarter": "4"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


def test_submission_type_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_types": ["not_a_b_or_c"], "fy": "2018", "quarter": "2"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


def test_fy_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_types": ["award_financial"], "fy": "string_not_int", "quarter": "4"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


def test_quarter_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_types": ["award_financial"], "fy": "2017", "quarter": "string_not_int"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


def test_download_accounts_bad_filter_type_raises(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    payload = {"account_level": "federal_account", "filters": "01", "columns": []}
    resp = client.post("/api/v2/download/accounts/", content_type="application/json", data=json.dumps(payload))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json()["detail"] == "Missing value: 'filters|fy' is a required field"


def test_multiple_submission_types_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    def all_subsets(ss):
        return chain(*map(lambda x: combinations(ss, x), range(1, len(ss) + 1)))

    for submission_list in all_subsets(VALID_ACCOUNT_SUBMISSION_TYPES):
        resp = client.post(
            "/api/v2/download/accounts/",
            content_type="application/json",
            data=json.dumps(
                {
                    "account_level": "treasury_account",
                    "filters": {"submission_types": submission_list, "fy": "2017", "quarter": "3"},
                    "file_format": "csv",
                }
            ),
        )

        assert resp.status_code == status.HTTP_200_OK
        assert ".zip" in resp.json()["file_url"]
        if len(submission_list) > 1:
            assert "AccountData" in resp.json()["file_url"]


def test_duplicate_submission_types_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    duplicated_submission_list = VALID_ACCOUNT_SUBMISSION_TYPES * 11

    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_types": duplicated_submission_list, "fy": "2017", "quarter": "3"},
                "file_format": "tsv",
            }
        ),
    )

    download_types = resp.json()["download_request"]["download_types"]

    assert resp.status_code == status.HTTP_200_OK
    assert len(download_types) == len(VALID_ACCOUNT_SUBMISSION_TYPES), "De-duplication failed"
    assert set(download_types) == set(VALID_ACCOUNT_SUBMISSION_TYPES), "Wrong values in response"


def test_empty_submission_types_enum_fail(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_types": [], "fy": "2017", "quarter": "3"},
                "file_format": "tsv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert (
        "Field 'filters|submission_types' value '[]' is below min '1' items" in resp.json()["detail"]
    ), "Incorrect error message"


def test_empty_array_filter_fail(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_types": ["award_financial"], "fy": "2017", "quarter": "3", "def_codes": []},
                "file_format": "tsv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert (
        "Field 'filters|def_codes' value '[]' is below min '1' items" in resp.json()["detail"]
    ), "Incorrect error message"
