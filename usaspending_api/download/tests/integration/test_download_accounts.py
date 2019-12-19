import csv
import json
import os
import pytest
import random

from model_mommy import mommy
from rest_framework import status
from unittest.mock import Mock
from zipfile import ZipFile

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
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import ObjectClass, RefProgramActivity


@pytest.fixture
def download_test_data(db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        mommy.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    # Create Locations
    mommy.make("references.Location")

    # Create LE
    mommy.make("references.LegalEntity")

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

    # Create Awarding subs
    mommy.make("references.SubtierAgency", name="Bureau of Things")

    # Create Awarding Agencies
    aa1 = mommy.make("references.Agency", id=1, toptier_agency=ata1, toptier_flag=False)
    aa2 = mommy.make("references.Agency", id=2, toptier_agency=ata2, toptier_flag=False)

    # Create Funding Top Agency
    mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=102,
        name="Bureau of Money",
        toptier_code="102",
        website="http://test.com",
        mission="test",
        icon_filename="test",
    )

    # Create Funding SUB
    mommy.make("references.SubtierAgency", name="Bureau of Things")

    # Create Funding Agency
    mommy.make("references.Agency", id=3, toptier_flag=False)

    # Create Awards
    award1 = mommy.make("awards.Award", id=123, category="idv")
    award2 = mommy.make("awards.Award", id=456, category="contracts")
    award3 = mommy.make("awards.Award", id=789, category="assistance")

    # Create Transactions
    trann1 = mommy.make(
        TransactionNormalized,
        award=award1,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa1,
    )
    trann2 = mommy.make(
        TransactionNormalized,
        award=award2,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2,
    )
    trann3 = mommy.make(
        TransactionNormalized,
        award=award3,
        action_date="2018-01-01",
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2,
    )

    # Create TransactionContract
    mommy.make(TransactionFPDS, transaction=trann1, piid="tc1piid")
    mommy.make(TransactionFPDS, transaction=trann2, piid="tc2piid")

    # Create TransactionAssistance
    mommy.make(TransactionFABS, transaction=trann3, fain="ta1fain")

    # Create FederalAccount
    fa1 = mommy.make(FederalAccount, id=10)

    # Create TreasuryAppropriationAccount
    taa1 = mommy.make(TreasuryAppropriationAccount, treasury_account_identifier=100, federal_account=fa1)

    # Create FinancialAccountsByAwards
    mommy.make(FinancialAccountsByAwards, financial_accounts_by_awards_id=1000, award=award1, treasury_account=taa1)

    # Set latest_award for each award
    update_awards()


@pytest.mark.django_db
def test_tas_a_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_type": "account_balances", "fy": "2017", "quarter": "3"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
def test_tas_b_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_type": "object_class_program_activity", "fy": "2018", "quarter": "1"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
def test_tas_c_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_type": "award_financial", "fy": "2016", "quarter": "4"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
def test_federal_account_a_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_type": "account_balances", "fy": "2017", "quarter": "3"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
def test_federal_account_b_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_type": "object_class_program_activity", "fy": "2018", "quarter": "1"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
def test_federal_account_c_defaults_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_type": "award_financial", "fy": "2016", "quarter": "4"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
def test_agency_filter_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_type": "account_balances", "fy": "2017", "quarter": "4", "agency": "100"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_agency_filter_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {
                    "submission_type": "object_class_program_activity",
                    "fy": "2017",
                    "quarter": "4",
                    "agency": "-2",
                },
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_federal_account_filter_success(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {
                    "submission_type": "award_financial",
                    "fy": "2017",
                    "quarter": "4",
                    "federal_account": "10",
                },
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_federal_account_filter_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {
                    "submission_type": "account_balances",
                    "fy": "2017",
                    "quarter": "4",
                    "federal_account": "-1",
                },
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_account_level_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "not_tas_or_fa",
                "filters": {"submission_type": "account_balances", "fy": "2017", "quarter": "4"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_submission_type_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_type": "not_a_b_or_c", "fy": "2018", "quarter": "2"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_fy_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {"submission_type": "award_financial", "fy": "string_not_int", "quarter": "4"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_quarter_failure(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {"submission_type": "award_financial", "fy": "2017", "quarter": "string_not_int"},
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_download_accounts_bad_filter_type_raises(client, download_test_data):
    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())
    payload = {"account_level": "federal_account", "filters": "01", "columns": []}
    resp = client.post("/api/v2/download/accounts/", content_type="application/json", data=json.dumps(payload))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json()["detail"] == "Filters parameter not provided as a dict"


@pytest.mark.django_db
def test_dev_3997(client, transactional_db):
    """
    As part of DEV-3997, special roll-ups/excisions were added to File B downloads.  This tests to
    ensure those are happening correctly.  The specific conditions we are testing for:

        - Eliminate zero dollar rows where the treasury_account_symbol/federal_account_symbol is
          represented elsewhere in the download with at least one non-zero dollar value.
        - If all rows for a specific treasury_account_symbol/federal_account_symbol are zero dollar,
          sort and pick the first based on the sort criteria:
            - last_reported_submission_period DESCENDING
            - object_class_code
            - program_activity_code
            - budget_function
            - budget_subfunction
            - direct_or_reimbursable
        - FOR federal_account ONLY, in cases where we have multiple records with the same
            - federal_account_symbol
            - object_class_code
            - program_activity_code
            - budget_function
            - budget_subfunction
          and two flavors of direct_reimbursable where at least one is missing (e.g. either null and
          R or null and D), combine all the rows into one, summing their dollar figures and grabbing
          the highest last_reported_submission_period while retaining either the R or D.

    To this end, we are going to perform a simple input => output check by carefully mocking data that
    expresses all of the conditions above ensuring they get rolled up or eliminated correctly in the output.
    """

    def get_csv_contents_and_clean_up(zip_file_path):
        """
        Accepts a CSV ZIP file path, unzips it, reads in the file contained therein, disposes of the
        ZIP file, and returns the pertinent bits of the contents.
        """
        with ZipFile(zip_file_path) as zip_file:
            zip_files = zip_file.namelist()
            file_count = len(zip_files)
            assert file_count > 0, "No files found in zip archive"
            assert file_count < 2, "Expected exactly one file in zip archive"
            csv_file = zip_files[0]
            csv_contents = zip_file.read(csv_file).decode("utf-8")

        os.unlink(zip_file_path)
        csv_reader = csv.DictReader(csv_contents.splitlines())

        important_columns = [
            "last_reported_submission_period",
            "treasury_account_symbol",
            "federal_account_symbol",
            "program_activity_code",
            "object_class_code",
            "direct_or_reimbursable_funding_source",
            "obligations_incurred",
            "deobligations_or_recoveries_or_refunds_from_prior_year",
            "gross_outlay_amount",
        ]

        return [{k: v for k, v in r.items() if k in important_columns} for r in csv_reader]

    download_generation.retrieve_db_string = Mock(return_value=generate_test_db_connection_string())

    mommy.make("download.JobStatus", job_status_id=1, name="ready")
    mommy.make("download.JobStatus", job_status_id=2, name="running")
    mommy.make("download.JobStatus", job_status_id=3, name="finished")
    mommy.make("download.JobStatus", job_status_id=4, name="failed")

    oc = mommy.make(ObjectClass, id=1, object_class="123", direct_reimbursable=None)
    ocd = mommy.make(ObjectClass, id=2, object_class="123", direct_reimbursable="D")
    ocr = mommy.make(ObjectClass, id=3, object_class="123", direct_reimbursable="R")

    pa = mommy.make(RefProgramActivity, id=1, program_activity_code="1234")

    fa = mommy.make(
        FederalAccount, id=1, federal_account_code="000-0001", agency_identifier="000", main_account_code="0001"
    )
    taa = mommy.make(
        TreasuryAppropriationAccount,
        treasury_account_identifier=1001,
        tas_rendering_label="000-X-0001-001",
        federal_account=fa,
        agency_id="000",
        availability_type_code="X",
        main_account_code="0001",
        sub_account_code="001",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=1,
        treasury_account=taa,
        object_class=ocd,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=7,
        deobligations_recoveries_refund_pri_program_object_class_cpe=8,
        gross_outlay_amount_by_program_object_class_cpe=9,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    fa = mommy.make(
        FederalAccount, id=2, federal_account_code="000-0002", agency_identifier="000", main_account_code="0002"
    )
    taa = mommy.make(
        TreasuryAppropriationAccount,
        treasury_account_identifier=2001,
        tas_rendering_label="000-X-0002-001",
        federal_account=fa,
        agency_id="000",
        availability_type_code="X",
        main_account_code="0002",
        sub_account_code="001",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=2,
        treasury_account=taa,
        object_class=ocd,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=7,
        deobligations_recoveries_refund_pri_program_object_class_cpe=8,
        gross_outlay_amount_by_program_object_class_cpe=9,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=3,
        treasury_account=taa,
        object_class=oc,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=7,
        deobligations_recoveries_refund_pri_program_object_class_cpe=8,
        gross_outlay_amount_by_program_object_class_cpe=9,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    fa = mommy.make(
        FederalAccount, id=3, federal_account_code="000-0003", agency_identifier="000", main_account_code="0003"
    )
    taa = mommy.make(
        TreasuryAppropriationAccount,
        treasury_account_identifier=3001,
        tas_rendering_label="000-X-0003-001",
        federal_account=fa,
        agency_id="000",
        availability_type_code="X",
        main_account_code="0003",
        sub_account_code="001",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=4,
        treasury_account=taa,
        object_class=ocd,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=7,
        deobligations_recoveries_refund_pri_program_object_class_cpe=8,
        gross_outlay_amount_by_program_object_class_cpe=9,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=5,
        treasury_account=taa,
        object_class=ocr,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=7,
        deobligations_recoveries_refund_pri_program_object_class_cpe=8,
        gross_outlay_amount_by_program_object_class_cpe=9,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=6,
        treasury_account=taa,
        object_class=oc,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=7,
        deobligations_recoveries_refund_pri_program_object_class_cpe=8,
        gross_outlay_amount_by_program_object_class_cpe=9,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    fa = mommy.make(
        FederalAccount, id=4, federal_account_code="000-0004", agency_identifier="000", main_account_code="0004"
    )
    taa = mommy.make(
        TreasuryAppropriationAccount,
        treasury_account_identifier=4001,
        tas_rendering_label="000-X-0004-001",
        federal_account=fa,
        agency_id="000",
        availability_type_code="X",
        main_account_code="0004",
        sub_account_code="001",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=7,
        treasury_account=taa,
        object_class=ocd,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=7,
        deobligations_recoveries_refund_pri_program_object_class_cpe=8,
        gross_outlay_amount_by_program_object_class_cpe=9,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=8,
        treasury_account=taa,
        object_class=ocr,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=0,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    fa = mommy.make(
        FederalAccount, id=5, federal_account_code="000-0005", agency_identifier="000", main_account_code="0005"
    )
    taa = mommy.make(
        TreasuryAppropriationAccount,
        treasury_account_identifier=5001,
        tas_rendering_label="000-X-0005-001",
        federal_account=fa,
        agency_id="000",
        availability_type_code="X",
        main_account_code="0005",
        sub_account_code="001",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=9,
        treasury_account=taa,
        object_class=ocd,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=0,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    mommy.make(
        FinancialAccountsByProgramActivityObjectClass,
        financial_accounts_by_program_activity_object_class_id=10,
        treasury_account=taa,
        object_class=ocr,
        program_activity=pa,
        obligations_incurred_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=0,
        reporting_period_start="2017-10-01",
        reporting_period_end="2017-12-31",
    )

    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "treasury_account",
                "filters": {
                    "budget_function": "all",
                    "agency": "all",
                    "submission_type": "object_class_program_activity",
                    "fy": "2018",
                    "quarter": "4",
                },
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["status"] == "finished"

    csv_contents = get_csv_contents_and_clean_up(resp.data["url"])

    # 8/10 treasury account rows should remain
    # 000-X-0004-001 with all $0 gets rolled into 000-X-0004-001 with actual dollar values
    # One of 000-X-0005-001 with all $0 gets rolled into the other 000-X-0005-001 with all $0
    assert csv_contents == [
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0001-001",
            "federal_account_symbol": "000-0001",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0002-001",
            "federal_account_symbol": "000-0002",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0002-001",
            "federal_account_symbol": "000-0002",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0003-001",
            "federal_account_symbol": "000-0003",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0003-001",
            "federal_account_symbol": "000-0003",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "R",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0003-001",
            "federal_account_symbol": "000-0003",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0004-001",
            "federal_account_symbol": "000-0004",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0005-001",
            "federal_account_symbol": "000-0005",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "0.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "0.00",
            "gross_outlay_amount": "0.00",
        },
    ]

    resp = client.post(
        "/api/v2/download/accounts/",
        content_type="application/json",
        data=json.dumps(
            {
                "account_level": "federal_account",
                "filters": {
                    "budget_function": "all",
                    "agency": "all",
                    "submission_type": "object_class_program_activity",
                    "fy": "2018",
                    "quarter": "4",
                },
                "file_format": "csv",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["status"] == "finished"

    csv_contents = get_csv_contents_and_clean_up(resp.data["url"])

    # 7/10 federal account rows should remain
    # 000-0002 missing direct_reimbursable gets rolled into 000-0002 with direct_reimbursable
    # 000-0004 with all $0 gets rolled into 000-0004 with actual dollar values
    # One of 000-0005 with all $0 gets rolled into the other 000-0005 with all $0
    assert csv_contents == [
        {
            "last_reported_submission_period": "FY2018Q1",
            "federal_account_symbol": "000-0001",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "federal_account_symbol": "000-0002",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "14.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "16.00",
            "gross_outlay_amount": "18.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "federal_account_symbol": "000-0003",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "federal_account_symbol": "000-0003",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "R",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "federal_account_symbol": "000-0003",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "federal_account_symbol": "000-0004",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "7.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "8.00",
            "gross_outlay_amount": "9.00",
        },
        {
            "last_reported_submission_period": "FY2018Q1",
            "federal_account_symbol": "000-0005",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "0.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "0.00",
            "gross_outlay_amount": "0.00",
        },
    ]
