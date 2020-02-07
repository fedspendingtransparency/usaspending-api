import csv
import json
import os
import pytest

from model_mommy import mommy
from rest_framework import status
from unittest.mock import Mock
from zipfile import ZipFile

from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.common.helpers.generic_helper import generate_test_db_connection_string
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import ObjectClass, RefProgramActivity


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
        "budget_function",
        "budget_subfunction",
        "deobligations_or_recoveries_or_refunds_from_prior_year",
        "direct_or_reimbursable_funding_source",
        "federal_account_symbol",
        "gross_outlay_amount",
        "last_reported_submission_period",
        "object_class_code",
        "obligations_incurred",
        "program_activity_code",
        "treasury_account_symbol",
    ]

    return [{k: v for k, v in r.items() if k in important_columns} for r in csv_reader]


# Temporary hotfix to get custom award downloads working until a permanent solution can be found
@pytest.mark.skip
@pytest.mark.django_db(transaction=True)
def test_download_accounts_dev_3997(client):
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
          the highest last_reported_submission_period while retaining the R or D.

    To this end, we are going to perform a simple input => output check by carefully mocking data that
    expresses all of the conditions above ensuring they get rolled up or eliminated correctly in the output.
    """

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
        budget_function_title="Budget Function",
        budget_subfunction_title="Budget Subfunction",
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
        budget_function_title="Budget Function",
        budget_subfunction_title="Budget Subfunction",
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
        budget_function_title="Budget Function",
        budget_subfunction_title="Budget Subfunction",
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
        budget_function_title="Budget Function",
        budget_subfunction_title="Budget Subfunction",
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
        budget_function_title="Budget Function",
        budget_subfunction_title="Budget Subfunction",
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
    assert "api/v2/download/status?file_name=FY2018Q1-Q4_All_TAS_AccountBreakdownByPA-OC" in resp.data["status_url"]

    csv_contents = get_csv_contents_and_clean_up(resp.data["file_url"])

    # 8/10 treasury account rows should remain
    # 000-X-0004-001 with all $0 gets rolled into 000-X-0004-001 with actual dollar values
    # One of 000-X-0005-001 with all $0 gets rolled into the other 000-X-0005-001 with all $0
    assert csv_contents == [
        {
            "last_reported_submission_period": "FY2018Q1",
            "treasury_account_symbol": "000-X-0001-001",
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
    assert "api/v2/download/status?file_name=FY2018Q1-Q4_All_FA_AccountBreakdownByPA-OC" in resp.data["status_url"]

    csv_contents = get_csv_contents_and_clean_up(resp.data["file_url"])

    # 7/10 federal account rows should remain
    # 000-0002 missing direct_reimbursable gets rolled into 000-0002 with direct_reimbursable
    # 000-0004 with all $0 gets rolled into 000-0004 with actual dollar values
    # One of 000-0005 with all $0 gets rolled into the other 000-0005 with all $0
    assert csv_contents == [
        {
            "last_reported_submission_period": "FY2018Q1",
            "federal_account_symbol": "000-0001",
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
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
            "budget_function": "Budget Function",
            "budget_subfunction": "Budget Subfunction",
            "program_activity_code": "1234",
            "object_class_code": "123",
            "direct_or_reimbursable_funding_source": "D",
            "obligations_incurred": "0.00",
            "deobligations_or_recoveries_or_refunds_from_prior_year": "0.00",
            "gross_outlay_amount": "0.00",
        },
    ]
