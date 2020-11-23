import pytest

from datetime import datetime
from decimal import Decimal

from django.conf import settings
from model_mommy import mommy

from usaspending_api.common.helpers.sql_helpers import get_connection


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    sub = mommy.make(
        "submissions.SubmissionAttributes", submission_id=1, reporting_fiscal_year=2019, reporting_fiscal_period=3
    )
    agency = mommy.make("references.ToptierAgency", toptier_code="123", abbreviation="ABC", name="Test Agency")

    treas_accounts = [
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=1,
            funding_toptier_agency_id=agency.toptier_agency_id,
            tas_rendering_label="tas-1",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            funding_toptier_agency_id=agency.toptier_agency_id,
            tas_rendering_label="tas-2",
        ),
    ]
    approps = [
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[0], "ob_incur": 50},
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[1], "ob_incur": 12},
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[1], "ob_incur": 29},
    ]
    for approp in approps:
        mommy.make(
            "accounts.AppropriationAccountBalances",
            submission_id=approp["sub_id"],
            treasury_account_identifier=approp["treasury_account"],
            obligations_incurred_total_by_tas_cpe=approp["ob_incur"],
        )

    ocpas = [
        {
            "sub_id": sub.submission_id,
            "treasury_account": treas_accounts[0].treasury_account_identifier,
            "ob_incur": 20.5,
        },
        {
            "sub_id": sub.submission_id,
            "treasury_account": treas_accounts[1].treasury_account_identifier,
            "ob_incur": 29,
        },
        {
            "sub_id": sub.submission_id,
            "treasury_account": treas_accounts[1].treasury_account_identifier,
            "ob_incur": 13.3,
        },
    ]
    for ocpa in ocpas:
        mommy.make(
            "financial_activities.FinancialAccountsByProgramActivityObjectClass",
            submission_id=ocpa["sub_id"],
            treasury_account_id=ocpa["treasury_account"],
            obligations_incurred_by_program_object_class_cpe=ocpa["ob_incur"],
        )


def test_run_script(setup_test_data):
    """ Test that the populate_reporting_agency_tas script acts as expected """
    connection = get_connection(read_only=False)
    sql_path = str(settings.APP_DIR / "reporting/management/sql/populate_reporting_agency_tas.sql")

    with open(sql_path) as f:
        test_sql = f.read()

    # Executing the SQL and testing the entry with only one record for the period/fiscal year/tas per table
    with connection.cursor() as cursor:
        cursor.execute(test_sql)
        cursor.execute(
            """
            SELECT appropriation_obligated_amount,
                object_class_pa_obligated_amount,
                diff_approp_ocpa_obligated_amounts
            FROM reporting_agency_tas
            WHERE fiscal_period = 3 AND fiscal_year = 2019 AND tas_rendering_label = 'tas-1'
        """
        )
        results = cursor.fetchall()

    assert len(results) == 1
    for result in results:
        assert result[0] == 50
        assert result[1] == 20.5
        assert result[2] == 29.5

    # Testing an entry with multiple rows that roll up into a single period/fiscal year/tas
    with connection.cursor() as cursor:
        cursor.execute(
            """
                    SELECT appropriation_obligated_amount,
                        object_class_pa_obligated_amount,
                        diff_approp_ocpa_obligated_amounts
                    FROM reporting_agency_tas
                    WHERE fiscal_period = 3 AND fiscal_year = 2019 AND tas_rendering_label = 'tas-2'
                """
        )
        results = cursor.fetchall()

    assert len(results) == 1
    for result in results:
        assert result[0] == 41
        assert result[1] == Decimal("42.30")
        assert result[2] == Decimal("-1.30")
