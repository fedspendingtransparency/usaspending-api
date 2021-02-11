import pytest

from datetime import datetime
from decimal import Decimal

from django.conf import settings
from model_mommy import mommy

from usaspending_api.reporting.models import ReportingAgencyTas

from usaspending_api.common.helpers.sql_helpers import get_connection


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    future_date = datetime(datetime.now().year + 1, 1, 19)
    dsws = [
        {
            "id": 1,
            "submission_fiscal_year": 2019,
            "submission_fiscal_quarter": 1,
            "submission_fiscal_month": 3,
            "submission_reveal_date": "2019-1-15",
        },
        {
            "id": 2,
            "submission_fiscal_year": future_date.year,
            "submission_fiscal_quarter": 1,
            "submission_fiscal_month": 3,
            "submission_reveal_date": datetime.strftime(future_date, "%Y-%m-%d"),
        },
    ]
    for dabs_window in dsws:
        mommy.make("submissions.DABSSubmissionWindowSchedule", **dabs_window)

    sub = [
        mommy.make(
            "submissions.SubmissionAttributes",
            submission_id=1,
            reporting_fiscal_year=2019,
            reporting_fiscal_period=3,
            submission_window_id=dsws[0]["id"],
        ),
        mommy.make(
            "submissions.SubmissionAttributes",
            submission_id=2,
            reporting_fiscal_year=future_date.year,
            reporting_fiscal_quarter=1,
            reporting_fiscal_period=3,
            submission_window_id=dsws[1]["id"],
        ),
    ]

    agencies = [
        mommy.make("references.ToptierAgency", toptier_code="123", abbreviation="ABC", name="Test Agency"),
        mommy.make("references.ToptierAgency", toptier_code="987", abbreviation="XYZ", name="Test Agency 2"),
    ]

    treas_accounts = [
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=1,
            funding_toptier_agency_id=agencies[0].toptier_agency_id,
            tas_rendering_label="tas-1",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            funding_toptier_agency_id=agencies[0].toptier_agency_id,
            tas_rendering_label="tas-2",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=3,
            funding_toptier_agency_id=agencies[1].toptier_agency_id,
            tas_rendering_label="tas-3",
        ),
    ]
    approps = [
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[0], "ob_incur": 50},
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[1], "ob_incur": 12},
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[1], "ob_incur": 29},
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[2], "ob_incur": 15.5},
        {"sub_id": sub[1].submission_id, "treasury_account": treas_accounts[2], "ob_incur": 1000},
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
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[0].treasury_account_identifier,
            "ob_incur": 20.5,
        },
        {
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[1].treasury_account_identifier,
            "ob_incur": 29,
        },
        {
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[1].treasury_account_identifier,
            "ob_incur": 13.3,
        },
        {
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[2].treasury_account_identifier,
            "ob_incur": -5,
        },
        {
            "sub_id": sub[1].submission_id,
            "treasury_account": treas_accounts[2].treasury_account_identifier,
            "ob_incur": -500,
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
    sql_path = settings.APP_DIR / "reporting" / "management" / "sql" / "populate_reporting_agency_tas.sql"
    test_sql = sql_path.read_text()

    # Executing the SQL and testing the entry with only one record for the period/fiscal year/tas per table
    with connection.cursor() as cursor:
        cursor.execute(test_sql)

    results = ReportingAgencyTas.objects.all()
    assert len(results) == 3

    results = ReportingAgencyTas.objects.filter(fiscal_year=2019, fiscal_period=3, tas_rendering_label="tas-1").all()

    assert len(results) == 1
    assert results[0].appropriation_obligated_amount == 50
    assert results[0].object_class_pa_obligated_amount == 20.5
    assert results[0].diff_approp_ocpa_obligated_amounts == 29.5

    # Testing an entry with multiple rows that roll up into a single period/fiscal year/tas
    results = ReportingAgencyTas.objects.filter(fiscal_year=2019, fiscal_period=3, tas_rendering_label="tas-2").all()

    assert len(results) == 1
    assert results[0].appropriation_obligated_amount == 41
    assert results[0].object_class_pa_obligated_amount == Decimal("42.30")
    assert results[0].diff_approp_ocpa_obligated_amounts == Decimal("-1.30")

    # Making sure that 2 different agencies under the same year/period don't get rolled up together
    results = (
        ReportingAgencyTas.objects.filter(fiscal_year=2019, fiscal_period=3)
        .order_by("diff_approp_ocpa_obligated_amounts")
        .all()
    )

    assert len(results) == 3
    assert results[0].diff_approp_ocpa_obligated_amounts == Decimal("-1.30")
    assert results[1].diff_approp_ocpa_obligated_amounts == 20.5
    assert results[2].diff_approp_ocpa_obligated_amounts == 29.5
