import pytest

from decimal import Decimal

from django.conf import settings
from model_mommy import mommy

from usaspending_api.reporting.models import ReportingAgencyOverview

from usaspending_api.common.helpers.sql_helpers import get_connection


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    sub = mommy.make(
        "submissions.SubmissionAttributes", submission_id=1, reporting_fiscal_year=2019, reporting_fiscal_period=3
    )
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
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[0], "total_resources": 50},
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[1], "total_resources": 12},
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[1], "total_resources": 29},
        {"sub_id": sub.submission_id, "treasury_account": treas_accounts[2], "total_resources": 15.5},
    ]
    for approp in approps:
        mommy.make(
            "accounts.AppropriationAccountBalances",
            submission_id=approp["sub_id"],
            treasury_account_identifier=approp["treasury_account"],
            total_budgetary_resources_amount_cpe=approp["total_resources"],
        )

    reporting_tases = [
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "label": treas_accounts[0].tas_rendering_label,
            "toptier_code": agencies[0].toptier_code,
            "diff": 29.5,
        },
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "label": treas_accounts[1].tas_rendering_label,
            "toptier_code": agencies[0].toptier_code,
            "diff": -1.3,
        },
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "label": treas_accounts[2].tas_rendering_label,
            "toptier_code": agencies[1].toptier_code,
            "diff": 20.5,
        },
    ]
    for reporting_tas in reporting_tases:
        mommy.make(
            "reporting.ReportingAgencyTas",
            fiscal_year=reporting_tas["year"],
            fiscal_period=reporting_tas["period"],
            tas_rendering_label=reporting_tas["label"],
            toptier_code=reporting_tas["toptier_code"],
            diff_approp_ocpa_obligated_amounts=reporting_tas["diff"],
        )

    gtas_sf133s = [
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "tas_id": treas_accounts[0],
            "ob_incur": 4,
        },
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "tas_id": treas_accounts[1],
            "ob_incur": 19.54,
        },
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "tas_id": treas_accounts[2],
            "ob_incur": -12.4,
        },
        {
            "year": sub.reporting_fiscal_year,
            "period": sub.reporting_fiscal_period,
            "tas_id": treas_accounts[2],
            "ob_incur": 9.2,
        },
    ]
    for sf133 in gtas_sf133s:
        mommy.make(
            "references.GTASSF133Balances",
            fiscal_year=sf133["year"],
            fiscal_period=sf133["period"],
            treasury_account_identifier=sf133["tas_id"],
            obligations_incurred_total_cpe=sf133["ob_incur"],
        )


def test_run_script(setup_test_data):
    """ Test that the populate_reporting_agency_tas script acts as expected """
    connection = get_connection(read_only=False)
    sql_path = settings.APP_DIR / "reporting" / "management" / "sql" / "populate_reporting_agency_overview.sql"
    test_sql = sql_path.read_text()

    # Executing the SQL and testing the entry with only one record for the period/fiscal year in reporting_agency_tas
    with connection.cursor() as cursor:
        cursor.execute(test_sql)

    results = ReportingAgencyOverview.objects.filter(fiscal_year=2019, fiscal_period=3, toptier_code="987").all()

    assert len(results) == 1
    assert results[0].total_dollars_obligated_gtas == Decimal("-3.2")
    assert results[0].total_budgetary_resources == 15.5
    assert results[0].total_diff_approp_ocpa_obligated_amounts == 20.5

    # Testing an agency with multiple rows in reporting_agency_tas that roll up into a single period/fiscal year
    results = ReportingAgencyOverview.objects.filter(fiscal_year=2019, fiscal_period=3, toptier_code="123").all()

    assert len(results) == 1
    assert results[0].total_dollars_obligated_gtas == Decimal("23.54")
    assert results[0].total_budgetary_resources == 91
    assert results[0].total_diff_approp_ocpa_obligated_amounts == Decimal("28.2")
