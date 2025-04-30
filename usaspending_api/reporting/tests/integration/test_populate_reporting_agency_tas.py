import pytest

from datetime import datetime

from django.conf import settings
from model_bakery import baker

from usaspending_api.reporting.models import ReportingAgencyTas

from usaspending_api.common.helpers.sql_helpers import get_connection


@pytest.fixture
def setup_test_data(db):
    """Insert data into DB for testing"""
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
        baker.make("submissions.DABSSubmissionWindowSchedule", **dabs_window)

    sub = [
        baker.make(
            "submissions.SubmissionAttributes",
            submission_id=1,
            reporting_fiscal_year=2019,
            reporting_fiscal_period=3,
            submission_window_id=dsws[0]["id"],
        ),
        baker.make(
            "submissions.SubmissionAttributes",
            submission_id=2,
            reporting_fiscal_year=future_date.year,
            reporting_fiscal_quarter=1,
            reporting_fiscal_period=3,
            submission_window_id=dsws[1]["id"],
        ),
    ]

    agencies = [
        baker.make(
            "references.ToptierAgency", toptier_code="123", abbreviation="ABC", name="Test Agency", _fill_optional=True
        ),
        baker.make(
            "references.ToptierAgency",
            toptier_code="987",
            abbreviation="XYZ",
            name="Test Agency 2",
            _fill_optional=True,
        ),
    ]

    treas_accounts = [
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=1,
            funding_toptier_agency=agencies[0],
            tas_rendering_label="tas-1",
        ),
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            funding_toptier_agency=agencies[0],
            tas_rendering_label="tas-2",
        ),
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=3,
            funding_toptier_agency=agencies[1],
            tas_rendering_label="tas-3",
        ),
    ]
    approps = [
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[0], "ob_incur": 50},
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[1], "ob_incur": 12},
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[1], "ob_incur": 29},
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[2], "ob_incur": 15},
        {"sub_id": sub[1].submission_id, "treasury_account": treas_accounts[2], "ob_incur": 1000},
    ]
    for approp in approps:
        baker.make(
            "accounts.AppropriationAccountBalances",
            submission_id=approp["sub_id"],
            treasury_account_identifier=approp["treasury_account"],
            obligations_incurred_total_by_tas_cpe=approp["ob_incur"],
        )

    ocpas = [
        {
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[0].treasury_account_identifier,
            "ob_incur": -500,
            "deobligation": 100,
            "ussgl480110": 1000,
            "ussgl490110": 10000,
            "pya": "P",
        },
        {
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[0].treasury_account_identifier,
            "ob_incur": 20,
            "deobligation": 100,
            "ussgl480110": 1000,
            "ussgl490110": 10000,
            "pya": "X",
        },
        {
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[1].treasury_account_identifier,
            "ob_incur": 29,
            "deobligation": 100,
            "ussgl480110": 1000,
            "ussgl490110": 10000,
            "pya": "X",
        },
        {
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[1].treasury_account_identifier,
            "ob_incur": 13,
            "deobligation": 100,
            "ussgl480110": 1000,
            "ussgl490110": 10000,
            "pya": "X",
        },
        {
            "sub_id": sub[0].submission_id,
            "treasury_account": treas_accounts[2].treasury_account_identifier,
            "ob_incur": -5,
            "deobligation": 100,
            "ussgl480110": 1000,
            "ussgl490110": 10000,
            "pya": "X",
        },
        {
            "sub_id": sub[1].submission_id,
            "treasury_account": treas_accounts[2].treasury_account_identifier,
            "ob_incur": -500,
            "deobligation": 100,
            "ussgl480110": 1000,
            "ussgl490110": 10000,
            "pya": "X",
        },
    ]
    for ocpa in ocpas:
        baker.make(
            "financial_activities.FinancialAccountsByProgramActivityObjectClass",
            submission_id=ocpa["sub_id"],
            treasury_account_id=ocpa["treasury_account"],
            obligations_incurred_by_program_object_class_cpe=ocpa["ob_incur"],
            deobligations_recoveries_refund_pri_program_object_class_cpe=ocpa["deobligation"],
            prior_year_adjustment=ocpa["pya"],
            ussgl480110_rein_undel_ord_cpe=ocpa["ussgl480110"],
            ussgl490110_rein_deliv_ord_cpe=ocpa["ussgl490110"],
        )


def test_run_script(setup_test_data):
    """Test that the populate_reporting_agency_tas script acts as expected"""
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
    assert results[0].object_class_pa_obligated_amount == 20
    assert results[0].diff_approp_ocpa_obligated_amounts == 30

    # Testing an entry with multiple rows that roll up into a single period/fiscal year/tas
    results = ReportingAgencyTas.objects.filter(fiscal_year=2019, fiscal_period=3, tas_rendering_label="tas-2").all()

    assert len(results) == 1
    assert results[0].appropriation_obligated_amount == 41
    assert results[0].object_class_pa_obligated_amount == 42
    assert results[0].diff_approp_ocpa_obligated_amounts == -1

    # Making sure that 2 different agencies under the same year/period don't get rolled up together
    results = (
        ReportingAgencyTas.objects.filter(fiscal_year=2019, fiscal_period=3)
        .order_by("diff_approp_ocpa_obligated_amounts")
        .all()
    )

    assert len(results) == 3
    assert results[0].diff_approp_ocpa_obligated_amounts == -1
    assert results[1].diff_approp_ocpa_obligated_amounts == 20
    assert results[2].diff_approp_ocpa_obligated_amounts == 30
