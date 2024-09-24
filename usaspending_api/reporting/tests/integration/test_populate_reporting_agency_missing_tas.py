import pytest

from datetime import datetime

from django.conf import settings
from django.db import connection
from model_bakery import baker

from usaspending_api.reporting.models import ReportingAgencyMissingTas


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
        {
            "id": 3,
            "submission_fiscal_year": 2020,
            "submission_fiscal_quarter": 4,
            "submission_fiscal_month": 11,
            "submission_reveal_date": "2020-09-30",
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
            reporting_fiscal_year=2019,
            reporting_fiscal_period=4,
            submission_window_id=dsws[0]["id"],
        ),
        baker.make(
            "submissions.SubmissionAttributes",
            submission_id=3,
            reporting_fiscal_year=future_date.year,
            reporting_fiscal_quarter=1,
            reporting_fiscal_period=3,
            submission_window_id=dsws[1]["id"],
        ),
    ]

    agency = baker.make("references.ToptierAgency", toptier_agency_id=1, toptier_code="123", _fill_optional=True)

    treas_accounts = [
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=1,
            funding_toptier_agency=agency,
            tas_rendering_label="tas-1",
        ),
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            funding_toptier_agency=agency,
            tas_rendering_label="tas-2",
        ),
    ]

    approps = [
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[0]},
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[1]},
        {"sub_id": sub[1].submission_id, "treasury_account": treas_accounts[1]},
        {"sub_id": sub[2].submission_id, "treasury_account": treas_accounts[1]},
    ]
    for approp in approps:
        baker.make(
            "accounts.AppropriationAccountBalances",
            submission_id=approp["sub_id"],
            treasury_account_identifier=approp["treasury_account"],
        )

    gtas_rows = [
        {
            "treasury_account_identifier": approps[0]["treasury_account"],
            "fiscal_year": 2019,
            "fiscal_period": 3,
            "obligations_incurred_total_cpe": 1,
        },
        {
            "treasury_account_identifier": approps[1]["treasury_account"],
            "fiscal_year": 2019,
            "fiscal_period": 4,
            "obligations_incurred_total_cpe": 10,
        },
        {
            "treasury_account_identifier": approps[0]["treasury_account"],
            "fiscal_year": 2019,
            "fiscal_period": 5,
            "obligations_incurred_total_cpe": 100,
        },
        {
            "treasury_account_identifier": approps[0]["treasury_account"],
            "fiscal_year": 2020,
            "fiscal_period": 3,
            "obligations_incurred_total_cpe": 1000,
        },
        {
            "treasury_account_identifier": approps[1]["treasury_account"],
            "fiscal_year": 2020,
            "fiscal_period": 3,
            "obligations_incurred_total_cpe": 10000,
        },
        {
            "treasury_account_identifier": approps[1]["treasury_account"],
            "fiscal_year": 2020,
            "fiscal_period": 3,
            "obligations_incurred_total_cpe": 100000,
        },
        {
            "treasury_account_identifier": approps[3]["treasury_account"],
            "fiscal_year": future_date.year,
            "fiscal_period": 3,
            "obligations_incurred_total_cpe": 1000000,
        },
    ]
    for gtas in gtas_rows:
        baker.make(
            "references.GTASSF133Balances",
            treasury_account_identifier=gtas["treasury_account_identifier"],
            fiscal_year=gtas["fiscal_year"],
            fiscal_period=gtas["fiscal_period"],
            obligations_incurred_total_cpe=gtas["obligations_incurred_total_cpe"],
        )


def test_run_script(setup_test_data):
    """Test that the populate_reporting_agency_missing_tas script acts as expected"""
    sql_path = settings.APP_DIR / "reporting" / "management" / "sql" / "populate_reporting_agency_missing_tas.sql"

    with open(sql_path) as f:
        test_sql = f.read()

    with connection.cursor() as cursor:
        cursor.execute(test_sql)
    results = ReportingAgencyMissingTas.objects.filter().all()

    # Expected results: GTAS rows 3, 4 and 5 & 6 summed
    assert len(results) == 3

    assert results[0].toptier_code == "123"
    assert results[0].fiscal_year == 2019
    assert results[0].fiscal_period == 5
    assert results[0].tas_rendering_label == "tas-1"
    assert results[0].obligated_amount == 100

    assert results[1].toptier_code == "123"
    assert results[1].fiscal_year == 2020
    assert results[1].fiscal_period == 3
    assert results[1].tas_rendering_label == "tas-1"
    assert results[1].obligated_amount == 1000

    assert results[2].toptier_code == "123"
    assert results[2].fiscal_year == 2020
    assert results[2].fiscal_period == 3
    assert results[2].tas_rendering_label == "tas-2"
    assert results[2].obligated_amount == 110000
