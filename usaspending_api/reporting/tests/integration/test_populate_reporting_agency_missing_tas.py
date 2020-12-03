import pytest
from decimal import Decimal

from django.conf import settings
from django.db import connection
from model_mommy import mommy


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    sub = [
        mommy.make(
            "submissions.SubmissionAttributes", submission_id=1, reporting_fiscal_year=2019, reporting_fiscal_period=3
        ),
        mommy.make(
            "submissions.SubmissionAttributes", submission_id=2, reporting_fiscal_year=2019, reporting_fiscal_period=4
        ),
    ]
    agency = mommy.make("references.ToptierAgency", toptier_agency_id=1, toptier_code="123")

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
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[0]},
        {"sub_id": sub[0].submission_id, "treasury_account": treas_accounts[1]},
        {"sub_id": sub[1].submission_id, "treasury_account": treas_accounts[1]},
    ]
    for approp in approps:
        mommy.make(
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
            "obligations_incurred_total_cpe": 2,
        },
        {
            "treasury_account_identifier": approps[0]["treasury_account"],
            "fiscal_year": 2019,
            "fiscal_period": 5,
            "obligations_incurred_total_cpe": 3,
        },
        {
            "treasury_account_identifier": approps[0]["treasury_account"],
            "fiscal_year": 2020,
            "fiscal_period": 3,
            "obligations_incurred_total_cpe": 4,
        },
        {
            "treasury_account_identifier": approps[1]["treasury_account"],
            "fiscal_year": 2020,
            "fiscal_period": 3,
            "obligations_incurred_total_cpe": 5,
        },
        {
            "treasury_account_identifier": approps[1]["treasury_account"],
            "fiscal_year": 2020,
            "fiscal_period": 3,
            "obligations_incurred_total_cpe": 6,
        },
    ]
    for gtas in gtas_rows:
        mommy.make(
            "references.GTASSF133Balances",
            treasury_account_identifier=gtas["treasury_account_identifier"],
            fiscal_year=gtas["fiscal_year"],
            fiscal_period=gtas["fiscal_period"],
            obligations_incurred_total_cpe=gtas["obligations_incurred_total_cpe"],
        )


def test_run_script(setup_test_data):
    """ Test that the populate_reporting_agency_missing_tas script acts as expected """
    sql_path = str(settings.APP_DIR / "reporting/management/sql/populate_reporting_agency_missing_tas.sql")

    with open(sql_path) as f:
        test_sql = f.read()

    with connection.cursor() as cursor:
        cursor.execute(test_sql)
        cursor.execute("SELECT * FROM reporting_agency_missing_tas ORDER BY fiscal_year, fiscal_period, toptier_code")
        results = cursor.fetchall()

    # Expected results: GTAS rows 3, 4 and 5 & 6 summed
    assert len(results) == 3

    assert results[0][1] == "123"
    assert results[0][2] == 2019
    assert results[0][3] == 5
    assert results[0][4] == "tas-1"
    assert results[0][5] == Decimal("3")

    assert results[1][1] == "123"
    assert results[1][2] == 2020
    assert results[1][3] == 3
    assert results[1][4] == "tas-1"
    assert results[1][5] == Decimal("4")

    assert results[2][1] == "123"
    assert results[2][2] == 2020
    assert results[2][3] == 3
    assert results[2][4] == "tas-2"
    assert results[2][5] == Decimal("11")
