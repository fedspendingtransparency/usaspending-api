import pytest

from decimal import Decimal
from model_bakery import baker

from django.core.management import call_command
from usaspending_api.reporting.models import ReportingAgencyOverview


@pytest.fixture
def setup_test_data(db):
    """Insert data into DB for testing"""

    dsws_dicts = [
        {
            "submission_fiscal_year": 2019,
            "submission_fiscal_quarter": 1,
            "submission_fiscal_month": 3,
            "submission_reveal_date": "2019-3-15",
            "is_quarter": False,
            "period_end_date": "2019-1-15",
        },
        {
            "submission_fiscal_year": 2020,
            "submission_fiscal_quarter": 1,
            "submission_fiscal_month": 3,
            "submission_reveal_date": "2019-3-09",
            "is_quarter": False,
            "period_end_date": "2020-01-09",
        },
    ]
    dsws = [baker.make("submissions.DABSSubmissionWindowSchedule", **dabs_window) for dabs_window in dsws_dicts]

    subs = [
        baker.make(
            "submissions.SubmissionAttributes",
            toptier_code="987",
            reporting_fiscal_year=2019,
            reporting_fiscal_quarter=1,
            reporting_fiscal_period=3,
            quarter_format_flag=True,
            submission_window=dsws[0],
        ),
        baker.make(
            "submissions.SubmissionAttributes",
            toptier_code="123",
            reporting_fiscal_year=2020,
            reporting_fiscal_quarter=1,
            reporting_fiscal_period=3,
            quarter_format_flag=True,
            submission_window=dsws[1],
        ),
    ]

    toptier_agencies = [
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

    agencies = [
        baker.make("references.Agency", toptier_agency=toptier_agencies[0], toptier_flag=True, _fill_optional=True),
        baker.make("references.Agency", toptier_agency=toptier_agencies[1], toptier_flag=True, _fill_optional=True),
    ]

    treas_accounts = [
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            funding_toptier_agency=toptier_agencies[0],
            tas_rendering_label="tas-1-overview",
        ),
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            funding_toptier_agency=toptier_agencies[0],
            tas_rendering_label="tas-2-overview",
        ),
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            funding_toptier_agency=toptier_agencies[1],
            tas_rendering_label="tas-3-overview",
        ),
    ]
    approps = [
        {"sub": subs[0], "treasury_account": treas_accounts[0], "total_resources": 50},
        {"sub": subs[0], "treasury_account": treas_accounts[1], "total_resources": 12},
        {"sub": subs[0], "treasury_account": treas_accounts[1], "total_resources": 29},
        {"sub": subs[0], "treasury_account": treas_accounts[2], "total_resources": 15.5},
    ]
    for approp in approps:
        baker.make(
            "accounts.AppropriationAccountBalances",
            submission=approp["sub"],
            treasury_account_identifier=approp["treasury_account"],
            total_budgetary_resources_amount_cpe=approp["total_resources"],
        )

    reporting_tases = [
        {
            "year": subs[0].reporting_fiscal_year,
            "period": subs[0].reporting_fiscal_period,
            "label": treas_accounts[0].tas_rendering_label,
            "toptier_code": toptier_agencies[0].toptier_code,
            "diff": 29.5,
        },
        {
            "year": subs[0].reporting_fiscal_year,
            "period": subs[0].reporting_fiscal_period,
            "label": treas_accounts[1].tas_rendering_label,
            "toptier_code": toptier_agencies[0].toptier_code,
            "diff": -1.3,
        },
        {
            "year": subs[0].reporting_fiscal_year,
            "period": subs[0].reporting_fiscal_period,
            "label": treas_accounts[2].tas_rendering_label,
            "toptier_code": toptier_agencies[1].toptier_code,
            "diff": 20.5,
        },
    ]
    for reporting_tas in reporting_tases:
        baker.make(
            "reporting.ReportingAgencyTas",
            fiscal_year=reporting_tas["year"],
            fiscal_period=reporting_tas["period"],
            tas_rendering_label=reporting_tas["label"],
            toptier_code=reporting_tas["toptier_code"],
            diff_approp_ocpa_obligated_amounts=reporting_tas["diff"],
        )

    gtas_sf133s = [
        {
            "year": subs[0].reporting_fiscal_year,
            "period": subs[0].reporting_fiscal_period,
            "tas_id": treas_accounts[0],
            "ob_incur": 4,
        },
        {
            "year": subs[0].reporting_fiscal_year,
            "period": subs[0].reporting_fiscal_period,
            "tas_id": treas_accounts[1],
            "ob_incur": 19.54,
        },
        {
            "year": subs[0].reporting_fiscal_year,
            "period": subs[0].reporting_fiscal_period,
            "tas_id": treas_accounts[2],
            "ob_incur": -12.4,
        },
        {
            "year": subs[0].reporting_fiscal_year,
            "period": subs[0].reporting_fiscal_period,
            "tas_id": treas_accounts[2],
            "ob_incur": 9.2,
        },
    ]
    for sf133 in gtas_sf133s:
        baker.make(
            "references.GTASSF133Balances",
            fiscal_year=sf133["year"],
            fiscal_period=sf133["period"],
            treasury_account_identifier=sf133["tas_id"],
            obligations_incurred_total_cpe=sf133["ob_incur"],
        )

    award_dicts = [
        {
            "award_id": 1,
            "is_fpds": True,
            "awarding_agency_id": agencies[0].id,
            "type": "A",
            "piid": "123",
            "certified_date": "2019-12-15",
        },
        {
            "award_id": 2,
            "is_fpds": False,
            "awarding_agency_id": agencies[0].id,
            "type": "08",
            "fain": "abc",
            "uri": "def",
            "certified_date": "2019-12-15",
            "total_subsidy_cost": 1000,
        },
        {
            "award_id": 3,
            "is_fpds": False,
            "awarding_agency_id": agencies[0].id,
            "type": "07",
            "fain": "abcdef",
            "certified_date": "2019-12-15",
            "total_subsidy_cost": 2000,
        },
    ]
    award_list = [baker.make("search.AwardSearch", **award) for award in award_dicts]
    transaction_list = [
        {
            "transaction_id": 1,
            "award": award_list[0],
            "fiscal_year": 2019,
            "action_date": "2018-11-15",
            "awarding_agency_id": agencies[0].id,
        },
        {
            "transaction_id": 2,
            "award": award_list[0],
            "fiscal_year": 2019,
            "action_date": "2018-12-15",
            "awarding_agency_id": agencies[0].id,
        },
        {
            "transaction_id": 3,
            "award": award_list[1],
            "fiscal_year": 2019,
            "action_date": "2018-10-15",
            "awarding_agency_id": agencies[0].id,
        },
        {
            "transaction_id": 4,
            "award": award_list[2],
            "fiscal_year": 2019,
            "action_date": "2018-10-28",
            "awarding_agency_id": agencies[0].id,
        },
    ]
    for transaction in transaction_list:
        baker.make("search.TransactionSearch", **transaction)

    faba_list = [
        {
            "award": award_list[0],
            "distinct_award_key": "123|||",
            "piid": award_list[0].piid,
            "fain": None,
            "uri": None,
            "submission": subs[0],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 1000,
        },
        {
            "award": award_list[0],
            "distinct_award_key": "123|||",
            "piid": award_list[0].piid,
            "fain": None,
            "uri": None,
            "submission": subs[0],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 2000,
        },
        {
            "distinct_award_key": "|456|789|",
            "piid": None,
            "fain": "456",
            "uri": "789",
            "submission": subs[0],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 3000,
        },
        {
            "distinct_award_key": "|456|789|",
            "piid": None,
            "fain": "456",
            "uri": "789",
            "submission": subs[0],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 4000,
        },
        {
            "distinct_award_key": "159|||",
            "piid": "159",
            "fain": None,
            "uri": None,
            "submission": subs[1],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 5000,
        },
    ]

    for faba in faba_list:
        baker.make("awards.FinancialAccountsByAwards", **faba)


def test_run_script(setup_test_data):
    """Test that the populate_reporting_agency_tas script acts as expected"""
    call_command("populate_reporting_agency_overview")

    results = ReportingAgencyOverview.objects.filter(fiscal_year=2019, fiscal_period=3, toptier_code="987").all()

    assert len(results) == 1
    assert results[0].total_dollars_obligated_gtas == Decimal("-3.2")
    assert results[0].total_budgetary_resources == 15.5
    assert results[0].total_diff_approp_ocpa_obligated_amounts == 20.5
    assert results[0].unlinked_procurement_c_awards == 0
    assert results[0].unlinked_assistance_c_awards == 0
    assert results[0].unlinked_procurement_d_awards == 0
    assert results[0].unlinked_assistance_d_awards == 0
    assert results[0].linked_procurement_awards == 0
    assert results[0].linked_assistance_awards == 0

    # Testing an agency with multiple rows in reporting_agency_tas that roll up into a single period/fiscal year
    results = ReportingAgencyOverview.objects.filter(fiscal_year=2019, fiscal_period=3, toptier_code="123").all()

    assert len(results) == 1
    assert results[0].total_dollars_obligated_gtas == Decimal("23.54")
    assert results[0].total_budgetary_resources == 91
    assert results[0].total_diff_approp_ocpa_obligated_amounts == Decimal("28.2")
    assert results[0].unlinked_procurement_c_awards == 0
    assert results[0].unlinked_assistance_c_awards == 1
    assert results[0].unlinked_procurement_d_awards == 0
    assert results[0].unlinked_assistance_d_awards == 2
    assert results[0].linked_procurement_awards == 1
    assert results[0].linked_assistance_awards == 0
