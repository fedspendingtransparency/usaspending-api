import pytest

from decimal import Decimal
from model_mommy import mommy

from django.core.management import call_command
from usaspending_api.reporting.models import ReportingAgencyOverview


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    subs = [
        mommy.make(
            "submissions.SubmissionAttributes",
            submission_id=1,
            reporting_fiscal_year=2019,
            reporting_fiscal_quarter=1,
            reporting_fiscal_period=3,
            quarter_format_flag=True,
        )
    ]

    mommy.make("submissions.DABSSubmissionWindowSchedule", id=subs[0].submission_id, submission_reveal_date="2019-1-15")

    toptier_agencies = [
        mommy.make("references.ToptierAgency", toptier_code="123", abbreviation="ABC", name="Test Agency"),
        mommy.make("references.ToptierAgency", toptier_code="987", abbreviation="XYZ", name="Test Agency 2"),
    ]

    agencies = [
        mommy.make("references.Agency", id=1, toptier_agency=toptier_agencies[0]),
        mommy.make("references.Agency", id=2, toptier_agency=toptier_agencies[1]),
    ]

    treas_accounts = [
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=1,
            funding_toptier_agency_id=toptier_agencies[0].toptier_agency_id,
            tas_rendering_label="tas-1-overview",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            funding_toptier_agency_id=toptier_agencies[0].toptier_agency_id,
            tas_rendering_label="tas-2-overview",
        ),
        mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=3,
            funding_toptier_agency_id=toptier_agencies[1].toptier_agency_id,
            tas_rendering_label="tas-3-overview",
        ),
    ]
    approps = [
        {"sub_id": subs[0].submission_id, "treasury_account": treas_accounts[0], "total_resources": 50},
        {"sub_id": subs[0].submission_id, "treasury_account": treas_accounts[1], "total_resources": 12},
        {"sub_id": subs[0].submission_id, "treasury_account": treas_accounts[1], "total_resources": 29},
        {"sub_id": subs[0].submission_id, "treasury_account": treas_accounts[2], "total_resources": 15.5},
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
        mommy.make(
            "references.GTASSF133Balances",
            fiscal_year=sf133["year"],
            fiscal_period=sf133["period"],
            treasury_account_identifier=sf133["tas_id"],
            obligations_incurred_total_cpe=sf133["ob_incur"],
        )

    award_list = [
        {
            "id": 1,
            "is_fpds": True,
            "funding_agency": agencies[0],
            "type": "A",
            "piid": "123",
            "certified_date": "2019-12-15",
        },
        {
            "id": 2,
            "is_fpds": False,
            "funding_agency": agencies[0],
            "type": "08",
            "fain": "abc",
            "uri": "def",
            "certified_date": "2019-12-15",
            "total_subsidy_cost": 1000,
        },
        {
            "id": 3,
            "is_fpds": False,
            "funding_agency": agencies[0],
            "type": "07",
            "fain": "abcdef",
            "certified_date": "2019-12-15",
            "total_subsidy_cost": 2000,
        },
    ]
    for award in award_list:
        mommy.make("awards.Award", **award)

    transaction_list = [
        {
            "id": 1,
            "award_id": award_list[0]["id"],
            "fiscal_year": 2019,
            "action_date": "2018-11-15",
            "funding_agency": agencies[0],
        },
        {
            "id": 2,
            "award_id": award_list[0]["id"],
            "fiscal_year": 2019,
            "action_date": "2018-12-15",
            "funding_agency": agencies[0],
        },
        {
            "id": 3,
            "award_id": award_list[1]["id"],
            "fiscal_year": 2019,
            "action_date": "2018-10-15",
            "funding_agency": agencies[0],
        },
        {
            "id": 4,
            "award_id": award_list[2]["id"],
            "fiscal_year": 2019,
            "action_date": "2018-10-28",
            "funding_agency": agencies[0],
        },
    ]
    for transaction in transaction_list:
        mommy.make("awards.TransactionNormalized", **transaction)

    faba_list = [
        {
            "award_id": award_list[0]["id"],
            "distinct_award_key": "123|||",
            "piid": award_list[0]["piid"],
            "fain": None,
            "uri": None,
            "submission": subs[0],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 1000,
        },
        {
            "award_id": award_list[0]["id"],
            "distinct_award_key": "123|||",
            "piid": award_list[0]["piid"],
            "fain": None,
            "uri": None,
            "submission": subs[0],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 2000,
        },
        {
            "award_id": None,
            "distinct_award_key": "|456|789|",
            "piid": None,
            "fain": "456",
            "uri": "789",
            "submission": subs[0],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 3000,
        },
        {
            "award_id": None,
            "distinct_award_key": "|456|789|",
            "piid": None,
            "fain": "456",
            "uri": "789",
            "submission": subs[0],
            "treasury_account": treas_accounts[0],
            "transaction_obligated_amount": 4000,
        },
    ]

    for faba in faba_list:
        mommy.make("awards.FinancialAccountsByAwards", **faba)


def test_run_script(setup_test_data):
    """ Test that the populate_reporting_agency_tas script acts as expected """
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
