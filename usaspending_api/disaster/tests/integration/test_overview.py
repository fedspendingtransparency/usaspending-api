import pytest

from decimal import Decimal

from model_bakery import baker

from usaspending_api.disaster.tests.fixtures.overview_data import (
    EARLY_MONTH,
    LATE_MONTH,
    EARLY_YEAR,
    LATE_YEAR,
    LATE_GTAS_CALCULATIONS,
    QUARTERLY_GTAS_CALCULATIONS,
    EARLY_GTAS_CALCULATIONS,
    YEAR_2_GTAS_CALCULATIONS,
    OTHER_BUDGET_AUTHORITY_GTAS_CALCULATIONS,
)

OVERVIEW_URL = "/api/v2/disaster/overview/"


@pytest.mark.django_db
def test_basic_data_set(client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, basic_faba):
    helpers.patch_datetime_now(monkeypatch, EARLY_YEAR, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data == {
        "funding": [{"amount": EARLY_GTAS_CALCULATIONS["total_budgetary_resources"], "def_code": "M"}],
        "total_budget_authority": EARLY_GTAS_CALCULATIONS["total_budgetary_resources"],
        "spending": {
            "award_obligations": Decimal("0.0"),
            "award_outlays": Decimal("0"),
            "total_obligations": EARLY_GTAS_CALCULATIONS["total_obligations"],
            "total_outlays": EARLY_GTAS_CALCULATIONS["total_outlays"],
        },
        "additional": None,
    }


@pytest.mark.django_db
def test_using_only_latest_gtas(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, early_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, EARLY_YEAR, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": Decimal("0.3"), "def_code": "M"}]
    assert resp.data["total_budget_authority"] == LATE_GTAS_CALCULATIONS["total_budgetary_resources"]
    assert resp.data["spending"]["total_obligations"] == LATE_GTAS_CALCULATIONS["total_obligations"]
    assert resp.data["spending"]["total_outlays"] == LATE_GTAS_CALCULATIONS["total_outlays"]


@pytest.mark.django_db
def test_total_obligations(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, unobligated_balance_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, EARLY_YEAR, LATE_MONTH, 25)
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["total_obligations"] == Decimal("0.0")


@pytest.mark.django_db
def test_exclude_gtas_for_incompleted_period(
    client, monkeypatch, helpers, defc_codes, partially_completed_year, late_gtas, early_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, EARLY_YEAR, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": EARLY_GTAS_CALCULATIONS["total_budgetary_resources"], "def_code": "M"}]
    assert resp.data["total_budget_authority"] == EARLY_GTAS_CALCULATIONS["total_budgetary_resources"]
    assert resp.data["spending"]["total_obligations"] == EARLY_GTAS_CALCULATIONS["total_obligations"]
    assert resp.data["spending"]["total_outlays"] == EARLY_GTAS_CALCULATIONS["total_outlays"]


@pytest.mark.django_db
def test_exclude_non_selected_defc_for_gtas(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, year_2_gtas_non_covid, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, LATE_YEAR, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL + "?def_codes=M,N")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.0")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.0")

    resp = client.get(OVERVIEW_URL + "?def_codes=M,A")
    assert resp.data["spending"]["total_obligations"] == YEAR_2_GTAS_CALCULATIONS["total_obligations"]
    assert resp.data["spending"]["total_outlays"] == YEAR_2_GTAS_CALCULATIONS["total_outlays"]


@pytest.mark.django_db
def test_summing_multiple_years(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, early_gtas, year_2_gtas_covid, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, LATE_YEAR, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [
        {
            "amount": LATE_GTAS_CALCULATIONS["total_budgetary_resources"]
            + YEAR_2_GTAS_CALCULATIONS["total_budgetary_resources"],
            "def_code": "M",
        }
    ]
    assert (
        resp.data["total_budget_authority"]
        == LATE_GTAS_CALCULATIONS["total_budgetary_resources"] + YEAR_2_GTAS_CALCULATIONS["total_budgetary_resources"]
    )
    assert (
        resp.data["spending"]["total_obligations"]
        == LATE_GTAS_CALCULATIONS["total_obligations"] + YEAR_2_GTAS_CALCULATIONS["total_obligations"]
    )
    assert (
        resp.data["spending"]["total_outlays"]
        == LATE_GTAS_CALCULATIONS["total_outlays"] + YEAR_2_GTAS_CALCULATIONS["total_outlays"]
    )


@pytest.mark.django_db
def test_summing_period_and_quarterly_in_same_year(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, quarterly_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, EARLY_YEAR, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [
        {
            "amount": LATE_GTAS_CALCULATIONS["total_budgetary_resources"]
            + QUARTERLY_GTAS_CALCULATIONS["total_budgetary_resources"],
            "def_code": "M",
        }
    ]
    assert (
        resp.data["total_budget_authority"]
        == LATE_GTAS_CALCULATIONS["total_budgetary_resources"]
        + QUARTERLY_GTAS_CALCULATIONS["total_budgetary_resources"]
    )


@pytest.mark.django_db
def test_ignore_gtas_not_yet_revealed(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, early_gtas, year_2_gtas_covid, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, LATE_YEAR, EARLY_MONTH - 1, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": LATE_GTAS_CALCULATIONS["total_budgetary_resources"], "def_code": "M"}]
    assert resp.data["total_budget_authority"] == LATE_GTAS_CALCULATIONS["total_budgetary_resources"]
    assert resp.data["spending"]["total_obligations"] == LATE_GTAS_CALCULATIONS["total_obligations"]
    assert resp.data["spending"]["total_outlays"] == LATE_GTAS_CALCULATIONS["total_outlays"]


@pytest.mark.django_db
def test_ignore_funding_for_unselected_defc(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, year_2_gtas_covid, year_2_gtas_covid_2
):
    helpers.patch_datetime_now(monkeypatch, LATE_YEAR, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL + "?def_codes=M,A")
    assert resp.data["funding"] == [{"amount": YEAR_2_GTAS_CALCULATIONS["total_budgetary_resources"], "def_code": "M"}]
    assert resp.data["total_budget_authority"] == YEAR_2_GTAS_CALCULATIONS["total_budgetary_resources"]
    assert resp.data["spending"]["total_obligations"] == YEAR_2_GTAS_CALCULATIONS["total_obligations"]
    assert resp.data["spending"]["total_outlays"] == YEAR_2_GTAS_CALCULATIONS["total_outlays"]

    resp = client.get(OVERVIEW_URL + "?def_codes=M,N")
    assert resp.data["funding"] == [
        {"amount": YEAR_2_GTAS_CALCULATIONS["total_budgetary_resources"], "def_code": "M"},
        {"amount": YEAR_2_GTAS_CALCULATIONS["total_budgetary_resources"], "def_code": "N"},
    ]
    assert resp.data["total_budget_authority"] == YEAR_2_GTAS_CALCULATIONS["total_budgetary_resources"] * 2
    assert resp.data["spending"]["total_obligations"] == YEAR_2_GTAS_CALCULATIONS["total_obligations"] * 2
    assert resp.data["spending"]["total_outlays"] == YEAR_2_GTAS_CALCULATIONS["total_outlays"] * 2


@pytest.mark.django_db
def test_adds_budget_values(client, monkeypatch, helpers, defc_codes, basic_ref_data, other_budget_authority_gtas):
    helpers.patch_datetime_now(monkeypatch, EARLY_YEAR, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [
        {"amount": OTHER_BUDGET_AUTHORITY_GTAS_CALCULATIONS["total_budgetary_resources"], "def_code": "M"}
    ]
    assert resp.data["total_budget_authority"] == OTHER_BUDGET_AUTHORITY_GTAS_CALCULATIONS["total_budgetary_resources"]
    assert resp.data["spending"]["total_obligations"] == OTHER_BUDGET_AUTHORITY_GTAS_CALCULATIONS["total_obligations"]
    assert resp.data["spending"]["total_outlays"] == OTHER_BUDGET_AUTHORITY_GTAS_CALCULATIONS["total_outlays"]


@pytest.mark.django_db
def test_award_obligation_adds_values(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, faba_with_values
):
    helpers.patch_datetime_now(monkeypatch, EARLY_YEAR, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["award_obligations"] == Decimal("2.3")
    assert resp.data["spending"]["award_outlays"] == Decimal("1.15")


@pytest.mark.django_db
def test_faba_excludes_non_selected_defc(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, faba_with_non_covid_values
):
    helpers.patch_datetime_now(monkeypatch, EARLY_YEAR, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL + "?def_codes=M,N")
    assert resp.data["spending"]["award_obligations"] == Decimal("1.6")
    assert resp.data["spending"]["award_outlays"] == Decimal("0.8")


@pytest.mark.django_db
def test_award_outlays_sum_multiple_years(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, multi_year_faba
):
    helpers.patch_datetime_now(monkeypatch, LATE_YEAR, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["award_outlays"] == Decimal("1.15")


@pytest.mark.django_db
def test_award_outlays_doesnt_sum_multiple_periods(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, multi_period_faba
):
    helpers.patch_datetime_now(monkeypatch, LATE_YEAR, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["award_outlays"] == Decimal("0.8")


@pytest.mark.django_db
def test_award_outlays_ignores_future_faba(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, multi_period_faba_with_future
):
    helpers.patch_datetime_now(monkeypatch, LATE_YEAR, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["award_outlays"] == Decimal("0.35")


@pytest.mark.django_db
def test_dol_defc_v_special_case(client, monkeypatch, helpers, defc_codes, basic_ref_data):
    helpers.patch_datetime_now(monkeypatch, 2022, 6, 1)
    helpers.reset_dabs_cache()
    fta = baker.make("references.ToptierAgency", abbreviation="DOL", _fill_optional=True)
    taa = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=fta)

    def _gtas_values(multiplier):
        return {
            # Constants
            "treasury_account_identifier": taa,
            "disaster_emergency_fund_id": "O",
            "tas_rendering_label": "016-X-0168-000",
            # Values with multiplier
            "obligations_incurred_total_cpe": 10000000 * multiplier,
            "gross_outlay_amount_by_tas_cpe": 1000000 * multiplier,
            "total_budgetary_resources_cpe": 100000 * multiplier,
            "budget_authority_unobligated_balance_brought_forward_cpe": 1 * multiplier,
            "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": 10 * multiplier,
            "prior_year_paid_obligation_recoveries": 100 * multiplier,
            "anticipated_prior_year_obligation_recoveries": 1000 * multiplier,
        }

    baker.make("references.GTASSF133Balances", fiscal_year=2020, fiscal_period=12, **_gtas_values(1))
    baker.make("references.GTASSF133Balances", fiscal_year=2021, fiscal_period=6, **_gtas_values(2))
    baker.make("references.GTASSF133Balances", fiscal_year=2021, fiscal_period=12, **_gtas_values(3))
    baker.make("references.GTASSF133Balances", fiscal_year=2022, fiscal_period=5, **_gtas_values(4))
    defc_v_values = _gtas_values(6)
    defc_v_values.update(
        {"disaster_emergency_fund_id": "V", "treasury_account_identifier": None, "tas_rendering_label": None}
    )
    baker.make("references.GTASSF133Balances", fiscal_year=2022, fiscal_period=5, **defc_v_values)
    resp = client.get(f"{OVERVIEW_URL}?def_codes=V")
    assert resp.data == {
        "funding": [{"amount": 599334.0, "def_code": "V"}],
        "total_budget_authority": 599334.0,
        "spending": {
            "award_obligations": 0.0,
            "award_outlays": 0.0,
            "total_obligations": 59999940.0,
            "total_outlays": 5994000.0,
        },
        "additional": {
            "total_budget_authority": 49999950.0,
            "spending": {"total_obligations": 49999950.0, "total_outlays": 4995000.0},
        },
    }
