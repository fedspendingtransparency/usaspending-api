import pytest

from decimal import Decimal
from usaspending_api.disaster.tests.fixtures.overview_data import EARLY_MONTH, LATE_MONTH

OVERVIEW_URL = "/api/v2/disaster/overview/"

BASIC_FUNDING = [{"amount": Decimal("0.20"), "def_code": "M"}]


@pytest.mark.django_db
def test_basic_data_set(client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, basic_faba):
    helpers.patch_datetime_now(monkeypatch, 2021, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data == {
        "funding": BASIC_FUNDING,
        "total_budget_authority": Decimal("0.20"),
        "spending": {
            "award_obligations": Decimal("0.0"),
            "award_outlays": Decimal("0"),
            "total_obligations": Decimal("0.2"),
            "total_outlays": Decimal("0.02"),
        },
    }


@pytest.mark.django_db
def test_using_only_latest_gtas(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, early_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2021, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": Decimal("0.3"), "def_code": "M"}]
    assert resp.data["total_budget_authority"] == Decimal("0.3")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.3")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.03")


@pytest.mark.django_db
def test_total_obligations(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, unobligated_balance_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2021, LATE_MONTH, 25)
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["total_obligations"] == Decimal("0.0")


@pytest.mark.django_db
def test_total_obligation_only_uses_current_year(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, unobligated_balance_gtas, year_2_gtas_covid, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2022, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["total_obligations"] == Decimal("1.72")


@pytest.mark.django_db
def test_exclude_gtas_for_incompleted_period(
    client, monkeypatch, helpers, defc_codes, partially_completed_year, late_gtas, early_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2021, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": Decimal("0.2"), "def_code": "M"}]
    assert resp.data["total_budget_authority"] == Decimal("0.2")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.2")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.02")


@pytest.mark.django_db
def test_exclude_non_selected_defc_for_gtas(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, year_2_gtas_non_covid, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2022, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL + "?def_codes=M,N")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.0")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.00")

    resp = client.get(OVERVIEW_URL + "?def_codes=M,A")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.22")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.07")


@pytest.mark.django_db
def test_summing_multiple_years(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, early_gtas, year_2_gtas_covid, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2022, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": Decimal("0.62"), "def_code": "M"}]
    assert resp.data["total_budget_authority"] == Decimal("0.62")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.52")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.1")


@pytest.mark.django_db
def test_summing_period_and_quarterly_in_same_year(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, quarterly_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2021, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": Decimal("0.30"), "def_code": "M"}]
    assert resp.data["total_budget_authority"] == Decimal("0.30")


@pytest.mark.django_db
def test_ignore_gtas_not_yet_revealed(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, early_gtas, year_2_gtas_covid, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2022, EARLY_MONTH - 1, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": Decimal("0.3"), "def_code": "M"}]
    assert resp.data["total_budget_authority"] == Decimal("0.3")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.3")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.03")


@pytest.mark.django_db
def test_ignore_funding_for_unselected_defc(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, year_2_gtas_covid, year_2_gtas_covid_2
):
    helpers.patch_datetime_now(monkeypatch, 2022, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL + "?def_codes=M,A")
    assert resp.data["funding"] == [{"amount": Decimal("0.32"), "def_code": "M"}]
    assert resp.data["total_budget_authority"] == Decimal("0.32")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.22")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.07")

    resp = client.get(OVERVIEW_URL + "?def_codes=M,N")
    assert resp.data["funding"] == [
        {"amount": Decimal("0.32"), "def_code": "M"},
        {"amount": Decimal("0.32"), "def_code": "N"},
    ]
    assert resp.data["total_budget_authority"] == Decimal("0.64")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.44")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.14")


@pytest.mark.django_db
def test_isolate_defc(client, monkeypatch, helpers, defc_codes, basic_ref_data, year_2_gtas_covid, year_2_gtas_covid_2):
    helpers.patch_datetime_now(monkeypatch, 2022, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [
        {"amount": Decimal("0.32"), "def_code": "M"},
        {"amount": Decimal("0.32"), "def_code": "N"},
    ]
    assert resp.data["total_budget_authority"] == Decimal("0.64")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.44")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.14")


@pytest.mark.django_db
def test_adds_budget_values(client, monkeypatch, helpers, defc_codes, basic_ref_data, other_budget_authority_gtas):
    helpers.patch_datetime_now(monkeypatch, 2021, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["funding"] == [{"amount": Decimal("0.85"), "def_code": "M"}]
    assert resp.data["total_budget_authority"] == Decimal("0.85")
    assert resp.data["spending"]["total_obligations"] == Decimal("0.85")
    assert resp.data["spending"]["total_outlays"] == Decimal("0.02")


@pytest.mark.django_db
def test_award_obligation_adds_values(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, faba_with_values
):
    helpers.patch_datetime_now(monkeypatch, 2021, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["award_obligations"] == Decimal("2.3")
    assert resp.data["spending"]["award_outlays"] == Decimal("1.15")


@pytest.mark.django_db
def test_faba_excludes_non_selected_defc(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, faba_with_non_covid_values
):
    helpers.patch_datetime_now(monkeypatch, 2021, LATE_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL + "?def_codes=M,N")
    assert resp.data["spending"]["award_obligations"] == Decimal("1.6")
    assert resp.data["spending"]["award_outlays"] == Decimal("0.8")


@pytest.mark.django_db
def test_award_outlays_sum_multiple_years(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, multi_year_faba
):
    helpers.patch_datetime_now(monkeypatch, 2022, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["award_outlays"] == Decimal("1.15")


@pytest.mark.django_db
def test_award_outlays_doesnt_sum_multiple_periods(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, multi_period_faba
):
    helpers.patch_datetime_now(monkeypatch, 2022, LATE_MONTH, 25)
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["award_outlays"] == Decimal("0.8")


@pytest.mark.django_db
def test_award_outlays_ignores_future_faba(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, multi_period_faba
):
    helpers.patch_datetime_now(monkeypatch, 2022, EARLY_MONTH, 25)
    helpers.reset_dabs_cache()
    resp = client.get(OVERVIEW_URL)
    assert resp.data["spending"]["award_outlays"] == Decimal("0.35")
