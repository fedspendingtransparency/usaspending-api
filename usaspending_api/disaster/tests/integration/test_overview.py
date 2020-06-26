import pytest

from decimal import Decimal

OVERVIEW_URL = "/api/v2/disaster/overview/"


@pytest.mark.django_db
def test_basic_data_set(client, monkeypatch, helpers, defc_codes, basic_ref_data, early_gtas, basic_faba):
    helpers.patch_datetime_now(monkeypatch, 2021, 12, 25)
    resp = client.get(OVERVIEW_URL)
    assert resp.data == {
        "funding": [{"amount": Decimal("0.20"), "def_code": "M"}],
        "total_budget_authority": Decimal("0.20"),
        "spending": {
            "award_obligations": Decimal("0.0"),
            "award_outlays": Decimal("0"),
            "total_obligations": Decimal("0.2"),
            "total_outlays": Decimal("0.00"),
        },
    }


@pytest.mark.django_db
def test_using_only_latest_gtas(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, early_gtas, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2021, 12, 25)
    resp = client.get(OVERVIEW_URL)
    assert resp.data == {
        "funding": [{"amount": Decimal("0.3"), "def_code": "M"}],
        "total_budget_authority": Decimal("0.3"),
        "spending": {
            "award_obligations": Decimal("0.0"),
            "award_outlays": Decimal("0"),
            "total_obligations": Decimal("0.3"),
            "total_outlays": Decimal("0.00"),
        },
    }


@pytest.mark.django_db
def test_summing_multiple_years(
    client, monkeypatch, helpers, defc_codes, basic_ref_data, late_gtas, early_gtas, year_2_gtas_covid, basic_faba
):
    helpers.patch_datetime_now(monkeypatch, 2021, 12, 25)
    resp = client.get(OVERVIEW_URL)
    assert resp.data == {
        "funding": [{"amount": Decimal("0.52"), "def_code": "M"}],
        "total_budget_authority": Decimal("0.52"),
        "spending": {
            "award_obligations": Decimal("0.0"),
            "award_outlays": Decimal("0"),
            "total_obligations": Decimal("0.52"),
            "total_outlays": Decimal("0.00"),
        },
    }


@pytest.mark.django_db
def test_isolate_defc(client, monkeypatch, helpers, defc_codes, basic_ref_data, year_2_gtas_covid, year_2_gtas_covid_2):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 25)
    resp = client.get(OVERVIEW_URL)
    assert resp.data == {
        "funding": [{"amount": Decimal("0.22"), "def_code": "M"}, {"amount": Decimal("0.22"), "def_code": "N"}],
        "total_budget_authority": Decimal("0.44"),
        "spending": {
            "award_obligations": 0,
            "award_outlays": 0,
            "total_obligations": Decimal("0.44"),
            "total_outlays": Decimal("0.00"),
        },
    }
