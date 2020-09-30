# Stdlib imports
import datetime
import decimal

# Core Django imports

# Third-party app imports
from rest_framework import status
from model_mommy import mommy
import pytest

# Imports from your apps
from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_year
from usaspending_api.recipient.v2.views.states import obtain_state_totals

# Getting relative dates as the 'latest'/default argument returns results relative to when it gets called
TODAY = datetime.datetime.now()
OUTSIDE_OF_LATEST = datetime.datetime(TODAY.year - 2, TODAY.month, TODAY.day)
CURRENT_FISCAL_YEAR = generate_fiscal_year(TODAY)

EXPECTED_STATE = {
    "name": "Test State",
    "code": "TS",
    "fips": "01",
    "type": "state",
    "population": 100000,
    "pop_year": CURRENT_FISCAL_YEAR,
    "pop_source": "Census 2010 Pop",
    "median_household_income": 50000,
    "mhi_year": CURRENT_FISCAL_YEAR - 2,
    "mhi_source": "Census 2010 MHI",
    "total_prime_amount": 100000,
    "total_prime_awards": 1,
    "total_face_value_loan_amount": 0,
    "total_face_value_loan_prime_awards": 0,
    "award_amount_per_capita": 1,
}
EXPECTED_DISTRICT = EXPECTED_STATE.copy()
EXPECTED_DISTRICT.update(
    {
        "name": "Test District",
        "code": "TD",
        "fips": "02",
        "type": "district",
        "pop_year": CURRENT_FISCAL_YEAR - 2,
        "median_household_income": 20000,
        "population": 5000,
        "total_prime_amount": 1000,
        "total_prime_awards": 1,
        "total_face_value_loan_amount": 0,
        "total_face_value_loan_prime_awards": 0,
        "award_amount_per_capita": round(decimal.Decimal(0.20), 2),
    }
)
EXPECTED_TERRITORY = EXPECTED_STATE.copy()
EXPECTED_TERRITORY.update(
    {
        "name": "Test Territory",
        "code": "TT",
        "fips": "03",
        "type": "territory",
        "pop_year": CURRENT_FISCAL_YEAR - 2,
        "median_household_income": 10000,
        "population": 5000,
        "total_prime_amount": 1000,
        "total_prime_awards": 1,
        "total_face_value_loan_amount": 0,
        "total_face_value_loan_prime_awards": 0,
        "award_amount_per_capita": round(decimal.Decimal(0.20), 2),
    }
)


def state_metadata_endpoint(fips, year=None):
    url = "/api/v2/recipient/state/{}/".format(fips)
    if year:
        url = "{}?year={}".format(url, year)
    return url


def sort_breakdown_response(response_list):
    """Sorting response since on Travis order of breakdown response list is different"""
    return sorted(response_list, key=lambda k: k["type"])


def sort_states_response(response_list):
    """Sorting response since on Travis order of breakdown response list is different"""
    return sorted(response_list, key=lambda k: k["fips"])


@pytest.fixture
def state_data(db):
    mommy.make(
        "awards.TransactionNormalized",
        assistance_data__place_of_perfor_state_code="TS",
        assistance_data__place_of_perform_country_c="USA",
        federal_action_obligation=100000,
        action_date=TODAY.strftime("%Y-%m-%d"),
    )
    mommy.make(
        "awards.TransactionNormalized",
        assistance_data__place_of_perfor_state_code="TS",
        assistance_data__place_of_perform_country_c="USA",
        federal_action_obligation=100000,
        action_date=OUTSIDE_OF_LATEST.strftime("%Y-%m-%d"),
    )
    mommy.make(
        "awards.TransactionNormalized",
        assistance_data__place_of_perfor_state_code="TD",
        assistance_data__place_of_perform_country_c="USA",
        federal_action_obligation=1000,
        action_date=TODAY.strftime("%Y-%m-%d"),
    )
    mommy.make(
        "awards.TransactionNormalized",
        assistance_data__place_of_perfor_state_code="TT",
        assistance_data__place_of_perform_country_c="USA",
        federal_action_obligation=1000,
        action_date=TODAY.strftime("%Y-%m-%d"),
    )
    mommy.make(
        "recipient.StateData",
        id="01-{}".format(CURRENT_FISCAL_YEAR - 2),
        fips="01",
        name="Test State",
        code="TS",
        type="state",
        year=CURRENT_FISCAL_YEAR - 2,
        population=50000,
        pop_source="Census 2010 Pop",
        median_household_income=50000,
        mhi_source="Census 2010 MHI",
    )
    mommy.make(
        "recipient.StateData",
        id="01-{}".format(CURRENT_FISCAL_YEAR),
        fips="01",
        name="Test State",
        code="TS",
        type="state",
        year=CURRENT_FISCAL_YEAR,
        population=100000,
        pop_source="Census 2010 Pop",
        median_household_income=None,
        mhi_source="Census 2010 MHI",
    )
    mommy.make(
        "recipient.StateData",
        id="02-{}".format(CURRENT_FISCAL_YEAR - 2),
        fips="02",
        name="Test District",
        code="TD",
        type="district",
        year=CURRENT_FISCAL_YEAR - 2,
        population=5000,
        pop_source="Census 2010 Pop",
        median_household_income=20000,
        mhi_source="Census 2010 MHI",
    )
    mommy.make(
        "recipient.StateData",
        id="03-{}".format(CURRENT_FISCAL_YEAR - 2),
        fips="03",
        name="Test Territory",
        code="TT",
        type="territory",
        year=CURRENT_FISCAL_YEAR - 2,
        population=5000,
        pop_source="Census 2010 Pop",
        median_household_income=10000,
        mhi_source="Census 2010 MHI",
    )


@pytest.fixture
def state_view_data(db, monkeypatch):
    monkeypatch.setattr("usaspending_api.recipient.v2.views.states.VALID_FIPS", {"01": {"code": "AB"}})

    award_old = mommy.make("awards.Award", type="A")

    award_cur = mommy.make("awards.Award", type="B")

    trans_old = mommy.make(
        "awards.TransactionNormalized",
        award=award_old,
        type="A",
        assistance_data__place_of_perfor_state_code="AB",
        assistance_data__place_of_perform_country_c="USA",
        federal_action_obligation=10,
        fiscal_year=generate_fiscal_year(OUTSIDE_OF_LATEST),
        action_date=OUTSIDE_OF_LATEST.strftime("%Y-%m-%d"),
    )

    trans_cur = mommy.make(
        "awards.TransactionNormalized",
        award=award_cur,
        type="B",
        assistance_data__place_of_perfor_state_code="AB",
        assistance_data__place_of_perform_country_c="USA",
        federal_action_obligation=15,
        fiscal_year=generate_fiscal_year(TODAY),
        action_date=TODAY.strftime("%Y-%m-%d"),
    )

    mommy.make("awards.TransactionFPDS", transaction=trans_old)
    mommy.make("awards.TransactionFPDS", transaction=trans_cur)


@pytest.fixture
def state_view_loan_data(db, monkeypatch):
    monkeypatch.setattr("usaspending_api.recipient.v2.views.states.VALID_FIPS", {"01": {"code": "AB"}})

    award_old = mommy.make("awards.Award", type="07")
    award_old2 = mommy.make("awards.Award", type="08")
    award_cur = mommy.make("awards.Award", type="07")

    trans_old = mommy.make(
        "awards.TransactionNormalized",
        award=award_old,
        type="07",
        assistance_data__place_of_perfor_state_code="AB",
        assistance_data__place_of_perform_country_c="USA",
        original_loan_subsidy_cost=10,
        fiscal_year=generate_fiscal_year(OUTSIDE_OF_LATEST),
        face_value_loan_guarantee=1500,
        action_date=OUTSIDE_OF_LATEST.strftime("%Y-%m-%d"),
    )

    trans_old2 = mommy.make(
        "awards.TransactionNormalized",
        award=award_old2,
        type="08",
        assistance_data__place_of_perfor_state_code="AB",
        assistance_data__place_of_perform_country_c="USA",
        original_loan_subsidy_cost=15,
        fiscal_year=generate_fiscal_year(OUTSIDE_OF_LATEST),
        face_value_loan_guarantee=11,
        action_date=OUTSIDE_OF_LATEST.strftime("%Y-%m-%d"),
    )

    trans_cur = mommy.make(
        "awards.TransactionNormalized",
        award=award_cur,
        type="A",
        assistance_data__place_of_perfor_state_code="AB",
        assistance_data__place_of_perform_country_c="USA",
        federal_action_obligation=100,
        fiscal_year=generate_fiscal_year(OUTSIDE_OF_LATEST),
        face_value_loan_guarantee=2000,
        action_date=TODAY.strftime("%Y-%m-%d"),
    )

    mommy.make("awards.TransactionFPDS", transaction=trans_old)
    mommy.make("awards.TransactionFPDS", transaction=trans_old2)
    mommy.make("awards.TransactionFPDS", transaction=trans_cur)


@pytest.fixture()
def state_breakdown_result():
    expected_result = [
        {"type": "contracts", "amount": 0, "count": 0},
        {"type": "direct_payments", "amount": 0, "count": 0},
        {"type": "grants", "amount": 0, "count": 0},
        {"type": "loans", "amount": 0, "count": 0},
        {"type": "other_financial_assistance", "amount": 0, "count": 0},
    ]

    return expected_result


@pytest.mark.django_db
def test_state_metadata_success(client, state_data):
    # test small request - state
    resp = client.get(state_metadata_endpoint("01"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == EXPECTED_STATE

    # test small request - district
    resp = client.get(state_metadata_endpoint("02"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == EXPECTED_DISTRICT

    # test small request - territory
    resp = client.get(state_metadata_endpoint("03"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == EXPECTED_TERRITORY


@pytest.mark.django_db
def test_state_years_success(client, state_data):
    # test future year
    expected_response = EXPECTED_STATE.copy()
    expected_response.update(
        {
            "pop_year": CURRENT_FISCAL_YEAR,
            "mhi_year": CURRENT_FISCAL_YEAR - 2,
            "total_prime_amount": 0,
            "total_prime_awards": 0,
            "award_amount_per_capita": 0,
        }
    )
    resp = client.get(state_metadata_endpoint("01", "3000"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response

    # test old year
    expected_response = EXPECTED_STATE.copy()
    expected_response.update(
        {
            "pop_year": CURRENT_FISCAL_YEAR - 2,
            "mhi_year": CURRENT_FISCAL_YEAR - 2,
            "population": 50000,
            "total_prime_amount": 0,
            "total_prime_awards": 0,
            "award_amount_per_capita": 0,
        }
    )
    resp = client.get(state_metadata_endpoint("01", "2000"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response

    # test older year
    expected_response = EXPECTED_STATE.copy()
    expected_response.update(
        {
            "pop_year": CURRENT_FISCAL_YEAR - 2,
            "mhi_year": CURRENT_FISCAL_YEAR - 2,
            "population": 50000,
            "award_amount_per_capita": 2,
        }
    )
    resp = client.get(state_metadata_endpoint("01", CURRENT_FISCAL_YEAR - 2))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response

    # test latest year
    expected_response = EXPECTED_STATE.copy()
    expected_response.update({"pop_year": CURRENT_FISCAL_YEAR, "mhi_year": CURRENT_FISCAL_YEAR - 2})
    resp = client.get(state_metadata_endpoint("01", "latest"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response


@pytest.mark.django_db
def test_state_current_all_years_success(client, state_data):
    # test all years
    expected_response = EXPECTED_STATE.copy()
    expected_response.update(
        {
            "pop_year": CURRENT_FISCAL_YEAR,
            "mhi_year": CURRENT_FISCAL_YEAR - 2,
            "award_amount_per_capita": None,
            "total_prime_awards": 2,
            "total_prime_amount": 200000,
        }
    )
    resp = client.get(state_metadata_endpoint("01", "all"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response


@pytest.mark.django_db
def test_state_current_fy_capita_success(client, state_data):
    # making sure amount per capita is null for current fiscal year
    expected_response = EXPECTED_STATE.copy()
    expected_response.update({"award_amount_per_capita": None})
    resp = client.get(state_metadata_endpoint("01", CURRENT_FISCAL_YEAR))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == expected_response


@pytest.mark.django_db
def test_state_metadata_failure(client, state_data):
    """Verify error on bad autocomplete request for budget function."""

    # There is no FIPS with 04
    resp = client.get(state_metadata_endpoint("04"))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    # There is no break year
    resp = client.get(state_metadata_endpoint("01", "break"))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    # test small request - district, testing 1 digit FIPS
    resp = client.get(state_metadata_endpoint("2"))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_obtain_state_totals(state_view_data):
    result = obtain_state_totals("01", str(generate_fiscal_year(OUTSIDE_OF_LATEST)), ["A"])
    expected = {
        "pop_state_code": "AB",
        "total": 10,
        "count": 1,
        "total_face_value_loan_amount": 0,
    }
    assert result == expected


@pytest.mark.django_db
def test_obtain_state_totals_loan_agg(state_view_loan_data):
    result = obtain_state_totals("01", str(generate_fiscal_year(OUTSIDE_OF_LATEST)))
    expected = {
        "pop_state_code": "AB",
        "total": 25,
        "count": 2,
        "total_face_value_loan_amount": 1511,
    }
    assert result == expected


@pytest.mark.django_db
def test_obtain_state_totals_none(state_view_data, monkeypatch):
    monkeypatch.setattr("usaspending_api.recipient.v2.views.states.VALID_FIPS", {"02": {"code": "No State"}})
    result = obtain_state_totals("02")
    expected = {
        "pop_state_code": None,
        "total": 0,
        "count": 0,
        "total_face_value_loan_amount": 0,
    }

    assert result == expected


@pytest.mark.django_db
def test_state_breakdown_success_state(client, state_view_data, state_breakdown_result):
    resp = client.get("/api/v2/recipient/state/awards/01/?year=all")
    sorted_resp = sort_breakdown_response(resp.data)

    expected = state_breakdown_result
    expected[0] = {"type": "contracts", "amount": 25, "count": 2}

    assert resp.status_code == status.HTTP_200_OK
    assert sorted_resp == expected


@pytest.mark.django_db
def test_state_breakdown_success_year(client, state_view_data, state_breakdown_result):
    resp = client.get("/api/v2/recipient/state/awards/01/?year={}".format(str(generate_fiscal_year(TODAY))))
    sorted_resp = sort_breakdown_response(resp.data)

    expected = state_breakdown_result
    expected[0] = {"type": "contracts", "amount": 15, "count": 1}

    assert resp.status_code == status.HTTP_200_OK
    assert sorted_resp == expected


@pytest.mark.django_db
def test_state_breakdown_success_no_data(client, state_view_data, state_breakdown_result):
    resp = client.get("/api/v2/recipient/state/awards/01/?year={}".format(CURRENT_FISCAL_YEAR - 3))
    sorted_resp = sort_breakdown_response(resp.data)

    expected = state_breakdown_result

    assert resp.status_code == status.HTTP_200_OK
    assert sorted_resp == expected


@pytest.mark.django_db
def test_state_breakdown_failure(client, state_view_data):
    resp = client.get("/api/v2/recipient/state/awards/05/")

    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_state_list_success_state(client, state_data):
    resp = client.get("/api/v2/recipient/state/")
    sorted_resp = sort_states_response(resp.data)

    expected = [
        {"name": "Test State", "code": "TS", "fips": "01", "type": "state", "amount": 100000.00, "count": 1},
        {"name": "Test District", "code": "TD", "fips": "02", "type": "district", "amount": 1000.00, "count": 1},
        {"name": "Test Territory", "code": "TT", "fips": "03", "type": "territory", "amount": 1000.00, "count": 1},
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert sorted_resp == expected
