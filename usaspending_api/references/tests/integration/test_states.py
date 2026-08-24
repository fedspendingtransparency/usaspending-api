import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.recipient.models import StateData

ENDPOINT_URL = "/api/v2/references/states/"


@pytest.mark.django_db
def test_states_endpoint_success(client):
    """Test that the states endpoint returns all states with latest year data."""

    # Create test data with multiple years for same FIPS
    baker.make(StateData, fips="01", code="AL", name="Alabama", year=2022)
    baker.make(StateData, fips="01", code="AL", name="Alabama", year=2023)  # Latest
    baker.make(StateData, fips="02", code="AK", name="Alaska", year=2023)
    baker.make(StateData, fips="06", code="CA", name="California", year=2023)

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should return 3 distinct states
    assert len(results) == 3

    # Verify structure of first result
    assert "fips" in results[0]
    assert "code" in results[0]
    assert "name" in results[0]

    # Verify sorted by code
    codes = [r["code"] for r in results]
    assert codes == sorted(codes)

    # Verify latest year is returned (only one Alabama entry)
    alabama_entries = [r for r in results if r["fips"] == "01"]
    assert len(alabama_entries) == 1


@pytest.mark.django_db
def test_states_endpoint_returns_latest_year_only(client):
    """Test that only the latest year data is returned for each FIPS."""

    # Create multiple years for same state
    baker.make(StateData, fips="48", code="TX", name="Texas", year=2020)
    baker.make(StateData, fips="48", code="TX", name="Texas", year=2021)
    baker.make(StateData, fips="48", code="TX", name="Texas Updated", year=2023)  # Latest

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should only return one Texas entry
    texas_entries = [r for r in results if r["fips"] == "48"]
    assert len(texas_entries) == 1

    # Should be the latest year (2023)
    assert texas_entries[0]["name"] == "TEXAS UPDATED"


@pytest.mark.django_db
def test_states_endpoint_uppercase_conversion(client):
    """Test that state codes and names are converted to uppercase."""

    baker.make(StateData, fips="12", code="fl", name="florida", year=2023)
    baker.make(StateData, fips="36", code="ny", name="New York", year=2023)

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    florida = next(r for r in results if r["fips"] == "12")
    assert florida["code"] == "FL"
    assert florida["name"] == "FLORIDA"

    new_york = next(r for r in results if r["fips"] == "36")
    assert new_york["code"] == "NY"
    assert new_york["name"] == "NEW YORK"


@pytest.mark.django_db
def test_states_endpoint_handles_empty_values(client):
    """Test that empty code/name values are handled gracefully."""

    baker.make(StateData, fips="99", code="", name="", year=2023)
    baker.make(StateData, fips="13", code="GA", name="Georgia", year=2023)

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should still return both entries
    assert len(results) == 2

    # Null values should be converted to empty strings
    null_entry = next(r for r in results if r["fips"] == "99")
    assert null_entry["code"] == ""
    assert null_entry["name"] == ""


@pytest.mark.django_db
def test_states_endpoint_empty_database(client):
    """Test endpoint behavior when no state data exists."""

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]
    assert results == []


@pytest.mark.django_db
def test_states_endpoint_sorted_by_code(client):
    """Test that results are sorted alphabetically by state code."""

    # Create states in random order
    baker.make(StateData, fips="48", code="TX", name="Texas", year=2023)
    baker.make(StateData, fips="01", code="AL", name="Alabama", year=2023)
    baker.make(StateData, fips="36", code="NY", name="New York", year=2023)
    baker.make(StateData, fips="06", code="CA", name="California", year=2023)

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]
    codes = [r["code"] for r in results]

    # Should be sorted: AL, CA, NY, TX
    assert codes == ["AL", "CA", "NY", "TX"]


@pytest.mark.django_db
def test_states_endpoint_includes_territories(client):
    """Test that territories and districts are included."""

    # Create states and territories
    baker.make(StateData, fips="01", code="AL", name="Alabama", year=2023)
    baker.make(StateData, fips="11", code="DC", name="District of Columbia", year=2023)
    baker.make(StateData, fips="72", code="PR", name="Puerto Rico", year=2023)
    baker.make(StateData, fips="78", code="VI", name="Virgin Islands", year=2023)

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should include all 4 entries
    assert len(results) == 4

    # Verify territories are present
    codes = [r["code"] for r in results]
    assert "DC" in codes
    assert "PR" in codes
    assert "VI" in codes


@pytest.mark.django_db
def test_states_endpoint_response_structure(client):
    """Test the exact structure of the response."""

    baker.make(StateData, fips="01", code="AL", name="Alabama", year=2023)

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    json_response = response.json()

    # Top level should have "results" key
    assert "results" in json_response
    assert isinstance(json_response["results"], list)

    # Each result should have exactly these keys
    result = json_response["results"][0]
    assert set(result.keys()) == {"fips", "code", "name"}

    # Verify data types
    assert isinstance(result["fips"], str)
    assert isinstance(result["code"], str)
    assert isinstance(result["name"], str)


@pytest.mark.django_db
def test_states_endpoint_only_accepts_get(client):
    """Test that only GET method is allowed."""

    # POST should not be allowed
    response = client.post(ENDPOINT_URL, data={})
    assert response.status_code == status.HTTP_405_METHOD_NOT_ALLOWED

    # PUT should not be allowed
    response = client.put(ENDPOINT_URL, data={})
    assert response.status_code == status.HTTP_405_METHOD_NOT_ALLOWED

    # DELETE should not be allowed
    response = client.delete(ENDPOINT_URL)
    assert response.status_code == status.HTTP_405_METHOD_NOT_ALLOWED


@pytest.mark.django_db
def test_states_endpoint_with_all_50_states(client):
    """Test with realistic data - all 50 states plus territories."""

    states_data = [
        ("01", "AL", "Alabama"),
        ("02", "AK", "Alaska"),
        ("04", "AZ", "Arizona"),
        ("05", "AR", "Arkansas"),
        ("06", "CA", "California"),
        ("08", "CO", "Colorado"),
        ("09", "CT", "Connecticut"),
        ("10", "DE", "Delaware"),
        ("11", "DC", "District of Columbia"),
        ("12", "FL", "Florida"),
        ("13", "GA", "Georgia"),
        ("15", "HI", "Hawaii"),
        ("16", "ID", "Idaho"),
        ("17", "IL", "Illinois"),
        ("18", "IN", "Indiana"),
        ("19", "IA", "Iowa"),
        ("20", "KS", "Kansas"),
        ("21", "KY", "Kentucky"),
        ("22", "LA", "Louisiana"),
        ("23", "ME", "Maine"),
        ("24", "MD", "Maryland"),
        ("25", "MA", "Massachusetts"),
        ("26", "MI", "Michigan"),
        ("27", "MN", "Minnesota"),
        ("28", "MS", "Mississippi"),
        ("29", "MO", "Missouri"),
        ("30", "MT", "Montana"),
        ("31", "NE", "Nebraska"),
        ("32", "NV", "Nevada"),
        ("33", "NH", "New Hampshire"),
        ("34", "NJ", "New Jersey"),
        ("35", "NM", "New Mexico"),
        ("36", "NY", "New York"),
        ("37", "NC", "North Carolina"),
        ("38", "ND", "North Dakota"),
        ("39", "OH", "Ohio"),
        ("40", "OK", "Oklahoma"),
        ("41", "OR", "Oregon"),
        ("42", "PA", "Pennsylvania"),
        ("44", "RI", "Rhode Island"),
        ("45", "SC", "South Carolina"),
        ("46", "SD", "South Dakota"),
        ("47", "TN", "Tennessee"),
        ("48", "TX", "Texas"),
        ("49", "UT", "Utah"),
        ("50", "VT", "Vermont"),
        ("51", "VA", "Virginia"),
        ("53", "WA", "Washington"),
        ("54", "WV", "West Virginia"),
        ("55", "WI", "Wisconsin"),
        ("56", "WY", "Wyoming"),
        ("72", "PR", "Puerto Rico"),
        ("78", "VI", "Virgin Islands"),
    ]

    for fips, code, name in states_data:
        baker.make(StateData, fips=fips, code=code, name=name, year=2023)

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should return all entries
    assert len(results) == len(states_data)

    # Verify first and last in sorted order
    assert results[0]["code"] == "AK"  # Alaska first alphabetically
    assert results[-1]["code"] == "WY"  # Wyoming last
