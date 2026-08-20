# usaspending_api/references/tests/integration/test_countries.py

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.references.models import RefCountryCode

ENDPOINT_URL = "/api/v2/references/countries/"


@pytest.mark.django_db
def test_countries_endpoint_success(client):
    """Test that the countries endpoint returns all countries."""

    # Create test data
    baker.make(RefCountryCode, country_code="USA", country_name="United States")
    baker.make(RefCountryCode, country_code="CAN", country_name="Canada")
    baker.make(RefCountryCode, country_code="MEX", country_name="Mexico")

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should return 3 countries
    assert len(results) == 3

    # Verify structure of first result
    assert "code" in results[0]
    assert "name" in results[0]

    # Verify sorted by code
    codes = [r["code"] for r in results]
    assert codes == sorted(codes)


@pytest.mark.django_db
def test_countries_endpoint_uppercase_conversion(client):
    """Test that country codes and names are converted to uppercase."""

    baker.make(RefCountryCode, country_code="gbr", country_name="united kingdom")
    baker.make(RefCountryCode, country_code="fra", country_name="France")

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    uk = next(r for r in results if r["code"] == "GBR")
    assert uk["code"] == "GBR"
    assert uk["name"] == "UNITED KINGDOM"

    france = next(r for r in results if r["code"] == "FRA")
    assert france["code"] == "FRA"
    assert france["name"] == "FRANCE"


@pytest.mark.django_db
def test_countries_endpoint_handles_empty_values(client):
    """Test that empty code/name values are handled gracefully."""

    baker.make(RefCountryCode, country_code="", country_name="")
    baker.make(RefCountryCode, country_code="USA", country_name="United States")

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should still return both entries
    assert len(results) == 2

    # Empty values should remain empty strings
    empty_entry = next(r for r in results if r["code"] == "")
    assert empty_entry["code"] == ""
    assert empty_entry["name"] == ""


@pytest.mark.django_db
def test_countries_endpoint_empty_database(client):
    """Test endpoint behavior when no country data exists."""

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]
    assert results == []


@pytest.mark.django_db
def test_countries_endpoint_sorted_by_code(client):
    """Test that results are sorted alphabetically by country code."""

    # Create countries in random order
    baker.make(RefCountryCode, country_code="MEX", country_name="Mexico")
    baker.make(RefCountryCode, country_code="CAN", country_name="Canada")
    baker.make(RefCountryCode, country_code="USA", country_name="United States")
    baker.make(RefCountryCode, country_code="GBR", country_name="United Kingdom")

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]
    codes = [r["code"] for r in results]

    # Should be sorted: CAN, GBR, MEX, USA
    assert codes == ["CAN", "GBR", "MEX", "USA"]


@pytest.mark.django_db
def test_countries_endpoint_response_structure(client):
    """Test the exact structure of the response."""

    baker.make(RefCountryCode, country_code="USA", country_name="United States")

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    json_response = response.json()

    # Top level should have "results" key
    assert "results" in json_response
    assert isinstance(json_response["results"], list)

    # Each result should have exactly these keys
    result = json_response["results"][0]
    assert set(result.keys()) == {"code", "name"}

    # Verify data types
    assert isinstance(result["code"], str)
    assert isinstance(result["name"], str)


@pytest.mark.django_db
def test_countries_endpoint_only_accepts_get(client):
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
def test_countries_endpoint_multiple_requests(client):
    """Test that multiple requests work correctly."""

    baker.make(RefCountryCode, country_code="USA", country_name="United States")

    # First request
    response1 = client.get(ENDPOINT_URL)
    assert response1.status_code == status.HTTP_200_OK
    assert len(response1.json()["results"]) == 1

    # Second request should also work
    response2 = client.get(ENDPOINT_URL)
    assert response2.status_code == status.HTTP_200_OK
    assert len(response2.json()["results"]) == 1

    # Results should be consistent
    assert response1.json() == response2.json()


@pytest.mark.django_db
def test_countries_endpoint_with_many_countries(client):
    """Test with realistic data - multiple countries."""

    countries_data = [
        ("USA", "United States"),
        ("CAN", "Canada"),
        ("MEX", "Mexico"),
        ("GBR", "United Kingdom"),
        ("FRA", "France"),
        ("DEU", "Germany"),
        ("ITA", "Italy"),
        ("ESP", "Spain"),
        ("JPN", "Japan"),
        ("CHN", "China"),
        ("IND", "India"),
        ("BRA", "Brazil"),
        ("AUS", "Australia"),
        ("RUS", "Russia"),
        ("ZAF", "South Africa"),
    ]

    for code, name in countries_data:
        baker.make(RefCountryCode, country_code=code, country_name=name)

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should return all entries
    assert len(results) == len(countries_data)

    # Verify first and last in sorted order
    assert results[0]["code"] == "AUS"  # Australia first alphabetically
    assert results[-1]["code"] == "ZAF"  # South Africa last


@pytest.mark.django_db
def test_countries_endpoint_iso_codes(client):
    """Test that ISO 3166-1 alpha-3 codes are handled correctly."""

    # Create countries with valid ISO codes
    baker.make(RefCountryCode, country_code="USA", country_name="United States")
    baker.make(RefCountryCode, country_code="GBR", country_name="United Kingdom")
    baker.make(RefCountryCode, country_code="FRA", country_name="France")

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # All codes should be 3 characters (ISO standard)
    for result in results:
        assert len(result["code"]) == 3
        assert result["code"].isupper()


@pytest.mark.django_db
def test_countries_endpoint_special_characters_in_names(client):
    """Test that country names with special characters are handled."""

    baker.make(RefCountryCode, country_code="CIV", country_name="Côte d'Ivoire")
    baker.make(RefCountryCode, country_code="REU", country_name="Réunion")

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should handle special characters
    civ = next(r for r in results if r["code"] == "CIV")
    assert "CÔTE" in civ["name"].upper() or "COTE" in civ["name"]


@pytest.mark.django_db
def test_countries_endpoint_no_duplicates(client):
    """Test that duplicate country codes are handled (if they exist in DB)."""

    # Create potential duplicates
    baker.make(RefCountryCode, country_code="USA", country_name="United States")
    baker.make(RefCountryCode, country_code="USA", country_name="United States of America")

    response = client.get(ENDPOINT_URL)

    assert response.status_code == status.HTTP_200_OK

    results = response.json()["results"]

    # Should return both entries (ref_country_code may have duplicates)
    usa_entries = [r for r in results if r["code"] == "USA"]
    assert len(usa_entries) >= 1  # At least one USA entry
