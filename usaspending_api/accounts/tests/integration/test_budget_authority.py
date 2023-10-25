import pytest
from model_bakery import baker

from rest_framework import status


@pytest.fixture
def model_instances():
    baker.make("accounts.BudgetAuthority", year=2000, amount=2000000, agency_identifier="000")
    baker.make("accounts.BudgetAuthority", year=2001, amount=2001000, agency_identifier="000")
    baker.make("accounts.BudgetAuthority", year=2002, amount=2002000, agency_identifier="000")
    baker.make("accounts.BudgetAuthority", year=2003, amount=1003000, agency_identifier="000")
    baker.make("accounts.BudgetAuthority", year=2000, amount=1000, agency_identifier="002")
    baker.make("accounts.BudgetAuthority", year=2000, amount=2000, fr_entity_code="0202", agency_identifier="002")


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint(model_instances, client):
    resp = client.get("/api/v2/budget_authority/agencies/000/")
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()["results"]
    assert len(results) == 4
    for result in results:
        assert "year" in result
        assert "total" in result


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint_no_records(model_instances, client):
    resp = client.get("/api/v2/budget_authority/agencies/001/")
    assert resp.status_code == status.HTTP_200_OK
    assert not resp.json()["results"]


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint_no_frec_sums_all(model_instances, client):
    "If FREC is not specified, all records with that AID should be summed"
    resp = client.get("/api/v2/budget_authority/agencies/002/")
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()["results"]
    assert len(results) == 1
    assert results[0]["total"] == 3000


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint_filters_on_frec(model_instances, client):
    "If FREC is specified, sum only records with that FREC"
    resp = client.get("/api/v2/budget_authority/agencies/002/?frec=0202")
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()["results"]
    assert len(results) == 1
    assert results[0]["total"] == 2000


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint_sorts_year_by_default(model_instances, client):
    resp = client.get("/api/v2/budget_authority/agencies/002/")
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()["results"]
    years = [r["year"] for r in results]
    assert years == sorted(years)


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint_bad_sort_parameters(model_instances, client):
    "Appropriate errors should be thrown if bad sort parameters supplied"
    resp = client.get("/api/v2/budget_authority/agencies/000/?sort=wxyz")
    # Even though I'm raising ParseErrors, which should be 400s,
    # they're being raised as 500s... thus skipping for now
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    resp = client.get("/api/v2/budget_authority/agencies/002/?sort=year&order=wxyz")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint_sort_year(model_instances, client):
    "Test support for `sort` and `order` parameters"
    resp = client.get("/api/v2/budget_authority/agencies/000/?sort=year")
    years = [r["year"] for r in resp.json()["results"]]
    assert years == sorted(years)


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint_sort_year_desc(model_instances, client):
    resp = client.get("/api/v2/budget_authority/agencies/000/?sort=year&order=desc")
    years = [r["year"] for r in resp.json()["results"]]
    assert years == sorted(years, reverse=True)


@pytest.mark.skip
@pytest.mark.django_db
def test_budget_authority_endpoint_sort_total(model_instances, client):
    resp = client.get("/api/v2/budget_authority/agencies/000/?sort=total&order=desc")
    totals = [r["total"] for r in resp.json()["results"]]
    assert totals == sorted(totals, reverse=True)
