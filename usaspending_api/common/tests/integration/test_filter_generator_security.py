"""
Integration tests for FilterGenerator security fixes.
Tests validation of filter field paths to prevent ORM injection (CVE-943).
Uses /api/v1/tas/categories/total/ endpoint which uses FilterQuerysetMixin.
"""
import pytest


@pytest.mark.django_db
def test_post_filter_blocks_regex_injection(client):
    """Test that __regex in POST filter field is rejected."""
    response = client.post(
        "/api/v1/tas/categories/total/",
        {
            "field": "obligations_incurred_by_program_object_class_cpe",
            "group": "treasury_account",
            "filters": [
                {"field": "submission__reporting_fiscal_year__regex", "operation": "equals", "value": ".*2020.*"}
            ],
        },
        content_type="application/json",
    )
    assert response.status_code == 400
    assert "Invalid field" in response.json()["detail"]


@pytest.mark.django_db
def test_post_filter_blocks_iregex_injection(client):
    """Test that __iregex in POST filter field is rejected."""
    response = client.post(
        "/api/v1/tas/categories/total/",
        {
            "field": "obligations_incurred_by_program_object_class_cpe",
            "group": "treasury_account",
            "filters": [
                {"field": "submission__reporting_fiscal_year__iregex", "operation": "equals", "value": ".*2020.*"}
            ],
        },
        content_type="application/json",
    )
    assert response.status_code == 400
    assert "Invalid field" in response.json()["detail"]


@pytest.mark.django_db
def test_post_filter_allows_safe_lookups(client):
    """Test that safe lookups like greater_than_or_equal work in filter operations."""
    response = client.post(
        "/api/v1/tas/categories/total/",
        {
            "field": "obligations_incurred_by_program_object_class_cpe",
            "group": "treasury_account",
            "filters": [{
                "field": "submission__reporting_fiscal_year",
                "operation": "greater_than_or_equal",
                "value": 2020
            }],
        },
        content_type="application/json",
    )
    assert response.status_code == 200


@pytest.mark.django_db
def test_post_filter_allows_fk_traversal(client):
    """Test that FK traversal works in filter fields."""
    response = client.post(
        "/api/v1/tas/categories/total/",
        {
            "field": "obligations_incurred_by_program_object_class_cpe",
            "group": "treasury_account",
            "filters": [
                {"field": "treasury_account__federal_account__account_title", "operation": "equals", "value": "Test"}
            ],
        },
        content_type="application/json",
    )
    assert response.status_code == 200


@pytest.mark.django_db
def test_get_filter_blocks_regex_injection(client):
    """Test that __regex in GET params is rejected."""
    response = client.get("/api/v1/federal_accounts/?account_title__regex=.*sensitive.*")
    assert response.status_code == 400


@pytest.mark.django_db
def test_get_filter_blocks_iregex_injection(client):
    """Test that __iregex in GET params is rejected."""
    response = client.get("/api/v1/federal_accounts/?account_title__iregex=.*sensitive.*")
    assert response.status_code == 400


@pytest.mark.django_db
def test_get_filter_allows_safe_lookups(client):
    """Test that safe lookups work in GET params."""
    response = client.get("/api/v1/federal_accounts/?account_title__icontains=defense")
    assert response.status_code == 200
