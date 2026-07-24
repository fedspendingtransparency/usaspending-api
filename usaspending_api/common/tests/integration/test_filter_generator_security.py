"""
Integration tests for FilterGenerator security fixes.
Tests validation of filter field paths to prevent ORM injection (CVE-943).
"""
import pytest


@pytest.mark.django_db
def test_post_filter_blocks_regex_injection(client):
    """Test that __regex in POST filter field is rejected."""
    response = client.post(
        "/api/v1/awards/",
        {"filters": [{"field": "award_type__regex", "operation": "equals", "value": ".*sensitive.*"}]},
        content_type="application/json",
    )
    assert response.status_code == 400
    assert "Invalid field" in response.json()["detail"]


@pytest.mark.django_db
def test_post_filter_blocks_iregex_injection(client):
    """Test that __iregex in POST filter field is rejected."""
    response = client.post(
        "/api/v1/awards/",
        {"filters": [{"field": "award_type__iregex", "operation": "equals", "value": ".*sensitive.*"}]},
        content_type="application/json",
    )
    assert response.status_code == 400
    assert "Invalid field" in response.json()["detail"]


@pytest.mark.django_db
def test_post_filter_allows_safe_lookups(client):
    """Test that safe lookups like __icontains work in filter fields."""
    response = client.post(
        "/api/v1/awards/",
        {"filters": [{"field": "description__icontains", "operation": "equals", "value": "software"}]},
        content_type="application/json",
    )
    assert response.status_code == 200


@pytest.mark.django_db
def test_post_filter_allows_fk_traversal(client):
    """Test that FK traversal works in filter fields."""
    response = client.post(
        "/api/v1/awards/",
        {
            "filters": [
                {"field": "awarding_agency__toptier_agency__name", "operation": "equals", "value": "Department of Defense"}
            ]
        },
        content_type="application/json",
    )
    assert response.status_code == 200


@pytest.mark.django_db
def test_get_filter_blocks_regex_injection(client):
    """Test that __regex in GET params is rejected."""
    response = client.get("/api/v1/awards/?award_type__regex=.*sensitive.*")
    assert response.status_code == 400


@pytest.mark.django_db
def test_get_filter_blocks_iregex_injection(client):
    """Test that __iregex in GET params is rejected."""
    response = client.get("/api/v1/awards/?award_type__iregex=.*sensitive.*")
    assert response.status_code == 400


@pytest.mark.django_db
def test_get_filter_allows_safe_lookups(client):
    """Test that safe lookups work in GET params."""
    response = client.get("/api/v1/awards/?award_type__icontains=grant")
    assert response.status_code == 200
