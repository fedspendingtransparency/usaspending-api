import pytest
from django.test import Client
from django.urls import NoReverseMatch, reverse


def test_api_login_paths_returns_404(client: Client):
    response = client.get("/api-auth/login/")
    assert response.status_code == 404

    response = client.get("/api-auth/logout/")
    assert response.status_code == 404


def test_url_name_no_longer_resolves():
    with pytest.raises(NoReverseMatch):
        reverse("rest_framework:login")
