import pytest
from mock import patch, Mock
from rest_framework import status

@patch("requests.post")
def mock503():
    return {"status_code": status.HTTP_503_SERVICE_UNAVAILABLE}

@pytest.mark.django_db
def test_service_unavailable(client):
    with pytest.raises(Exception) as e_info:
        client.get("/api/v2/references/cfda/totals/")
    print(e_info)
    assert e_info == 0

# class Mock503:
#     @staticmethod
#     def status_code():
#         return status.HTTP_503_SERVICE_UNAVAILABLE

#     @staticmethod
#     def json():
#         return {"cfdas": None}


# @pytest.mark.django_db
# def test_service_unavailable(client, monkeypatch):
#     def mock_post(*args, **kwargs):
#         return Mock503()

#     monkeypatch.setattr(requests, "post", mock_post)

#     with pytest.raises(Exception) as e_info:
#         client.get("/api/v2/references/cfda/totals/")
#     print(e_info)
#     assert e_info == 0
