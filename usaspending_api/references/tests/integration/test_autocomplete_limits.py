import json

import pytest
from django.test import Client


@pytest.mark.parametrize(
    "endpoint",
    [
        "/api/v2/autocomplete/cfda/",
        "/api/v2/autocomplete/naics/",
        "/api/v2/autocomplete/psc/",
        "/api/v2/autocomplete/program_activity/",
        "/api/v2/autocomplete/awarding_agency/",
        "/api/v2/autocomplete/awarding_agency_office/",
        "/api/v2/autocomplete/funding_agency/",
        "/api/v2/autocomplete/funding_agency_office/",
        "/api/v2/autocomplete/recipient/",
        "/api/v2/autocomplete/city/",
        "/api/v2/autocomplete/location/"
    ]
)
def test_autocomplete_limit_validation(client: Client, endpoint: str):
    body = {"search_text": "testing", "limit": 501}
    response = client.post(endpoint, content_type="application/json", data=json.dumps(body))
    response_data = response.json()

    assert response.status_code == 400
    #assert response_data.get("detail") == "Field 'limit' value '501' is above max '500'"
