from unittest.mock import patch

import pytest


@pytest.fixture(scope="session", autouse=True)
def patch_llm_api_header():
    with patch("usaspending_api.llm.v2.auth.LLMApiKeyAuth.authenticate") as mock_llm_api_auth:
        mock_llm_api_auth.return_value = True
        yield mock_llm_api_auth
