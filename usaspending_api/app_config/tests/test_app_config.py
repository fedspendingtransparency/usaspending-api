import sys
from unittest.mock import patch

import pytest
from pprint import pprint
from usaspending_api.app_config import APP_CONFIG, _load_app_config
from usaspending_api.app_config.default import DefaultAppConfig
from usaspending_api.app_config.lcl import LocalAppConfig


def test_app_config_values():
    pprint(APP_CONFIG.dict())
    print(APP_CONFIG.POSTGRES_DSN)
    print(str(APP_CONFIG.POSTGRES_DSN))


def test_cannot_instantiate_default_settings():
    with pytest.raises(NotImplementedError):
        DefaultAppConfig()


def test_can_instantiate_non_default_settings():
    LocalAppConfig()


def test_app_env_code_for_non_default_app_env():
    # Unit tests should fall back to the local app env, with "lcl" code
    assert APP_CONFIG.APP_ENV == "lcl"
    assert APP_CONFIG.APP_ENV == LocalAppConfig.APP_ENV


def test_override_with_constructor_kwargs():
    cfg = LocalAppConfig()
    assert cfg.APP_NAME == "USAspending API"
    cfg = LocalAppConfig(APP_NAME="Unit Test for USAspending API")
    assert cfg.APP_NAME == "Unit Test for USAspending API"


def test_override_with_command_line_args():
    assert APP_CONFIG.APP_NAME == "USAspending API"
    test_args = ["dummy_program", "--config", "APP_NAME=test_override_with_command_line_args"]
    with patch.object(sys, "argv", test_args):
        _load_app_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        app_cfg_copy = _load_app_config()
        assert app_cfg_copy.APP_NAME == "test_override_with_command_line_args"
    # Ensure the official APP_CONFIG is unchanged
    assert APP_CONFIG.APP_NAME == "USAspending API"


def test_override_multiple_with_command_line_args():
    assert APP_CONFIG.APP_NAME == "USAspending API"
    original_postgres_port = APP_CONFIG.POSTGRES_PORT
    test_args = [
        "dummy_program",
        "--config",
        "APP_NAME=test_override_multiple_with_command_line_args " "POSTGRES_PORT=123456789",
    ]
    with patch.object(sys, "argv", test_args):
        _load_app_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        app_cfg_copy = _load_app_config()
        assert app_cfg_copy.APP_NAME == "test_override_multiple_with_command_line_args"
        assert app_cfg_copy.POSTGRES_PORT == "123456789"
    # Ensure the official APP_CONFIG is unchanged
    assert APP_CONFIG.APP_NAME == "USAspending API"
    assert APP_CONFIG.POSTGRES_PORT == original_postgres_port
