import sys
from unittest.mock import patch

import os
import pytest
from pprint import pprint
from usaspending_api.config import CONFIG, _load_config
from usaspending_api.config.default import DefaultConfig
from usaspending_api.config.local import LocalConfig
from unittest import mock

_ENV_VAL = "component_name_set_in_env"


def test_config_values():
    pprint(CONFIG.dict())
    print(CONFIG.POSTGRES_DSN)
    print(str(CONFIG.POSTGRES_DSN))


def test_cannot_instantiate_default_settings():
    with pytest.raises(NotImplementedError):
        DefaultConfig()


def test_can_instantiate_non_default_settings():
    LocalConfig()


def test_env_code_for_non_default_env():
    # Unit tests should fall back to the local runtime env, with "lcl" code
    assert CONFIG.ENV_CODE == "lcl"
    assert CONFIG.ENV_CODE == LocalConfig.ENV_CODE


def test_override_with_dotenv_file(tmpdir):
    """Ensure that when .env files are used, they overwrite default values in the instantiated config class,
    rather than the other way around."""
    cfg = LocalConfig()
    assert cfg.COMPONENT_NAME == "USAspending API"
    tmp_config_dir = tmpdir.mkdir("config_dir")
    dotenv_file = tmp_config_dir.join(".env")
    dotenv_val = "a_test_verifying_dotenv_overrides_runtime_env_default_config"
    dotenv_file.write(f"COMPONENT_NAME={dotenv_val}")
    dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)
    cfg = LocalConfig(_env_file=dotenv_path)
    assert cfg.COMPONENT_NAME == dotenv_val


def test_override_with_env_var(tmpdir):
    """Ensure that when env vars exist, they override the default config value"""
    # Verify default if nothing overriding
    cfg = LocalConfig()
    assert cfg.COMPONENT_NAME == "USAspending API"
    # Confirm that an env var value will override the default value
    with mock.patch.dict(os.environ, {"COMPONENT_NAME": _ENV_VAL}):
        cfg = LocalConfig()
        assert cfg.COMPONENT_NAME == _ENV_VAL


def test_override_dotenv_file_with_env_var(tmpdir):
    """Ensure that when .env files are used, AND the same value is declared as an environment var, the env var takes
    precedence over the value in .env"""
    # Verify default if nothing overriding
    cfg = LocalConfig()
    assert cfg.COMPONENT_NAME == "USAspending API"

    # Now the .env file takes precedence
    tmp_config_dir = tmpdir.mkdir("config_dir")
    dotenv_file = tmp_config_dir.join(".env")
    dotenv_val = "a_test_verifying_dotenv_overrides_runtime_env_default_config"
    dotenv_file.write(f"COMPONENT_NAME={dotenv_val}")
    dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)
    cfg = LocalConfig(_env_file=dotenv_path)
    assert cfg.COMPONENT_NAME == dotenv_val

    # Now the env var takes ultimate precedence
    with mock.patch.dict(os.environ, {"COMPONENT_NAME": _ENV_VAL}):
        cfg = LocalConfig()
        assert cfg.COMPONENT_NAME == _ENV_VAL


def test_override_with_constructor_kwargs():
    cfg = LocalConfig()
    assert cfg.COMPONENT_NAME == "USAspending API"
    cfg = LocalConfig(COMPONENT_NAME="Unit Test for USAspending API")
    assert cfg.COMPONENT_NAME == "Unit Test for USAspending API"


def test_override_with_command_line_args():
    assert CONFIG.COMPONENT_NAME == "USAspending API"
    test_args = ["dummy_program", "--config", "COMPONENT_NAME=test_override_with_command_line_args"]
    with patch.object(sys, "argv", test_args):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        app_cfg_copy = _load_config()
        assert app_cfg_copy.COMPONENT_NAME == "test_override_with_command_line_args"
    # Ensure the official CONFIG is unchanged
    assert CONFIG.COMPONENT_NAME == "USAspending API"


def test_override_multiple_with_command_line_args():
    assert CONFIG.COMPONENT_NAME == "USAspending API"
    original_postgres_port = CONFIG.POSTGRES_PORT
    test_args = [
        "dummy_program",
        "--config",
        "COMPONENT_NAME=test_override_multiple_with_command_line_args " "POSTGRES_PORT=123456789",
    ]
    with patch.object(sys, "argv", test_args):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        app_cfg_copy = _load_config()
        assert app_cfg_copy.COMPONENT_NAME == "test_override_multiple_with_command_line_args"
        assert app_cfg_copy.POSTGRES_PORT == "123456789"
    # Ensure the official CONFIG is unchanged
    assert CONFIG.COMPONENT_NAME == "USAspending API"
    assert CONFIG.POSTGRES_PORT == original_postgres_port


def test_precedence_order(tmpdir):
    """Confirm all overrides happen in the expected order

    1. Default value set in DefaultConfig
    2. Is overridden by same config vars in subclass (e.g. LocalConfig(DefaultConfig))
    3. Is overridden by .env file values
    4. Is overridden by env var values
    5. Is overridden by constructor keyword args OR by command-line --config args
       - NOTE: since the --config args get used as constructor kwargs, CANNOT do both

    """
    # Verify default if nothing overriding
    cfg = LocalConfig()
    assert cfg.COMPONENT_NAME == "USAspending API"

    # Now the .env file takes precedence
    tmp_config_dir = tmpdir.mkdir("config_dir")
    dotenv_file = tmp_config_dir.join(".env")
    dotenv_val = "a_test_verifying_dotenv_overrides_runtime_env_default_config"
    dotenv_file.write(f"COMPONENT_NAME={dotenv_val}")
    dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)
    cfg = LocalConfig(_env_file=dotenv_path)
    assert cfg.COMPONENT_NAME == dotenv_val

    # Now the env var, when present, takes precedence
    with mock.patch.dict(os.environ, {"COMPONENT_NAME": _ENV_VAL}):
        cfg = LocalConfig()
        assert cfg.COMPONENT_NAME == _ENV_VAL

    # Now the keyword arg takes precedence
    with mock.patch.dict(os.environ, {"COMPONENT_NAME": _ENV_VAL}):
        kwarg_val = "component_name_set_as_a_kwarg"
        cfg = LocalConfig(COMPONENT_NAME=kwarg_val)
        assert cfg.COMPONENT_NAME == kwarg_val

    # Or if overriding via CLI, Now the CLI arg takes precedence
    cli_val = "test_override_with_command_line_args"
    test_args = ["dummy_program", "--config", f"COMPONENT_NAME={cli_val}"]
    with mock.patch.dict(os.environ, {"COMPONENT_NAME": _ENV_VAL}):
        with patch.object(sys, "argv", test_args):
            _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
            app_cfg_copy = _load_config()
            assert app_cfg_copy.COMPONENT_NAME == cli_val
