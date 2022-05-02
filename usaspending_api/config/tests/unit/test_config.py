import sys
from unittest.mock import patch

import os
import pytest
from pprint import pprint

from config import ENV_CODE_VAR
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


class _UnitTestBaseConfig(DefaultConfig):
    ENV_CODE = "utb"
    UNITTEST_CFG_A = "UNITTEST_CFG_A"
    UNITTEST_CFG_B = "UNITTEST_CFG_B"
    UNITTEST_CFG_C = "UNITTEST_CFG_C"
    UNITTEST_CFG_D = "UNITTEST_CFG_D"
    UNITTEST_CFG_E = property(lambda self: self.UNITTEST_CFG_A + ":" + self.UNITTEST_CFG_B)
    UNITTEST_CFG_F = property(lambda self: "UNITTEST_CFG_F")
    UNITTEST_CFG_G = property(lambda self: "UNITTEST_CFG_G")
    UNITTEST_CFG_H = property(lambda self: os.environ.get("UNITTEST_CFG_H", "UNITTEST_CFG_H"))


class _UnitTestSubConfig(_UnitTestBaseConfig):
    ENV_CODE = "uts"
    COMPONENT_NAME = "Unit Test SubConfig Component"  # grandparent value override
    UNITTEST_CFG_A = "SUB_UNITTEST_CFG_A"  # parent and child regular strings
    # Also, will UNITTEST_CFG_E show the original A value or the SUB A value?

    # prop evaluated as read when module loading class? Or late-eval when called?
    SUB_UNITTEST_1 = property(lambda self: self.UNITTEST_CFG_A + ":" + self.UNITTEST_CFG_D)
    UNITTEST_CFG_D = "SUB_UNITTEST_CFG_D"
    SUB_UNITTEST_2 = property(lambda self: self.UNITTEST_CFG_A + ":" + self.UNITTEST_CFG_B)

    UNITTEST_CFG_C = property(lambda self: "SUB_UNITTEST_CFG_C")  # parent not a prop, child a prop
    # Can't do the below: It throws a NameError because this name, not defined as a property, shadows the base class
    # name
    # UNITTEST_CFG_F = "SUB_UNITTEST_CFG_F"  # parent a prop, child not a prop
    UNITTEST_CFG_G = property(lambda self: "SUB_UNITTEST_CFG_G")  # parent and child both props

    SUB_UNITTEST_3 = "SUB_UNITTEST_3"
    SUB_UNITTEST_4 = "SUB_UNITTEST_4"
    SUB_UNITTEST_5 = property(lambda self: self.SUB_UNITTEST_3 + ":" + self.SUB_UNITTEST_4)


_UNITTEST_ENVS_DICTS = [
    {
        "env_type": "unittest",
        "code": _UnitTestBaseConfig.ENV_CODE,
        "long_name": "unittest_base",
        "description": "Unit Test Base Config",
        "constructor": _UnitTestBaseConfig,
    },
    {
        "env_type": "unittest",
        "code": _UnitTestSubConfig.ENV_CODE,
        "long_name": "unittest_sub",
        "description": "Unit Testing Sub Config",
        "constructor": _UnitTestSubConfig,
    },
]


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_new_runtime_env_config():
    """Test that envs with their own subclass of DefaultConfig work as expected"""
    with mock.patch.dict(os.environ, {ENV_CODE_VAR: _UnitTestBaseConfig.ENV_CODE}):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()
        assert cfg.UNITTEST_CFG_A == "UNITTEST_CFG_A"
        assert cfg.UNITTEST_CFG_B == "UNITTEST_CFG_B"
        assert cfg.UNITTEST_CFG_C == "UNITTEST_CFG_C"
        assert cfg.UNITTEST_CFG_D == "UNITTEST_CFG_D"
        assert cfg.UNITTEST_CFG_E == "UNITTEST_CFG_A" + ":" + "UNITTEST_CFG_B"
        assert cfg.COMPONENT_NAME == "USAspending API"


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_new_runtime_env_overrides_config():
    """Test that multiple levels of subclasses of DefaultConfig override their parents' config.
    Cases documented inline"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfig.ENV_CODE,
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. Config vars like COMPONENT_NAME still override even if originally defined at the grandparent config level
        assert cfg.COMPONENT_NAME == "Unit Test SubConfig Component"
        # 2. Same-named config vars in the subclass replace the parent value
        assert cfg.UNITTEST_CFG_A == "SUB_UNITTEST_CFG_A"
        # 3. Child prop composing 2 values from parent config is late-evaluated (at call time), not evaluated as the
        # class is read-in from a module import, or construction
        assert cfg.SUB_UNITTEST_1 == "SUB_UNITTEST_CFG_A:SUB_UNITTEST_CFG_D"
        # 4. Child prop composing 2 values from parent, one of which is overridden in child, does get the overridden part
        assert cfg.SUB_UNITTEST_2 == "SUB_UNITTEST_CFG_A:UNITTEST_CFG_B"
        # 5. Child property value DOES NOT override parent var if parent is NOT ALSO declared as a property
        assert cfg.UNITTEST_CFG_C == "UNITTEST_CFG_C"
        # 6. If child and parent are BOTH properties, child's property value overrides parent
        assert cfg.UNITTEST_CFG_G == "SUB_UNITTEST_CFG_G"
        # 7. Simple composition of values in the same class works
        assert cfg.SUB_UNITTEST_5 == "SUB_UNITTEST_3:SUB_UNITTEST_4"


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_new_runtime_env_overrides_config_with_env_vars_in_play():
    """Test that multiple levels of subclasses of DefaultConfig override their parents' config AND the way that
    environment variables replace default config var value, even when overriding among classes, is as expected. Cases
    documented inline"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestBaseConfig.ENV_CODE,
            "COMPONENT_NAME": _ENV_VAL,
            "UNITTEST_CFG_F": "ENVVAR_UNITTEST_CFG_F",
            "UNITTEST_CFG_G": "ENVVAR_UNITTEST_CFG_G",
            "UNITTEST_CFG_H": "ENVVAR_UNITTEST_CFG_H",
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. Env var is overrides field in base, and inherited by child even if not declared/overridden in child
        assert cfg.COMPONENT_NAME == _ENV_VAL
        # 5. If field is declared as a property, AND the field's name is overridden by an ENVVAR, it
        # WILL NOT be overridden by the env var value. It sticks to its value.
        # WHY?? Well ...
        # In short, property fields are not supported by and ignored by (aka made invisible) pydantic
        # There are a set of values that pydantic detects and specifically ignores (doesn't track/validate/replace)
        # their field altogether. See https://github.com/samuelcolvin/pydantic/blob/v1.9.0/pydantic/main.py#L119-L122
        # for a list of those. Anything _annotated_ with these types will be ignored. Property fields,
        # declared like either of the below fall into this group.
        #     >>> @property
        #     >>> def my_prop():
        #     >>>     return some_calculated_value
        # Or
        #     >>> MY_PROP = property(lambda self: some_calculated_value)
        # So while they still WORK in pure dataclass fashion, (provide a value, override their parent class
        # properties of the same name, etc), they do not take part in things like validation or replacing with
        # environment variables.
        assert cfg.UNITTEST_CFG_G == "UNITTEST_CFG_G"
        assert cfg.UNITTEST_CFG_H == "ENVVAR_UNITTEST_CFG_H"


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_new_runtime_env_overrides_config_with_env_vars_in_play_and_subclasses():
    """Test that multiple levels of subclasses of DefaultConfig override their parents' config AND the way that
    environment variables replace default config var value, even when overriding among classes, is as expected. Cases
    documented inline"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfig.ENV_CODE,
            "COMPONENT_NAME": _ENV_VAL,
            "UNITTEST_CFG_A": "ENVVAR_UNITTEST_CFG_A",
            "UNITTEST_CFG_D": "ENVVAR_UNITTEST_CFG_D",
            "UNITTEST_CFG_G": "ENVVAR_UNITTEST_CFG_G",
            "SUB_UNITTEST_3": "ENVVAR_SUB_UNITTEST_3",
            "SUB_UNITTEST_4": "ENVVAR_SUB_UNITTEST_4",
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. Env var takes priority over Config vars like COMPONENT_NAME still override even if originally defined at the
        # grandparent config level
        assert cfg.COMPONENT_NAME == _ENV_VAL
        # 2. Env var still takes priority when Same-named config vars in the subclass replace the parent value
        assert cfg.UNITTEST_CFG_A == "ENVVAR_UNITTEST_CFG_A"
        # 3. Child prop composing 2 values from parent config is late-evaluated (at call time), not evaluated as the
        #    class is read-in from a module import, or construction ... AND when doing the late-eval, it composes the
        #    values from the environment vars that replaced the original values for the fields it is composing
        assert cfg.SUB_UNITTEST_1 == "ENVVAR_UNITTEST_CFG_A:ENVVAR_UNITTEST_CFG_D"
        # 4. Child prop composing 2 values from parent, one of which is overridden in child, AND then further
        # overriden by the environment var, does get the overridden part FROM the env var
        assert cfg.SUB_UNITTEST_2 == "ENVVAR_UNITTEST_CFG_A:UNITTEST_CFG_B"
        # 5. If child and parent are BOTH properties, AND the field's name is overridden by an ENVVAR, the child
        # WILL NOT pick up the env var value. It sticks to its value.
        # WHY?? See extensive note in previous test
        assert cfg.UNITTEST_CFG_G == "SUB_UNITTEST_CFG_G"
        # 7. Simple composition of values in the same class works
        assert cfg.SUB_UNITTEST_5 == "ENVVAR_SUB_UNITTEST_3:ENVVAR_SUB_UNITTEST_4"
