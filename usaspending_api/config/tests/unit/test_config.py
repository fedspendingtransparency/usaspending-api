import shutil
import sys
from unittest.mock import patch

import re
import os
import pytest
from pathlib import Path
from pprint import pprint
from pydantic import validator, root_validator, PostgresDsn, SecretStr
from pydantic.fields import ModelField
from pydantic.error_wrappers import ValidationError

from usaspending_api.config import CONFIG, _load_config
from usaspending_api.config.envs import ENV_CODE_VAR
from usaspending_api.config.envs.default import _PROJECT_ROOT_DIR
from usaspending_api.config.utils import (
    eval_default_factory,
    FACTORY_PROVIDED_VALUE,
    eval_default_factory_from_root_validator,
    ENV_SPECIFIC_OVERRIDE,
)
from usaspending_api.config.envs.default import DefaultConfig
from usaspending_api.config.envs.local import LocalConfig
from unittest import mock

_ENV_VAL = "component_name_set_in_env"


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

    UNITTEST_CFG_I = "UNITTEST_CFG_I"
    UNITTEST_CFG_J = "UNITTEST_CFG_J"
    # See if these are stuck with defaults (eagerly evaluated) or late-bind to the values if the values are replaced
    # by env vars
    UNITTEST_CFG_K = UNITTEST_CFG_I + ":" + UNITTEST_CFG_J
    UNITTEST_CFG_L = lambda _: _UnitTestBaseConfig.UNITTEST_CFG_I + ":" + _UnitTestBaseConfig.UNITTEST_CFG_J  # noqa

    # Start trying to use validators (aka pre/post processors)
    UNITTEST_CFG_M: str = "UNITTEST_CFG_M"
    UNITTEST_CFG_N: str = "UNITTEST_CFG_N"
    # See if these are now replaced with the values of the result of the validator that references this field
    UNITTEST_CFG_O: str = UNITTEST_CFG_M + ":" + UNITTEST_CFG_N

    @validator("UNITTEST_CFG_O")
    def _UNITTEST_CFG_O(cls, v, values):
        return values["UNITTEST_CFG_M"] + ":" + values["UNITTEST_CFG_N"]

    UNITTEST_CFG_P: str = "UNITTEST_CFG_P"
    UNITTEST_CFG_Q: str = "UNITTEST_CFG_Q"
    UNITTEST_CFG_R: str = UNITTEST_CFG_P + ":" + UNITTEST_CFG_Q

    @validator("UNITTEST_CFG_R")
    def _UNITTEST_CFG_R(cls, v, values):
        return values["UNITTEST_CFG_P"] + ":" + values["UNITTEST_CFG_Q"]

    UNITTEST_CFG_S: str = "UNITTEST_CFG_S"
    UNITTEST_CFG_T: str = "UNITTEST_CFG_T"
    UNITTEST_CFG_U: str = None  # DO NOT SET. Value derived from validator (aka pre/post-processor) func below or env

    @validator("UNITTEST_CFG_U")
    def _UNITTEST_CFG_U(cls, v, values, field: ModelField):
        def factory_func():
            return values["UNITTEST_CFG_S"] + ":" + values["UNITTEST_CFG_T"]

        return eval_default_factory(cls, v, values, field, factory_func)

    UNITTEST_CFG_V: str = "UNITTEST_CFG_V"
    UNITTEST_CFG_W: str = "UNITTEST_CFG_W"
    UNITTEST_CFG_X: str = None  # DO NOT SET. Value derived from validator (aka pre/post-processor) func below or env

    @validator("UNITTEST_CFG_X")
    def _UNITTEST_CFG_X(cls, v, values, field: ModelField):
        def factory_func():
            return values["UNITTEST_CFG_V"] + ":" + values["UNITTEST_CFG_W"]

        return eval_default_factory(cls, v, values, field, factory_func)

    # Value derived from validator (aka pre/post-processor) func below or overridden by env or subclass
    UNITTEST_CFG_Y: str = FACTORY_PROVIDED_VALUE

    @validator("UNITTEST_CFG_Y")
    def _UNITTEST_CFG_Y(cls, v, values, field: ModelField):
        def factory_func():
            return values["UNITTEST_CFG_V"] + ":" + values["UNITTEST_CFG_W"]

        return eval_default_factory(cls, v, values, field, factory_func)

    # Value derived from validator (aka pre/post-processor) func below or overridden by env or subclass
    UNITTEST_CFG_Z: str = FACTORY_PROVIDED_VALUE

    UNITTEST_CFG_AA: str = "UNITTEST_CFG_AA"
    UNITTEST_CFG_AB: str = "UNITTEST_CFG_AB"
    UNITTEST_CFG_AC: str = "UNITTEST_CFG_AC"
    UNITTEST_CFG_AD: str = "UNITTEST_CFG_AD"

    @validator("UNITTEST_CFG_Z")
    def _UNITTEST_CFG_Z(cls, v, values, field: ModelField):
        def factory_func():
            return values["UNITTEST_CFG_V"] + ":" + values["UNITTEST_CFG_W"]

        return eval_default_factory(cls, v, values, field, factory_func)

    UNITTEST_CFG_AE: str = "UNITTEST_CFG_AE"
    UNITTEST_CFG_AF: str = "UNITTEST_CFG_AF"
    UNITTEST_CFG_AG: str = "UNITTEST_CFG_AG"
    UNITTEST_CFG_AH: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AI: str = "UNITTEST_CFG_AI"
    UNITTEST_CFG_AJ: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AK: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AL: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AM: str = FACTORY_PROVIDED_VALUE

    @validator("UNITTEST_CFG_AH")
    def _UNITTEST_CFG_AH(cls, v, values, field: ModelField):
        def factory_func():
            return values["UNITTEST_CFG_AE"] + ":" + values["UNITTEST_CFG_AF"]

        return eval_default_factory(cls, v, values, field, factory_func)

    # Leave NO validator for UNITTEST_CFG_AI, and create a root_validator on the subclass
    # --EMPTY--

    # Use root_validator for UNITTEST_CFG_AJ on parent, and NO root_validator or overrides on child
    @root_validator
    def _UNITTEST_CFG_AJ(cls, values):
        def factory_func():
            return values["UNITTEST_CFG_AE"] + ":" + values["UNITTEST_CFG_AF"]

        return eval_default_factory_from_root_validator(cls, values, "UNITTEST_CFG_AJ", factory_func)

    # Use root_validator for UNITTEST_CFG_AK on parent, and overriding root_validator for same field on child
    @root_validator
    def _UNITTEST_CFG_AK(cls, values):
        def factory_func():
            return values["UNITTEST_CFG_AE"] + ":" + values["UNITTEST_CFG_AF"]

        return eval_default_factory_from_root_validator(cls, values, "UNITTEST_CFG_AK", factory_func)

    # Use regular validator for UNITTEST_CFG_AL on parent, and root_validator for same field on child
    @validator("UNITTEST_CFG_AL")
    def _UNITTEST_CFG_AL(cls, v, values, field: ModelField):
        def factory_func():
            return values["UNITTEST_CFG_AE"] + ":" + values["UNITTEST_CFG_AF"]

        return eval_default_factory(cls, v, values, field, factory_func)

    # Show that env vars can't be honored if not using the helper eval function. They don't take precedence over
    # root_validators
    @root_validator
    def _UNITTEST_CFG_AM(cls, values):
        values["UNITTEST_CFG_AM"] = values["UNITTEST_CFG_AE"] + ":" + values["UNITTEST_CFG_AF"]
        return values


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

    # See if this will override the validator's factory default
    UNITTEST_CFG_X = "SUB_UNITTEST_CFG_X"

    # See if this will override the parent class's validator and take precedence over it
    @validator("UNITTEST_CFG_Y")
    def _UNITTEST_CFG_Y(cls, v, values, field: ModelField):
        def factory_func():
            return "SUB_UNITTEST_CFG_Y"

        return eval_default_factory(cls, v, values, field, factory_func)

    # See if this will override the parent class's validator, even when a different name is used (spoiler: no it won't)
    @validator("UNITTEST_CFG_Z")
    def _SUB_UNITTEST_CFG_Z(cls, v, values, field: ModelField):
        def factory_func():
            return "SUB_UNITTEST_CFG_Z"

        return eval_default_factory(cls, v, values, field, factory_func)

    # See if this will override parent field default value, even if field it is validating is not redeclared on subclass
    @validator("UNITTEST_CFG_AC")
    def _UNITTEST_CFG_AC(cls, v, values, field: ModelField):
        def factory_func():
            return values["UNITTEST_CFG_AA"] + ":" + values["UNITTEST_CFG_AB"]

        return eval_default_factory(cls, v, values, field, factory_func)

    UNITTEST_CFG_AD: str = FACTORY_PROVIDED_VALUE

    @validator("UNITTEST_CFG_AD")
    def _UNITTEST_CFG_AD(cls, v, values, field: ModelField):
        def factory_func():
            return values["UNITTEST_CFG_AA"] + ":" + values["UNITTEST_CFG_AB"]

        return eval_default_factory(cls, v, values, field, factory_func)

    SUB_UNITTEST_6 = "SUB_UNITTEST_6"
    SUB_UNITTEST_7 = "SUB_UNITTEST_7"
    SUB_UNITTEST_8 = FACTORY_PROVIDED_VALUE

    # See if validator for field not defined in super class works fine to compose subclass field values
    @validator("SUB_UNITTEST_8")
    def _SUB_UNITTEST_8(cls, v, values, field: ModelField):
        def factory_func():
            return values["SUB_UNITTEST_6"] + ":" + values["SUB_UNITTEST_7"]

        return eval_default_factory(cls, v, values, field, factory_func)

    UNITTEST_CFG_AG: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AH: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AI: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AJ: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AK: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AL: str = FACTORY_PROVIDED_VALUE

    # Use root_validator for UNITTEST_CFG_AK on parent, and overriding root_validator for same field on child
    @root_validator
    def _UNITTEST_CFG_AI(cls, values):
        def factory_func():
            return values["SUB_UNITTEST_6"] + ":" + values["SUB_UNITTEST_7"]

        return eval_default_factory_from_root_validator(cls, values, "UNITTEST_CFG_AI", factory_func)

    # See if validator overriding the same validator in super class works fine to compose subclass field values
    @root_validator
    def _UNITTEST_CFG_AK(cls, values):
        def factory_func():
            return values["SUB_UNITTEST_6"] + ":" + values["SUB_UNITTEST_7"]

        return eval_default_factory_from_root_validator(cls, values, "UNITTEST_CFG_AK", factory_func)


class _UnitTestSubConfigFailFindingSubclassFieldsInValidator1(_UnitTestBaseConfig):
    ENV_CODE = "utsf1"
    COMPONENT_NAME = "Unit Test SubConfig Component - Fail finding subclass fields in Validator (1)"

    UNITTEST_CFG_AG: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AH: str = FACTORY_PROVIDED_VALUE

    # See if validator not overriding the same validator in super class works fine to compose subclass field values
    @validator("UNITTEST_CFG_AG", check_fields=False)
    def _UNITTEST_CFG_AG(cls, v, values, field: ModelField):
        def factory_func():
            return values["SUB_UNITTEST_6"] + ":" + values["SUB_UNITTEST_7"]

        return eval_default_factory(cls, v, values, field, factory_func)


class _UnitTestSubConfigFailFindingSubclassFieldsInValidator2(_UnitTestBaseConfig):
    ENV_CODE = "utsf2"
    COMPONENT_NAME = "Unit Test SubConfig Component - Fail finding subclass fields in Validator (2)"

    UNITTEST_CFG_AG: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AH: str = FACTORY_PROVIDED_VALUE

    # See if validator overriding the same validator in super class works fine to compose subclass field values
    @validator("UNITTEST_CFG_AH")
    def _UNITTEST_CFG_AH(cls, v, values, field: ModelField):
        def factory_func():
            return values["SUB_UNITTEST_6"] + ":" + values["SUB_UNITTEST_7"]

        return eval_default_factory(cls, v, values, field, factory_func)


class _UnitTestSubConfigFailFindingSubclassFieldsInValidator3(_UnitTestBaseConfig):
    ENV_CODE = "utsf3"
    COMPONENT_NAME = "Unit Test SubConfig Component - Fail finding subclass fields in Validator (3)"

    UNITTEST_CFG_AG: str = FACTORY_PROVIDED_VALUE
    UNITTEST_CFG_AH: str = FACTORY_PROVIDED_VALUE

    # See if root_validator overriding the same function annotated as a validator in super class works fine to compose
    # subclass field values
    @root_validator
    def _UNITTEST_CFG_AL(cls, values):
        def factory_func():
            return values["SUB_UNITTEST_6"] + ":" + values["SUB_UNITTEST_7"]

        return eval_default_factory_from_root_validator(cls, values, "UNITTEST_CFG_AL", factory_func)


class _UnitTestDbPartsNoneConfig(DefaultConfig):
    ENV_CODE = "utdbpn"
    USASPENDING_DB_HOST: str = None
    USASPENDING_DB_PORT: str = None
    USASPENDING_DB_NAME: str = None
    USASPENDING_DB_USER: str = None
    USASPENDING_DB_PASSWORD: SecretStr = None

    BROKER_DB_HOST: str = None
    BROKER_DB_PORT: str = None
    BROKER_DB_NAME: str = None
    BROKER_DB_USER: str = None
    BROKER_DB_PASSWORD: SecretStr = None


class _UnitTestDbPartsPlaceholderConfig(DefaultConfig):
    ENV_CODE = "utdbpp"
    USASPENDING_DB_HOST: str = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_PORT: str = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_NAME: str = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_USER: str = ENV_SPECIFIC_OVERRIDE
    USASPENDING_DB_PASSWORD: SecretStr = ENV_SPECIFIC_OVERRIDE

    BROKER_DB_HOST: str = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_PORT: str = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_NAME: str = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_USER: str = ENV_SPECIFIC_OVERRIDE
    BROKER_DB_PASSWORD: SecretStr = ENV_SPECIFIC_OVERRIDE


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
    {
        "env_type": "unittest",
        "code": _UnitTestSubConfigFailFindingSubclassFieldsInValidator1.ENV_CODE,
        "long_name": "unittest_sub",
        "description": "Unit Testing Sub Config",
        "constructor": _UnitTestSubConfigFailFindingSubclassFieldsInValidator1,
    },
    {
        "env_type": "unittest",
        "code": _UnitTestSubConfigFailFindingSubclassFieldsInValidator2.ENV_CODE,
        "long_name": "unittest_sub",
        "description": "Unit Testing Sub Config",
        "constructor": _UnitTestSubConfigFailFindingSubclassFieldsInValidator2,
    },
    {
        "env_type": "unittest",
        "code": _UnitTestSubConfigFailFindingSubclassFieldsInValidator3.ENV_CODE,
        "long_name": "unittest_sub",
        "description": "Unit Testing Sub Config",
        "constructor": _UnitTestSubConfigFailFindingSubclassFieldsInValidator3,
    },
    {
        "env_type": "unittest",
        "code": _UnitTestDbPartsNoneConfig.ENV_CODE,
        "long_name": "unittest_db_parts_none",
        "description": "Unit Testing DB Parts None Config",
        "constructor": _UnitTestDbPartsNoneConfig,
    },
    {
        "env_type": "unittest",
        "code": _UnitTestDbPartsPlaceholderConfig.ENV_CODE,
        "long_name": "unittest_db_parts_placeholder",
        "description": "Unit Testing DB Parts with Placeholders Config",
        "constructor": _UnitTestDbPartsPlaceholderConfig,
    },
]


def test_config_values():
    """Test that config values are picked up. Also convenient for eyeballing the parsed config vals when
    pytest is configurd with flags to output printed statements. Note: unlike DATABASE_URL, DATA_BROKER_DATABASE_URL
    is not required, and so it is not included in this test as it can vary depending on the local settings."""
    config_values: dict = CONFIG.dict()
    assert len(config_values) > 0
    pprint(config_values)
    pg_uri = CONFIG.DATABASE_URL
    print(CONFIG.DATABASE_URL)
    assert pg_uri is not None
    assert len(str(pg_uri)) > 0
    print(str(CONFIG.DATABASE_URL))


def test_config_loading():
    """Test the _load_config runs without error"""
    with mock.patch.dict(os.environ, {ENV_CODE_VAR: LocalConfig.ENV_CODE}):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()
        pprint(cfg.dict())


def test_database_urls_and_parts_config_populated():
    """Validate that DATABASE_URL and all USASPENDING_DB_* parts are populated after config is loaded.
    Note: unlike DATABASE_URL, DATA_BROKER_DATABASE_URL is not required, and so it is not included in this test as it
    can vary depending on the local settings."""
    assert CONFIG.DATABASE_URL is not None
    assert CONFIG.USASPENDING_DB_HOST is not None
    assert CONFIG.USASPENDING_DB_PORT is not None
    assert CONFIG.USASPENDING_DB_NAME is not None
    assert CONFIG.USASPENDING_DB_USER is not None
    assert CONFIG.USASPENDING_DB_PASSWORD is not None


def test_database_urls_only_backfills_none_parts():
    """Test that only providing a value for DATABASE_URL backfills the CONFIG.USASPENDING_DB_* parts and keeps them
    consistent. Similarly with DATA_BROKER_DATABASE_URL and CONFIG.BROKER_DB_* parts.

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values to be defaulted to None (not set)
    - Instantiate the config with a DATABASE_URL and DATA_BROKER_DATABASE_URL env vars (ONLY) set
    """
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestDbPartsNoneConfig.ENV_CODE,
            "DATABASE_URL": "postgres://dummy:pwd@foobar:12345/fresh_new_db_name",
            "DATA_BROKER_DATABASE_URL": "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker",
        },
        clear=True,
    ):
        cfg = _UnitTestDbPartsNoneConfig(_env_file=None)

        assert cfg.DATABASE_URL is not None
        assert cfg.USASPENDING_DB_HOST is not None
        assert cfg.USASPENDING_DB_PORT is not None
        assert cfg.USASPENDING_DB_NAME is not None
        assert cfg.USASPENDING_DB_USER is not None
        assert cfg.USASPENDING_DB_PASSWORD is not None

        assert cfg.USASPENDING_DB_HOST == "foobar"
        assert cfg.USASPENDING_DB_PORT == "12345"
        assert cfg.USASPENDING_DB_NAME == "fresh_new_db_name"
        assert cfg.USASPENDING_DB_USER == "dummy"
        assert cfg.USASPENDING_DB_PASSWORD.get_secret_value() == "pwd"

        assert cfg.DATA_BROKER_DATABASE_URL is not None
        assert cfg.BROKER_DB_HOST is not None
        assert cfg.BROKER_DB_PORT is not None
        assert cfg.BROKER_DB_NAME is not None
        assert cfg.BROKER_DB_USER is not None
        assert cfg.BROKER_DB_PASSWORD is not None

        assert cfg.BROKER_DB_HOST == "broker-foobar"
        assert cfg.BROKER_DB_PORT == "54321"
        assert cfg.BROKER_DB_NAME == "fresh_new_db_name_broker"
        assert cfg.BROKER_DB_USER == "broker"
        assert cfg.BROKER_DB_PASSWORD.get_secret_value() == "pass"


def test_database_url_only_backfills_placeholder_parts():
    """Test that only providing a value for DATABASE_URL backfills the CONFIG.USASPENDING_DB_* parts and keeps them
    consistent. Similarly with DATA_BROKER_DATABASE_URL and CONFIG.BROKER_DB_* parts.

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values to ENV_SPECIFIC_PLACEHOLDERs
    - Instantiate the config with a DATABASE_URL and DATA_BROKER_DATABASE_URL env vars (ONLY) set
    """
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestDbPartsPlaceholderConfig.ENV_CODE,
            "DATABASE_URL": "postgres://dummy:pwd@foobar:12345/fresh_new_db_name",
            "DATA_BROKER_DATABASE_URL": "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker",
        },
        clear=True,
    ):
        cfg = _UnitTestDbPartsPlaceholderConfig(_env_file=None)

        assert cfg.DATABASE_URL is not None
        assert cfg.USASPENDING_DB_HOST is not None
        assert cfg.USASPENDING_DB_PORT is not None
        assert cfg.USASPENDING_DB_NAME is not None
        assert cfg.USASPENDING_DB_USER is not None
        assert cfg.USASPENDING_DB_PASSWORD is not None

        assert cfg.USASPENDING_DB_HOST == "foobar"
        assert cfg.USASPENDING_DB_PORT == "12345"
        assert cfg.USASPENDING_DB_NAME == "fresh_new_db_name"
        assert cfg.USASPENDING_DB_USER == "dummy"
        assert cfg.USASPENDING_DB_PASSWORD.get_secret_value() == "pwd"

        assert cfg.DATA_BROKER_DATABASE_URL is not None
        assert cfg.BROKER_DB_HOST is not None
        assert cfg.BROKER_DB_PORT is not None
        assert cfg.BROKER_DB_NAME is not None
        assert cfg.BROKER_DB_USER is not None
        assert cfg.BROKER_DB_PASSWORD is not None

        assert cfg.BROKER_DB_HOST == "broker-foobar"
        assert cfg.BROKER_DB_PORT == "54321"
        assert cfg.BROKER_DB_NAME == "fresh_new_db_name_broker"
        assert cfg.BROKER_DB_USER == "broker"
        assert cfg.BROKER_DB_PASSWORD.get_secret_value() == "pass"


def test_database_url_none_parts_will_build_database_url_with_only_parts_set():
    """Test that if only the CONFIG.USASPENDING_DB_* parts are provided, the DATABASE_URL will be built-up from
    parts, set on the CONFIG object, and consistent with the parts. Similarly with DATA_BROKER_DATABASE_URL and
    CONFIG.BROKER_DB_* parts.

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values to be defaulted to None (not set)
    - DefaultConfig leaves DATABASE_URL and DATA_BROKER_DATABASE_URL unset, and the subclass does not set it
    - Instantiate the config with a env vars for each part
    """
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestDbPartsNoneConfig.ENV_CODE,
            "USASPENDING_DB_HOST": "foobar",
            "USASPENDING_DB_PORT": "12345",
            "USASPENDING_DB_NAME": "fresh_new_db_name",
            "USASPENDING_DB_USER": "dummy",
            "USASPENDING_DB_PASSWORD": "pwd",
            "BROKER_DB_HOST": "broker-foobar",
            "BROKER_DB_PORT": "54321",
            "BROKER_DB_NAME": "fresh_new_db_name_broker",
            "BROKER_DB_USER": "broker",
            "BROKER_DB_PASSWORD": "pass",
        },
        clear=True,
    ):
        cfg = _UnitTestDbPartsNoneConfig(_env_file=None)

        assert cfg.DATABASE_URL is not None
        assert cfg.USASPENDING_DB_HOST is not None
        assert cfg.USASPENDING_DB_PORT is not None
        assert cfg.USASPENDING_DB_NAME is not None
        assert cfg.USASPENDING_DB_USER is not None
        assert cfg.USASPENDING_DB_PASSWORD is not None

        assert cfg.USASPENDING_DB_HOST == "foobar"
        assert cfg.USASPENDING_DB_PORT == "12345"
        assert cfg.USASPENDING_DB_NAME == "fresh_new_db_name"
        assert cfg.USASPENDING_DB_USER == "dummy"
        assert cfg.USASPENDING_DB_PASSWORD.get_secret_value() == "pwd"
        assert cfg.DATABASE_URL == "postgres://dummy:pwd@foobar:12345/fresh_new_db_name"

        assert cfg.DATA_BROKER_DATABASE_URL is not None
        assert cfg.BROKER_DB_HOST is not None
        assert cfg.BROKER_DB_PORT is not None
        assert cfg.BROKER_DB_NAME is not None
        assert cfg.BROKER_DB_USER is not None
        assert cfg.BROKER_DB_PASSWORD is not None

        assert cfg.BROKER_DB_HOST == "broker-foobar"
        assert cfg.BROKER_DB_PORT == "54321"
        assert cfg.BROKER_DB_NAME == "fresh_new_db_name_broker"
        assert cfg.BROKER_DB_USER == "broker"
        assert cfg.BROKER_DB_PASSWORD.get_secret_value() == "pass"
        assert cfg.DATA_BROKER_DATABASE_URL == "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker"


def test_database_url_placeholder_parts_will_build_database_url_with_only_parts_set():
    """Test that if only the CONFIG.USASPENDING_DB_* parts are provided, the DATABASE_URL will be built-up from
    parts, set on the CONFIG object, and consistent with the parts. Similarly with DATA_BROKER_DATABASE_URL and
    CONFIG.BROKER_DB_* parts.

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values to ENV_SPECIFIC_PLACEHOLDERs
    - DefaultConfig leaves DATABASE_URL and DATA_BROKER_DATABASE_URL unset, and the subclass does not set it
    - Instantiate the config with a env vars for each part
    """
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestDbPartsPlaceholderConfig.ENV_CODE,
            "USASPENDING_DB_HOST": "foobar",
            "USASPENDING_DB_PORT": "12345",
            "USASPENDING_DB_NAME": "fresh_new_db_name",
            "USASPENDING_DB_USER": "dummy",
            "USASPENDING_DB_PASSWORD": "pwd",
            "BROKER_DB_HOST": "broker-foobar",
            "BROKER_DB_PORT": "54321",
            "BROKER_DB_NAME": "fresh_new_db_name_broker",
            "BROKER_DB_USER": "broker",
            "BROKER_DB_PASSWORD": "pass",
        },
        clear=True,
    ):
        cfg = _UnitTestDbPartsPlaceholderConfig(_env_file=None)

        assert cfg.DATABASE_URL is not None
        assert cfg.USASPENDING_DB_HOST is not None
        assert cfg.USASPENDING_DB_PORT is not None
        assert cfg.USASPENDING_DB_NAME is not None
        assert cfg.USASPENDING_DB_USER is not None
        assert cfg.USASPENDING_DB_PASSWORD is not None

        assert cfg.USASPENDING_DB_HOST == "foobar"
        assert cfg.USASPENDING_DB_PORT == "12345"
        assert cfg.USASPENDING_DB_NAME == "fresh_new_db_name"
        assert cfg.USASPENDING_DB_USER == "dummy"
        assert cfg.USASPENDING_DB_PASSWORD.get_secret_value() == "pwd"
        assert cfg.DATABASE_URL == "postgres://dummy:pwd@foobar:12345/fresh_new_db_name"

        assert cfg.DATA_BROKER_DATABASE_URL is not None
        assert cfg.BROKER_DB_HOST is not None
        assert cfg.BROKER_DB_PORT is not None
        assert cfg.BROKER_DB_NAME is not None
        assert cfg.BROKER_DB_USER is not None
        assert cfg.BROKER_DB_PASSWORD is not None

        assert cfg.BROKER_DB_HOST == "broker-foobar"
        assert cfg.BROKER_DB_PORT == "54321"
        assert cfg.BROKER_DB_NAME == "fresh_new_db_name_broker"
        assert cfg.BROKER_DB_USER == "broker"
        assert cfg.BROKER_DB_PASSWORD.get_secret_value() == "pass"
        assert cfg.DATA_BROKER_DATABASE_URL == "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker"


def test_database_url_and_parts_defined_ok_if_consistent_none_parts():
    """Test that if BOTH the CONFIG.DATABASE_URL and the CONFIG.USASPENDING_DB_* parts are provided, neither is
    built-up or backfilled, but they are validated to ensure they are equal. This should validate fine. Similarly
    with DATA_BROKER_DATABASE_URL and CONFIG.BROKER_DB_* parts.

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values to be defaulted to None (not set)
    - DefaultConfig leaves DATABASE_URL and DATA_BROKER_DATABASE_URL unset, and the subclass does not set it
    - Instantiate the config with a env vars for each part and with a DATABASE_URL and DATA_BROKER_DATABASE_URL env vars
      made up of those parts.
    """
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestDbPartsNoneConfig.ENV_CODE,
            "DATABASE_URL": "postgres://dummy:pwd@foobar:12345/fresh_new_db_name",
            "USASPENDING_DB_HOST": "foobar",
            "USASPENDING_DB_PORT": "12345",
            "USASPENDING_DB_NAME": "fresh_new_db_name",
            "USASPENDING_DB_USER": "dummy",
            "USASPENDING_DB_PASSWORD": "pwd",
            "DATA_BROKER_DATABASE_URL": "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker",
            "BROKER_DB_HOST": "broker-foobar",
            "BROKER_DB_PORT": "54321",
            "BROKER_DB_NAME": "fresh_new_db_name_broker",
            "BROKER_DB_USER": "broker",
            "BROKER_DB_PASSWORD": "pass",
        },
        clear=True,
    ):
        cfg = _UnitTestDbPartsNoneConfig(_env_file=None)

        assert cfg.DATABASE_URL is not None
        assert cfg.USASPENDING_DB_HOST is not None
        assert cfg.USASPENDING_DB_PORT is not None
        assert cfg.USASPENDING_DB_NAME is not None
        assert cfg.USASPENDING_DB_USER is not None
        assert cfg.USASPENDING_DB_PASSWORD is not None

        assert cfg.USASPENDING_DB_HOST == "foobar"
        assert cfg.USASPENDING_DB_PORT == "12345"
        assert cfg.USASPENDING_DB_NAME == "fresh_new_db_name"
        assert cfg.USASPENDING_DB_USER == "dummy"
        assert cfg.USASPENDING_DB_PASSWORD.get_secret_value() == "pwd"
        assert cfg.DATABASE_URL == "postgres://dummy:pwd@foobar:12345/fresh_new_db_name"

        assert cfg.DATA_BROKER_DATABASE_URL is not None
        assert cfg.BROKER_DB_HOST is not None
        assert cfg.BROKER_DB_PORT is not None
        assert cfg.BROKER_DB_NAME is not None
        assert cfg.BROKER_DB_USER is not None
        assert cfg.BROKER_DB_PASSWORD is not None

        assert cfg.BROKER_DB_HOST == "broker-foobar"
        assert cfg.BROKER_DB_PORT == "54321"
        assert cfg.BROKER_DB_NAME == "fresh_new_db_name_broker"
        assert cfg.BROKER_DB_USER == "broker"
        assert cfg.BROKER_DB_PASSWORD.get_secret_value() == "pass"
        assert cfg.DATA_BROKER_DATABASE_URL == "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker"


def test_database_url_and_parts_defined_ok_if_consistent_placeholder_parts():
    """Test that if BOTH the CONFIG.DATABASE_URL and the CONFIG.USASPENDING_DB_* parts are provided, neither is
    built-up or backfilled, but they are validated to ensure they are equal. This should validate fine. Similarly
    with DATA_BROKER_DATABASE_URL and CONFIG.BROKER_DB_* parts.

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values to ENV_SPECIFIC_PLACEHOLDERs
    - DefaultConfig leaves DATABASE_URL and DATA_BROKER_DATABASE_URL unset, and the subclass does not set it
    - Instantiate the config with a env vars for each part and with a DATABASE_URL and DATA_BROKER_DATABASE_URL env vars
      made up of those parts.
    """
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestDbPartsPlaceholderConfig.ENV_CODE,
            "DATABASE_URL": "postgres://dummy:pwd@foobar:12345/fresh_new_db_name",
            "USASPENDING_DB_HOST": "foobar",
            "USASPENDING_DB_PORT": "12345",
            "USASPENDING_DB_NAME": "fresh_new_db_name",
            "USASPENDING_DB_USER": "dummy",
            "USASPENDING_DB_PASSWORD": "pwd",
            "DATA_BROKER_DATABASE_URL": "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker",
            "BROKER_DB_HOST": "broker-foobar",
            "BROKER_DB_PORT": "54321",
            "BROKER_DB_NAME": "fresh_new_db_name_broker",
            "BROKER_DB_USER": "broker",
            "BROKER_DB_PASSWORD": "pass",
        },
        clear=True,
    ):
        cfg = _UnitTestDbPartsPlaceholderConfig(_env_file=None)

        assert cfg.DATABASE_URL is not None
        assert cfg.USASPENDING_DB_HOST is not None
        assert cfg.USASPENDING_DB_PORT is not None
        assert cfg.USASPENDING_DB_NAME is not None
        assert cfg.USASPENDING_DB_USER is not None
        assert cfg.USASPENDING_DB_PASSWORD is not None

        assert cfg.USASPENDING_DB_HOST == "foobar"
        assert cfg.USASPENDING_DB_PORT == "12345"
        assert cfg.USASPENDING_DB_NAME == "fresh_new_db_name"
        assert cfg.USASPENDING_DB_USER == "dummy"
        assert cfg.USASPENDING_DB_PASSWORD.get_secret_value() == "pwd"
        assert cfg.DATABASE_URL == "postgres://dummy:pwd@foobar:12345/fresh_new_db_name"

        assert cfg.DATA_BROKER_DATABASE_URL is not None
        assert cfg.BROKER_DB_HOST is not None
        assert cfg.BROKER_DB_PORT is not None
        assert cfg.BROKER_DB_NAME is not None
        assert cfg.BROKER_DB_USER is not None
        assert cfg.BROKER_DB_PASSWORD is not None

        assert cfg.BROKER_DB_HOST == "broker-foobar"
        assert cfg.BROKER_DB_PORT == "54321"
        assert cfg.BROKER_DB_NAME == "fresh_new_db_name_broker"
        assert cfg.BROKER_DB_USER == "broker"
        assert cfg.BROKER_DB_PASSWORD.get_secret_value() == "pass"
        assert cfg.DATA_BROKER_DATABASE_URL == "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker"


def test_database_url_and_parts_error_if_inconsistent_none_parts():
    """Test that if BOTH the CONFIG.DATABASE_URL and the CONFIG.USASPENDING_DB_* parts are provided,
    but their values are not consistent with each other, than the validation will catch that and throw an error

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values to be defaulted to None (not set)
    - DefaultConfig leaves DATABASE_URL unset, and the subclass does not set it
    - Instantiate the config with a env vars for each part and with a DATABASE_URL env var made up of those parts.
    - Force the values to not match
    - Iterate through each part and test it fails validation
    """
    consistent_dict = {
        ENV_CODE_VAR: _UnitTestDbPartsNoneConfig.ENV_CODE,
        "DATABASE_URL": "postgres://dummy:pwd@foobar:12345/fresh_new_db_name",
        "USASPENDING_DB_HOST": "foobar",
        "USASPENDING_DB_PORT": "12345",
        "USASPENDING_DB_NAME": "fresh_new_db_name",
        "USASPENDING_DB_USER": "dummy",
        "USASPENDING_DB_PASSWORD": "pwd",
    }
    mismatched_parts = {
        "USASPENDING_DB_HOST": "bad_host",
        "USASPENDING_DB_PORT": "990099",
        "USASPENDING_DB_NAME": "misnamed_db",
        "USASPENDING_DB_USER": "fake_user",
        "USASPENDING_DB_PASSWORD": "not_your_secret",
    }

    for part, bad_val in mismatched_parts.items():
        test_env = consistent_dict.copy()
        test_env[part] = bad_val
        with mock.patch.dict(os.environ, test_env, clear=True):
            with pytest.raises(ValidationError) as exc_info:
                _UnitTestDbPartsNoneConfig(_env_file=None)

            provided = mismatched_parts[part]
            expected = consistent_dict[part]
            if part == "USASPENDING_DB_PASSWORD":
                # The error keeps the provided password obfuscated as a SecretStr
                provided = SecretStr(provided)
                expected = "*" * len(expected) if expected else None
            expected_error = (
                f"Part: {part}, Part Value Provided: {provided}, " f"Value found in DATABASE_URL: {expected}"
            )
            assert exc_info.match(re.escape(expected_error))


def test_database_url_and_parts_error_if_inconsistent_placeholder_parts():
    """Test that if BOTH the CONFIG.DATABASE_URL and the CONFIG.USASPENDING_DB_* parts are provided,
    but their values are not consistent with each other, than the validation will catch that and throw an error

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values ENV_SPECIFIC_PLACEHOLDERs
    - DefaultConfig leaves DATABASE_URL unset, and the subclass does not set it
    - Instantiate the config with a env vars for each part and with a DATABASE_URL env var made up of those parts.
    - Force the values to not match
    - Iterate through each part and test it fails validation
    """
    consistent_dict = {
        ENV_CODE_VAR: _UnitTestDbPartsPlaceholderConfig.ENV_CODE,
        "DATABASE_URL": "postgres://dummy:pwd@foobar:12345/fresh_new_db_name",
        "USASPENDING_DB_HOST": "foobar",
        "USASPENDING_DB_PORT": "12345",
        "USASPENDING_DB_NAME": "fresh_new_db_name",
        "USASPENDING_DB_USER": "dummy",
        "USASPENDING_DB_PASSWORD": "pwd",
    }
    mismatched_parts = {
        "USASPENDING_DB_HOST": "bad_host",
        "USASPENDING_DB_PORT": "990099",
        "USASPENDING_DB_NAME": "misnamed_db",
        "USASPENDING_DB_USER": "fake_user",
        "USASPENDING_DB_PASSWORD": "not_your_secret",
    }

    for part, bad_val in mismatched_parts.items():
        test_env = consistent_dict.copy()
        test_env[part] = bad_val
        with mock.patch.dict(os.environ, test_env, clear=True):
            with pytest.raises(ValidationError) as exc_info:
                _UnitTestDbPartsPlaceholderConfig(_env_file=None)

            provided = mismatched_parts[part]
            expected = consistent_dict[part]
            if part == "USASPENDING_DB_PASSWORD":
                # The error keeps the provided password obfuscated as a SecretStr
                provided = SecretStr(provided)
                expected = "*" * len(expected) if expected else None
            expected_error = (
                f"Part: {part}, Part Value Provided: {provided}, " f"Value found in DATABASE_URL: {expected}"
            )
            assert exc_info.match(re.escape(expected_error))


def test_data_act_database_url_and_parts_error_if_inconsistent_none_parts():
    """Test that if BOTH the CONFIG.DATA_BROKER_DATABASE_URL and the CONFIG.BROKER_DB_* parts are provided,
    but their values are not consistent with each other, than the validation will catch that and throw an error

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values to be defaulted to None (not set)
    - DefaultConfig leaves DATA_BROKER_DATABASE_URL unset, and the subclass does not set it
    - Instantiate the config with a env vars for each part and with a DATA_BROKER_DATABASE_URL env var made up of those
      parts.
    - Force the values to not match
    - Iterate through each part and test it fails validation
    """
    consistent_dict = {
        ENV_CODE_VAR: _UnitTestDbPartsNoneConfig.ENV_CODE,
        "DATABASE_URL": "postgres://dummy:pwd@foobar:12345/fresh_new_db_name",
        "DATA_BROKER_DATABASE_URL": "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker",
        "BROKER_DB_HOST": "broker-foobar",
        "BROKER_DB_PORT": "54321",
        "BROKER_DB_NAME": "fresh_new_db_name_broker",
        "BROKER_DB_USER": "broker",
        "BROKER_DB_PASSWORD": "pass",
    }
    mismatched_parts = {
        "BROKER_DB_HOST": "bad_host",
        "BROKER_DB_PORT": "990099",
        "BROKER_DB_NAME": "misnamed_db",
        "BROKER_DB_USER": "fake_user",
        "BROKER_DB_PASSWORD": "not_your_secret",
    }

    for part, bad_val in mismatched_parts.items():
        test_env = consistent_dict.copy()
        test_env[part] = bad_val
        with mock.patch.dict(os.environ, test_env, clear=True):
            with pytest.raises(ValidationError) as exc_info:
                _UnitTestDbPartsNoneConfig(_env_file=None)

            provided = mismatched_parts[part]
            expected = consistent_dict[part]
            if part == "BROKER_DB_PASSWORD":
                # The error keeps the provided password obfuscated as a SecretStr
                provided = SecretStr(provided)
                expected = "*" * len(expected) if expected else None
            expected_error = (
                f"Part: {part}, Part Value Provided: {provided}, "
                f"Value found in DATA_BROKER_DATABASE_URL:"
                f" {expected}"
            )
            assert exc_info.match(re.escape(expected_error))


def test_data_act_database_url_and_parts_error_if_inconsistent_placeholder_parts():
    """Test that if BOTH the CONFIG.DATA_BROKER_DATABASE_URL and the CONFIG.BROKER_DB_* parts are provided,
    but their values are not consistent with each other, than the validation will catch that and throw an error

    - Use a FRESH (empty) set of environment variables
    - Use NO .env file
    - Build-out a new subclass of DefaultConfig, which overrides the part values ENV_SPECIFIC_PLACEHOLDERs
    - DefaultConfig leaves DATA_BROKER_DATABASE_URL unset, and the subclass does not set it
    - Instantiate the config with a env vars for each part and with a DATA_BROKER_DATABASE_URL env var made up of those
      parts.
    - Force the values to not match
    - Iterate through each part and test it fails validation
    """
    consistent_dict = {
        ENV_CODE_VAR: _UnitTestDbPartsPlaceholderConfig.ENV_CODE,
        "DATABASE_URL": "postgres://dummy:pwd@foobar:12345/fresh_new_db_name",
        "DATA_BROKER_DATABASE_URL": "postgres://broker:pass@broker-foobar:54321/fresh_new_db_name_broker",
        "BROKER_DB_HOST": "broker-foobar",
        "BROKER_DB_PORT": "54321",
        "BROKER_DB_NAME": "fresh_new_db_name_broker",
        "BROKER_DB_USER": "broker",
        "BROKER_DB_PASSWORD": "pass",
    }
    mismatched_parts = {
        "BROKER_DB_HOST": "bad_host",
        "BROKER_DB_PORT": "990099",
        "BROKER_DB_NAME": "misnamed_db",
        "BROKER_DB_USER": "fake_user",
        "BROKER_DB_PASSWORD": "not_your_secret",
    }

    for part, bad_val in mismatched_parts.items():
        test_env = consistent_dict.copy()
        test_env[part] = bad_val
        with mock.patch.dict(os.environ, test_env, clear=True):
            with pytest.raises(ValidationError) as exc_info:
                _UnitTestDbPartsPlaceholderConfig(_env_file=None)

            provided = mismatched_parts[part]
            expected = consistent_dict[part]
            if part == "BROKER_DB_PASSWORD":
                # The error keeps the provided password obfuscated as a SecretStr
                provided = SecretStr(provided)
                expected = "*" * len(expected) if expected else None
            expected_error = (
                f"Part: {part}, Part Value Provided: {provided}, "
                f"Value found in DATA_BROKER_DATABASE_URL:"
                f" {expected}"
            )
            assert exc_info.match(re.escape(expected_error))


def test_postgres_dsn_constructed_with_only_url_leaves_none_parts():
    """Validate assumptions about parts of the PostgresDsn object getting populated when constructed with only a URL.
    Assumption is that the DSN can be used as string, but the "parts" will all be ``None``"""
    pg_dsn = PostgresDsn(str(CONFIG.DATABASE_URL), scheme="postgres")

    assert pg_dsn.host is None
    assert pg_dsn.port is None
    assert pg_dsn.user is None
    assert pg_dsn.password is None
    assert pg_dsn.path is None
    assert pg_dsn.scheme is not None

    pg_dsn = PostgresDsn(str(CONFIG.DATA_BROKER_DATABASE_URL), scheme="postgres")

    assert pg_dsn.host is None
    assert pg_dsn.port is None
    assert pg_dsn.user is None
    assert pg_dsn.password is None
    assert pg_dsn.path is None
    assert pg_dsn.scheme is not None


def test_postgres_dsn_constructed_with_only_parts_is_complete_and_consistent():
    """Validate assumptions about parts of the PostgresDsn object getting populated when constructed with only a parts.
    Assumption is that the DSN used as a string is composed of the parts"""
    pg_dsn = PostgresDsn(
        url=None,
        scheme="postgres",
        host="injected_host",
        port="0000",
        user="injected_user",
        password="injected_password",
        path="/injected_db",
    )

    assert str(pg_dsn) is not None
    assert pg_dsn.host is not None
    assert pg_dsn.port is not None
    assert pg_dsn.user is not None
    assert pg_dsn.password is not None
    assert pg_dsn.path is not None
    assert pg_dsn.scheme is not None

    assert pg_dsn.host in str(pg_dsn)
    assert pg_dsn.port in str(pg_dsn)
    assert pg_dsn.user in str(pg_dsn)
    assert pg_dsn.password in str(pg_dsn)
    assert pg_dsn.path in str(pg_dsn)


def test_postgres_dsn_constructed_with_url_and_parts_can_diverge():
    """Confirm unexpected behavior about parts of the PostgresDsn object getting populated when constructed with both a
    URL and parts. Behavior is that it allows the URL and the parts to differ"""
    pg_dsn = PostgresDsn(
        url=str(CONFIG.DATABASE_URL),
        scheme="postgres",
        host="injected_host",
        port="0000",
        user="injected_user",
        password="injected_password",
        path="/injected_db",
    )

    assert pg_dsn.host is not None
    assert pg_dsn.port is not None
    assert pg_dsn.user is not None
    assert pg_dsn.password is not None
    assert pg_dsn.path is not None
    assert pg_dsn.scheme is not None

    # Confirm that the constructor allows for parts of the URL string provided to be different than the component
    # parts that are also provided (not really a good thing, but it is how it behaves)
    assert pg_dsn.host not in str(pg_dsn)
    assert pg_dsn.port not in str(pg_dsn)
    assert pg_dsn.user not in str(pg_dsn)
    assert pg_dsn.password not in str(pg_dsn)
    assert pg_dsn.path not in str(pg_dsn)


def test_cannot_instantiate_default_settings():
    with pytest.raises(NotImplementedError):
        DefaultConfig()


def test_can_instantiate_non_default_settings():
    """Test an overriding config subclass to DefaultConfig can be instantiated without error"""
    LocalConfig()


def test_env_code_for_non_default_env():
    # Unit tests should fall back to the local runtime env, with "lcl" code
    assert CONFIG.ENV_CODE == "lcl"
    assert CONFIG.ENV_CODE == LocalConfig.ENV_CODE


def test_dotenv_file_template_found(tmpdir):
    """Verifying that the way the paths to locate the .env file are valid, by way of using them to locate the
    .env.template file which will always be alongside it"""
    proj_root_dir = Path(_PROJECT_ROOT_DIR)
    env_file_template = Path(_PROJECT_ROOT_DIR / ".env.template")
    assert env_file_template.is_file()

    cfg = LocalConfig()
    env_file_path = cfg.Config.env_file
    assert env_file_path is not None
    assert env_file_path != ""
    assert str(proj_root_dir) in env_file_path


def test_override_with_dotenv_file(tmpdir):
    """Ensure that when .env files are used, they overwrite default values in the instantiated config class,
    rather than the other way around."""
    cfg = LocalConfig()
    assert cfg.COMPONENT_NAME == "USAspending API"
    dotenv_val = "a_test_verifying_dotenv_overrides_runtime_env_default_config"

    tmp_config_dir = tmpdir.mkdir("config_dir")
    dotenv_file = tmp_config_dir.join(".env")
    # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
    shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
    if Path(_PROJECT_ROOT_DIR / ".env").exists():
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
    with open(dotenv_file, "w"):
        dotenv_file.write(f"COMPONENT_NAME={dotenv_val}", "w")
    dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)
    cfg = LocalConfig(_env_file=dotenv_path)
    assert cfg.COMPONENT_NAME == dotenv_val


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_override_with_dotenv_file_for_subclass_overridden_var(tmpdir):
    """Ensure that when .env files are used, they overwrite default values in the instantiated config class,
    rather than the other way around... EVEN when that value was overrided in a subclass"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfig.ENV_CODE,
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. override even if originally defined at the grandparent config level
        assert cfg.COMPONENT_NAME == "Unit Test SubConfig Component"

        dotenv_val = "a_test_verifying_dotenv_overrides_runtime_env_default_config"
        dotenv_val_a = "dotenv_UNITTEST_CFG_A"

        tmp_config_dir = tmpdir.mkdir("config_dir")
        dotenv_file = tmp_config_dir.join(".env")
        # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
        if Path(_PROJECT_ROOT_DIR / ".env").exists():
            shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
        with open(dotenv_file, "w"):
            dotenv_file.write(f"COMPONENT_NAME={dotenv_val}\n" f"UNITTEST_CFG_A={dotenv_val_a}", "w")
        dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)

        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _UnitTestSubConfig(_env_file=dotenv_path)
        assert cfg.COMPONENT_NAME == dotenv_val
        assert cfg.UNITTEST_CFG_A == dotenv_val_a


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_override_with_dotenv_file_for_subclass_only_var(tmpdir):
    """Ensure that when .env files are used, they overwrite default values in the instantiated config class,
    rather than the other way around... EVEN when that value only exists in a subclass"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfig.ENV_CODE,
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. override even if originally defined at the grandparent config level
        assert cfg.COMPONENT_NAME == "Unit Test SubConfig Component"

        dotenv_sub_3 = "dotenv_SUB_UNITTEST_3"

        tmp_config_dir = tmpdir.mkdir("config_dir")
        dotenv_file = tmp_config_dir.join(".env")
        # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
        if Path(_PROJECT_ROOT_DIR / ".env").exists():
            shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
        with open(dotenv_file, "w"):
            dotenv_file.write(f"SUB_UNITTEST_3={dotenv_sub_3}", "w")
        dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)

        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _UnitTestSubConfig(_env_file=dotenv_path)
        assert cfg.SUB_UNITTEST_3 == dotenv_sub_3


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_override_with_dotenv_file_for_validated_var(tmpdir):
    """Ensure that when .env files are used, they overwrite default values in the instantiated config class,
    rather than the other way around... EVEN when that value is provided by a validator factory function"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestBaseConfig.ENV_CODE,
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. override even if originally defined at the grandparent config level
        assert cfg.COMPONENT_NAME == "USAspending API"

        var_name = "UNITTEST_CFG_U"
        dotenv_val = f"dotenv_{var_name}"

        tmp_config_dir = tmpdir.mkdir("config_dir")
        dotenv_file = tmp_config_dir.join(".env")
        # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
        if Path(_PROJECT_ROOT_DIR / ".env").exists():
            shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
        with open(dotenv_file, "a"):
            dotenv_file.write(f"\n{var_name}={dotenv_val}", "a")
        print(dotenv_file.read_text("utf-8"))
        dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)

        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _UnitTestBaseConfig(_env_file=dotenv_path)
        assert cfg.UNITTEST_CFG_U == dotenv_val


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_override_with_dotenv_file_for_root_validated_var(tmpdir):
    """Ensure that when .env files are used, they overwrite default values in the instantiated config class,
    rather than the other way around... EVEN when that value is provided by a root_validator factory function"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestBaseConfig.ENV_CODE,
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. override even if originally defined at the grandparent config level
        assert cfg.COMPONENT_NAME == "USAspending API"

        var_name = "UNITTEST_CFG_AJ"
        dotenv_val = f"dotenv_{var_name}"

        tmp_config_dir = tmpdir.mkdir("config_dir")
        dotenv_file = tmp_config_dir.join(".env")
        # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
        if Path(_PROJECT_ROOT_DIR / ".env").exists():
            shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
        with open(dotenv_file, "w"):
            dotenv_file.write(f"{var_name}={dotenv_val}", "w")
        dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)

        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _UnitTestBaseConfig(_env_file=dotenv_path)
        assert cfg.UNITTEST_CFG_AJ == dotenv_val


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_override_with_dotenv_file_for_subclass_overriding_validated_var(tmpdir):
    """Ensure that when .env files are used, they overwrite default values in the instantiated config class,
    rather than the other way around... EVEN when that value is provided by a subclass validator factory function that
    overrides its parent class validator factory function"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfig.ENV_CODE,
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. override even if originally defined at the grandparent config level
        assert cfg.COMPONENT_NAME == "Unit Test SubConfig Component"

        var_name = "UNITTEST_CFG_Y"
        dotenv_val = f"dotenv_{var_name}"

        tmp_config_dir = tmpdir.mkdir("config_dir")
        dotenv_file = tmp_config_dir.join(".env")
        # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
        if Path(_PROJECT_ROOT_DIR / ".env").exists():
            shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
        with open(dotenv_file, "w"):
            dotenv_file.write(f"{var_name}={dotenv_val}", "w")
        dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)

        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _UnitTestSubConfig(_env_file=dotenv_path)
        assert cfg.UNITTEST_CFG_Y == dotenv_val


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_override_with_dotenv_file_for_subclass_overriding_root_validated_var(tmpdir):
    """Ensure that when .env files are used, they overwrite default values in the instantiated config class,
    rather than the other way around... EVEN when that value is provided by a subclass root_validator factory function
    that overrides its parent class root_validator factory function"""
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfig.ENV_CODE,
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. override even if originally defined at the grandparent config level
        assert cfg.COMPONENT_NAME == "Unit Test SubConfig Component"

        var_name = "UNITTEST_CFG_AK"
        dotenv_val = f"dotenv_{var_name}"

        tmp_config_dir = tmpdir.mkdir("config_dir")
        dotenv_file = tmp_config_dir.join(".env")
        # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
        if Path(_PROJECT_ROOT_DIR / ".env").exists():
            shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
        with open(dotenv_file, "w"):
            dotenv_file.write(f"{var_name}={dotenv_val}", "w")
        dotenv_path = os.path.join(dotenv_file.dirname, dotenv_file.basename)

        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _UnitTestSubConfig(_env_file=dotenv_path)
        assert cfg.UNITTEST_CFG_AK == dotenv_val


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
    dotenv_val = "a_test_verifying_dotenv_overrides_runtime_env_default_config"

    # Now the .env file takes precedence
    tmp_config_dir = tmpdir.mkdir("config_dir")
    dotenv_file = tmp_config_dir.join(".env")
    # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
    shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
    if Path(_PROJECT_ROOT_DIR / ".env").exists():
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
    with open(dotenv_file, "w"):
        dotenv_file.write(f"COMPONENT_NAME={dotenv_val}", "w")
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
    original_aws_region = CONFIG.AWS_REGION
    test_args = [
        "dummy_program",
        "--config",
        "COMPONENT_NAME=test_override_multiple_with_command_line_args AWS_REGION=a-new-region",
    ]
    with patch.object(sys, "argv", test_args):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        app_cfg_copy = _load_config()
        assert app_cfg_copy.COMPONENT_NAME == "test_override_multiple_with_command_line_args"
        assert app_cfg_copy.AWS_REGION == "a-new-region"
    # Ensure the official CONFIG is unchanged
    assert CONFIG.COMPONENT_NAME == "USAspending API"
    assert CONFIG.AWS_REGION == original_aws_region


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
    dotenv_val = "a_test_verifying_dotenv_overrides_runtime_env_default_config"

    # Now the .env file takes precedence
    tmp_config_dir = tmpdir.mkdir("config_dir")
    dotenv_file = tmp_config_dir.join(".env")
    # Must use some of the default overrides from .env, like USASPENDING_DB_*. Fallback to .env.template if not existing
    shutil.copy(str(_PROJECT_ROOT_DIR / ".env.template"), dotenv_file)
    if Path(_PROJECT_ROOT_DIR / ".env").exists():
        shutil.copy(str(_PROJECT_ROOT_DIR / ".env"), dotenv_file)
    with open(dotenv_file, "w"):
        dotenv_file.write(f"COMPONENT_NAME={dotenv_val}", "w")
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
        # Ensure root_validators can factory-generate values
        assert cfg.UNITTEST_CFG_AJ == "UNITTEST_CFG_AE" + ":" + "UNITTEST_CFG_AF"


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
        # 8. subclass with no overriding field or overriding validator will still get the parent class validator's value
        assert cfg.UNITTEST_CFG_U == "UNITTEST_CFG_S" + ":" + "UNITTEST_CFG_T"
        # 9. Subclass-declared override will not only override parent class default, but any value provided by a
        #    validator in the parent class
        assert cfg.UNITTEST_CFG_X == "SUB_UNITTEST_CFG_X"
        # 10. Subclasses adding their own validator to a field in a parent class, USING THE SAME NAME as the validator
        # function in the parent class, will yield the subclass's validator value (overriding the parent validator)
        assert cfg.UNITTEST_CFG_Y == "SUB_UNITTEST_CFG_Y"
        # 11. Subclasses adding their own validator to a field in a parent class, USING A DIFFERENT NAME for the
        # validator than the validator function in the parent class, will end up taking the parent validator's
        # value as the "assigned" (i.e. assigned/overridden elsewhere) value
        assert cfg.UNITTEST_CFG_Z == "UNITTEST_CFG_V:UNITTEST_CFG_W"
        # 12. Subclass validators DO NOT take precedence and override parent class default IF the field is not
        # re-declared on subclass
        # NOTE: Could not find a way to avoid this in the eval_default_factory function, because pydantic hijacks the
        # name of the field and replaces it with the validator name. So can't detect if the field exists on BOTH
        # subclass and parent class to determine if its being redeclared or not.
        assert cfg.UNITTEST_CFG_AC == "UNITTEST_CFG_AC"
        # 13. Subclass validators DO take precedence and override parent class default IF the field IS RE-DECLARED on
        # subclass
        assert cfg.UNITTEST_CFG_AD == "UNITTEST_CFG_AA:UNITTEST_CFG_AB"
        # 14. See if validator for field not defined in super class works fine to compose subclass field values
        assert cfg.SUB_UNITTEST_8 == "SUB_UNITTEST_6" + ":" + "SUB_UNITTEST_7"
        # 15. See if root_validator not overriding the same validator in super class works fine to compose subclass
        # field values. NOTE: The field must be re-declared on the subclass (with FACTORY_PROVIDED_VALUE)
        assert cfg.UNITTEST_CFG_AI == "SUB_UNITTEST_6" + ":" + "SUB_UNITTEST_7"
        # 16. Ensure this hasn't changed, and is inherited (not overridden) on child class
        assert cfg.UNITTEST_CFG_AJ == "UNITTEST_CFG_AE" + ":" + "UNITTEST_CFG_AF"
        # 17. See if validator overriding the same validator in super class works fine to compose subclass field values
        assert cfg.UNITTEST_CFG_AK == "SUB_UNITTEST_6" + ":" + "SUB_UNITTEST_7"


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_new_runtime_env_overrides_config_errors_subclass_only_validated_fields():
    """Test that a KeyError is raised if a validator in the subclass for a field in the parent class tries to
    compose fields it its factory function that are only present in the subclass, in this case, when the validator
    is only present on the subclass
    """

    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfigFailFindingSubclassFieldsInValidator1.ENV_CODE,
        },
    ):
        with pytest.raises(KeyError) as exc_info:
            _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
            _load_config()

        assert "SUB_UNITTEST_6" in str(exc_info.value)


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_new_runtime_env_overrides_config_errors_subclass_only_validated_fields_override():
    """Test that a KeyError is raised if a validator in the subclass for a field in the parent class tries to
    compose fields it its factory function that are only present in the subclass, in this case, when the validator
    is present on the parent and overridden in the subclass
    """

    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfigFailFindingSubclassFieldsInValidator2.ENV_CODE,
        },
    ):
        with pytest.raises(KeyError) as exc_info:
            _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
            _load_config()

        assert "SUB_UNITTEST_6" in str(exc_info.value)


@mock.patch(
    "usaspending_api.config.ENVS",  # Recall, it needs to be patched where imported, not where it lives
    _UNITTEST_ENVS_DICTS,
)
def test_new_runtime_env_overrides_config_errors_root_validator_overriding_validator():
    """Test that a ValidationError is raised if a validator in the parent class is overridden by a root_validator in
    the child class
    """
    with mock.patch.dict(
        os.environ,
        {
            ENV_CODE_VAR: _UnitTestSubConfigFailFindingSubclassFieldsInValidator3.ENV_CODE,
        },
    ):
        with pytest.raises(ValidationError) as exc_info:
            _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
            _load_config()

        assert "root_validators cannot override validators" in str(exc_info.value)


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
            "UNITTEST_CFG_I": "ENVVAR_UNITTEST_CFG_I",
            "UNITTEST_CFG_M": "ENVVAR_UNITTEST_CFG_M",
            "UNITTEST_CFG_P": "ENVVAR_UNITTEST_CFG_P",
            "UNITTEST_CFG_R": "ENVVAR_UNITTEST_CFG_R",
            "UNITTEST_CFG_S": "ENVVAR_UNITTEST_CFG_S",
            "UNITTEST_CFG_U": "ENVVAR_UNITTEST_CFG_U",
            "UNITTEST_CFG_AJ": "ENVVAR_UNITTEST_CFG_AJ",
            "UNITTEST_CFG_AM": "ENVVAR_UNITTEST_CFG_AM",
        },
    ):
        _load_config.cache_clear()  # wipes the @lru_cache for fresh run on next call
        cfg = _load_config()

        # 1. Env var is overrides field in base, and inherited by child even if not declared/overridden in child
        assert cfg.COMPONENT_NAME == _ENV_VAL
        # 2. If field is declared as a property, AND the field's name is overridden by an ENVVAR, it
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

        assert "UNITTEST_CFG_A" in cfg.__fields__.keys()
        # These two are excluded / ignored, because they are properties (or functions or lambdas)
        assert "UNITTEST_CFG_G" not in cfg.__fields__.keys()
        assert "UNITTEST_CFG_H" not in cfg.__fields__.keys()
        assert "UNITTEST_CFG_L" not in cfg.__fields__.keys()

        # 3. If a var that is composed in the default value of another var gets replaced by an env var, it happens
        # too "late" and the composing var hangs on to the default value rather than the replaced env var value
        #    - Property fields could solve this by late-binding the values by way of a lambda (like a closure),
        #    but if it's a property, or a lambda function, it's ignored from pydantic
        assert cfg.UNITTEST_CFG_K == "UNITTEST_CFG_I:UNITTEST_CFG_J"

        # 4. If validators are used as pre/post-processors, late-binding can be achieved
        assert "UNITTEST_CFG_O" in cfg.__fields__.keys()
        assert cfg.UNITTEST_CFG_O == "ENVVAR_UNITTEST_CFG_M:UNITTEST_CFG_N"

        # 5. By default in pydantic, if validators are used as pre/post-processors, late-binding can be achieved,
        # AND that late-bound (validated) value will take precedence over an overriding env var for the validated field
        assert "UNITTEST_CFG_R" in cfg.__fields__.keys()
        assert cfg.UNITTEST_CFG_R == "ENVVAR_UNITTEST_CFG_P:UNITTEST_CFG_Q"

        # 6. Circumvent the above default behavior of pydantic, to allow an assigned or (env)
        # sourced value of a field to take precedence over the
        # late-bound result of a validator, by having the validator delegate to our
        # usaspending_api.config.utils.eval_default_factory function
        assert "UNITTEST_CFG_U" in cfg.__fields__.keys()
        assert cfg.UNITTEST_CFG_U == "ENVVAR_UNITTEST_CFG_U"

        # 7. Circumvent the below default behavior of pydantic, to allow an assigned or (env)
        # sourced value of a field to take precedence over the
        # late-bound result of a root_validator, by having the root_validator delegate to our
        # usaspending_api.config.utils.eval_default_factory_from_root_validator function
        assert "UNITTEST_CFG_AJ" in cfg.__fields__.keys()
        assert cfg.UNITTEST_CFG_AJ == "ENVVAR_UNITTEST_CFG_AJ"

        # 8. By default in pydantic, if root_validators are used as pre/post-processors, late-binding can be achieved,
        # AND that late-bound (validated) value will take precedence over an overriding env var for the validated field
        assert "UNITTEST_CFG_AM" in cfg.__fields__.keys()
        assert cfg.UNITTEST_CFG_AM == "UNITTEST_CFG_AE:UNITTEST_CFG_AF"


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
