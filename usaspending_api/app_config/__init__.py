import os
import sys
import argparse

from functools import lru_cache
from typing import Type

from usaspending_api.app_config.app_envs import APP_ENV_VAR, APP_ENVS
from usaspending_api.app_config.default import DefaultAppConfig

__all__ = [
    "APP_CONFIG",
]

# If no APP_ENV environment variable is set, fallback to using settings from the APP_ENV with this code
_FALLBACK_APP_ENV_CODE = "lcl"
_CLI_CONFIG_ARG = "config"


def _parse_config_arg() -> dict:
    parser = argparse.ArgumentParser()

    class _KeyValueArgParser(argparse.Action):
        """
        argparse action to split an argument into KEY=VALUE form
        on the first = and append to a dictionary.
        """

        def __call__(self, parser, args, values, option_string=None):
            if len(values) != 1:
                raise argparse.ArgumentError(self, f"Could not parse --config value(s) given: {values}")
            config_dict = getattr(args, self.dest) or {}
            try:
                kv_list = values[0].split(" ")
                for kv_pair in kv_list:
                    k, v = kv_pair.split("=", 2)
                    config_dict[k] = v
            except ValueError:
                raise argparse.ArgumentError(self, f"Could not parse argument value \"{values[0]}\" as a "
                                                   f"space-delimited list of KEY=VALUE pairs.")
            setattr(args, self.dest, config_dict)

    parser.add_argument("--config",
                          nargs=1,
                          action=_KeyValueArgParser,
                          metavar="KEY=VALUE [KEY=VALUE ...]",
                          help="Provide new or overriding app config values in a space-delimited list of KEY=VALUE "
                               "format following the --config arg. Values with spaces should be quoted. Multi-value "
                               "or complex config entries should be passed as a JSON string surrounded with "
                               "single-quotes")

    config_arg = None
    if len(sys.argv) > 1:
        args, unknown_args = parser.parse_known_args()
        if args.config:
            config_arg = args.config
    return config_arg


@lru_cache()
def _load_app_config(app_env_code=None) -> Type[DefaultAppConfig]:
    """Compile application-environment-specific configuration inputs into a final collection of application
    configuration settings.
    """
    if not app_env_code:
        app_env_code = os.environ.get(APP_ENV_VAR, _FALLBACK_APP_ENV_CODE)
    app_env = next((app_env for app_env in APP_ENVS if app_env["code"] == app_env_code), None)
    if not app_env:
        raise KeyError(
            f"App environment with code={app_env_code} not found in supported APP_ENVS. "
            f"Check that you are supplying the correct app env specifier in the {APP_ENV_VAR} "
            f"environment variable when running this program"
        )
    cli_config_overrides = _parse_config_arg()
    return app_env["constructor"](**cli_config_overrides) if cli_config_overrides else app_env["constructor"]()


APP_CONFIG: Type[DefaultAppConfig] = _load_app_config()
