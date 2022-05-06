import argparse
import os
import sys
from functools import lru_cache
from typing import Type, Union

from usaspending_api.config.envs import ENV_CODE_VAR, ENVS
from usaspending_api.config.envs.default import DefaultConfig
from usaspending_api.config.envs.local import LocalConfig

__all__ = [
    "CONFIG",
]

# If no ENV_CODE environment variable is set, fallback to using settings from the ENV_CODE with this code
_FALLBACK_ENV_CODE = "lcl"
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
                raise argparse.ArgumentError(
                    self,
                    f'Could not parse argument value "{values[0]}" as a ' f"space-delimited list of KEY=VALUE pairs.",
                )
            setattr(args, self.dest, config_dict)

    parser.add_argument(
        "--config",
        nargs=1,
        action=_KeyValueArgParser,
        metavar="KEY=VALUE [KEY=VALUE ...]",
        help="Provide new or overriding config var values in a space-delimited list of KEY=VALUE "
        "format following the --config arg. Values with spaces should be quoted. Multi-value "
        "or complex config entries should be passed as a JSON string surrounded with "
        "single-quotes",
    )

    config_arg = None
    if len(sys.argv) > 1:
        args, unknown_args = parser.parse_known_args()
        if args.config:
            config_arg = args.config
    return config_arg


@lru_cache()
def _load_config(env_code=None) -> Type[DefaultConfig]:
    """Compile runtime-environment-specific configuration inputs into a final collection configuration values."""
    if not env_code:
        env_code = os.environ.get(ENV_CODE_VAR, _FALLBACK_ENV_CODE)
    runtime_env = next((env for env in ENVS if env["code"] == env_code), None)
    if not runtime_env:
        raise KeyError(
            f"Runtime environment with code={env_code} not found in supported runtime ENVS dict. "
            f"Check that you are supplying the correct runtime env specifier in the {ENV_CODE_VAR} "
            f"environment variable when running this program"
        )
    cli_config_overrides = _parse_config_arg()
    return runtime_env["constructor"](**cli_config_overrides) if cli_config_overrides else runtime_env["constructor"]()


CONFIG: Type[Union[DefaultConfig, LocalConfig]] = _load_config()
