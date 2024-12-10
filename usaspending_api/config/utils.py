from typing import Any, Dict, Callable, TypeVar, Union
from urllib.parse import ParseResult, urlparse, parse_qs

from pydantic import BaseSettings, SecretStr
from pydantic.fields import ModelField

# Placeholder sentinel value indicating a config var that is expected to be overridden in a runtime-env-specific
# config declaration. If this value emerges, it has not yet been set in the runtime env config and must be.
ENV_SPECIFIC_OVERRIDE = "__ENV_SPECIFIC_OVERRIDE__"

# Placeholder sentinel value indicating a config var that is expected to be overridden from a runtime env to
# accommodate a var value that is specific to an individual developer's local environment setup.
# If this value emerges, it has not yet been set in the user's config (e.g. .env file) and and must be.
USER_SPECIFIC_OVERRIDE = "__USER_SPECIFIC_OVERRIDE__"

# Placeholder sentinel value indicating that a default should NOT be provided for a field, because its default value
# is provided by a factory function (likely one provided within a function annotated with @pydantic.validator)
FACTORY_PROVIDED_VALUE = "__FACTORY_PROVIDED_VALUE__"

CONFIG_VAR_PLACEHOLDERS = [ENV_SPECIFIC_OVERRIDE, USER_SPECIFIC_OVERRIDE, FACTORY_PROVIDED_VALUE]

TBaseSettings = TypeVar("TBaseSettings", bound=BaseSettings)


def unveil(possible_secret_str: Union[Any, SecretStr]) -> Any:
    """A way to compare SecretStr wrappers to regular strings"""
    if isinstance(possible_secret_str, SecretStr):
        return possible_secret_str.get_secret_value()
    return possible_secret_str


def eval_default_factory(
    config_class: TBaseSettings,
    assigned_or_sourced_value: Any,
    configured_vars: Dict[str, Any],
    config_var: ModelField,
    factory_func: Callable,
):
    """A delegate function that acts as a default factory to produce/derive (or transform) a config var value

    NOTE: This functions's signature follows that of pydantic validator functions, because it is to be delegated to
    from validator functions.

    Args:
        config_class (TBaseSettings): The class inheriting from pydantic.BaseSettings who is being instantiated. It
            is NOT (necessarily) the class on which this config_var (or its validator) resides.

        config_var (ModelField): The pydantic setting aka field aka configuration variable for which to
            factory-derive a value

        assigned_or_sourced_value (Any): The value that pydantic found for this config var. It could be coming from
            an assignment, or from a config source, like environment variables.

        configured_vars (Dict[str, Any]): Dictionary of configuration variables that have been seen/processed (IN
            ORDER) by pydantic already. Note: due to the fact that validators are processed in the order of
            ModelFields in the model being seen, this dictionary may not be inclusive of ALL fields in the model (of
            all configuration variables), simply because they haven't been seen/processed yet.

        factory_func (Callable): A lambda or callable function passed in
            which will be invoked to produce a value for the field. The provided factory function is assumed to be a
            closure, enclosing variable values it needs in its scoped defniition so they don't need to be passed in.
            See Example below for a no-param lambda closure

        Example:
            >>> @validator("MY_COMPOSED_FIELD")
            >>> def _MY_COMPOSED_FIELD(cls, v, values, field: ModelField):
            >>>     def factory_func():
            >>>         return values["MY_FIELD_1"] + ":" + values["MY_FIELD_2"]
            >>>     return eval_default_factory(cls, v, values, field, factory_func)
    """
    # This "default_value" is the value that this field was declared with in python code. Note if it is
    #  overridden in a subclass, this will be the value assigned to the field in the subclass, which could differ
    #  from any 'default' value assigned in a super class.
    default_value = config_var.default

    # Get any parent config class fields, to determine if this field may be overriding it
    overridable_config_base_classes = [
        bc for bc in config_class.__bases__ if issubclass(bc, BaseSettings) and "__fields__" in dir(bc)
    ]
    overridable_config_fields = {k for bc in overridable_config_base_classes for k in bc.__fields__.keys()}
    is_override = config_var.name in overridable_config_fields

    if not is_override and default_value is not None and unveil(default_value) not in CONFIG_VAR_PLACEHOLDERS:
        raise ValueError(
            f'The "{config_var.name}" field, which is tied to a default-factory-based validator, must have its '
            f"default value set to None, "
            f"or to one of the CONFIG_VAR_PLACEHOLDERS. This is so that when a value is sourced or assigned "
            f"elsewhere for this field, it can easily be identified as a non-default value, and will take "
            f"precedence as the new value."
        )

    if (
        unveil(assigned_or_sourced_value) != unveil(default_value)
        and unveil(assigned_or_sourced_value) not in CONFIG_VAR_PLACEHOLDERS
    ):
        # The value of this field is being explicitly set DIFFERENT than its default value
        # in some source (initializer param, env var, etc.)
        # So honor it and don't use the default below
        return assigned_or_sourced_value

    if is_override and default_value and unveil(default_value) not in CONFIG_VAR_PLACEHOLDERS:
        # If this validator exists on a field (which is causing this evaluation), however that field was overridden
        # in a subclass to the class where the validator was defined, and an explicit non-None, non-placeholder value
        # is being set for that field in the overridden subclass...
        # Then the (new default) value set on the overriding subclass is next in line for precedence
        return default_value

    # Otherwise let this validator be the field's default factory
    # The provided factory function is assumed to be a closure, enclosing variable values it needs in its scoped
    # definition so they don't need to be passed in.
    # NOTE: pydantic orders annotated (even fields annotated with their typing Type, like field_name: str)
    #       BEFORE non-annotated fields. And the values dict ONLY has field values for fields it has seen,
    #       seeing them IN ORDER. So if the provided factory function references OTHER fields to produced the
    #       value for the field in question, AND the field in question is annotated, the other fields must be
    #       [type] annotated as well for their values to be accessible, or an error will raise.
    return factory_func()


def eval_default_factory_from_root_validator(
    config_class: TBaseSettings,
    configured_vars: Dict[str, Any],
    config_var_name: str,
    factory_func: Callable,
):
    """Wrapper to allow for the same eval logic when coming from a pydantic root_validator,
    which provides a different set of args.

    See Also: eval_default_factory
    """
    # Get any parent config class validators
    overridable_config_base_classes = [
        bc for bc in config_class.__bases__ if issubclass(bc, BaseSettings) and "__fields__" in dir(bc)
    ]
    base_class_validated_fields = {k for bc in overridable_config_base_classes for k in bc.__validators__.keys()}
    is_validated_in_base_class = config_var_name in base_class_validated_fields

    if is_validated_in_base_class:
        raise ValueError(
            f'root_validators cannot override validators. The "{config_var_name}" field, which is tied to a '
            f"default-factory-based root_validator, is not supported for root_validator in a subclass because its "
            f"value is produced from a parent class validator. Whether the assigned value comes from the "
            f"environment or from the parent validator cannot be distinguished. Consider making the parent validator "
            f"into a root_validator and override that."
        )

    assigned_or_sourced_value = configured_vars[config_var_name] if config_var_name in configured_vars else None
    config_var = config_class.__fields__[config_var_name]
    produced_value = eval_default_factory(
        config_class, assigned_or_sourced_value, configured_vars, config_var, factory_func
    )
    configured_vars[config_var_name] = produced_value
    return configured_vars


def parse_http_url(http_url) -> (ParseResult, str, str):
    """Use the urlparse lib to parse out parts of an HTTP(s) URL string

    Supports ``username:password`` format or if ``?username=...&password=...`` format

    Returns: A three-tuple of the URL Parts ParseResult, the username (or None), and the password (or None)
    """
    url_parts = urlparse(http_url)
    user = (
        url_parts.username if url_parts.username else parse_qs(url_parts.query)["user"][0] if url_parts.query else None
    )
    password = (
        url_parts.password
        if url_parts.password
        else parse_qs(url_parts.query)["password"][0] if url_parts.query else None
    )
    return url_parts, user, password


def parse_pg_uri(pg_uri) -> (ParseResult, str, str):
    """Use the urlparse lib to parse out parts of a PostgreSQL URI connection string

    Supports ``username:password`` format or if ``?username=...&password=...`` format

    Returns: A three-tuple of the URL Parts ParseResult, the username (or None), and the password (or None)
    """
    return parse_http_url(pg_uri)


def check_for_full_url_config(url_conf_name, values):
    return values[url_conf_name] and values[url_conf_name] not in CONFIG_VAR_PLACEHOLDERS


def backfill_url_parts_config(cls, url_conf_name, resource_conf_prefix, values):
    url_parts, username, password = parse_http_url(values[url_conf_name])
    backfill_configs = {
        f"{resource_conf_prefix}_SCHEME": lambda: url_parts.scheme,
        f"{resource_conf_prefix}_HOST": lambda: url_parts.hostname,
        f"{resource_conf_prefix}_PORT": lambda: str(url_parts.port) if url_parts.port else None,
        f"{resource_conf_prefix}_NAME": lambda: (
            url_parts.path.lstrip("/") if url_parts.path and url_parts.path != "/" else None
        ),
        f"{resource_conf_prefix}_USER": lambda: username,
        f"{resource_conf_prefix}_PASSWORD": lambda: SecretStr(password) if password else None,
    }
    # Backfill only URL CONFIG vars that are missing their value
    for config_name, transformation in backfill_configs.items():
        value = values.get(config_name, None)
        if config_name == f"{resource_conf_prefix}_PASSWORD":
            value = unveil(value)
        if value in [None] + CONFIG_VAR_PLACEHOLDERS:
            values = eval_default_factory_from_root_validator(cls, values, config_name, transformation)
    return values


def check_required_url_parts(
    error_if_missing,
    url_conf_name,
    resource_conf_prefix,
    values,
    required_parts=None,
):
    # Default required parts list if not overridden in call
    if required_parts is None:
        required_parts = ["SCHEME", "HOST", "PORT", "NAME", "USER", "PASSWORD"]
    enough_parts = False
    required_url_parts = [f"{resource_conf_prefix}_{part}" for part in required_parts]
    if not (
        all(url_part in values for url_part in required_url_parts)
        and all(values[url_part] is not None for url_part in required_url_parts)
    ):
        if error_if_missing:
            raise ValueError(
                f"Config URL {url_conf_name} was not provided and could not be built because one or more of"
                " these required parts were missing. Please check that the parts are completely configured, or"
                f" provide the complete URL for {url_conf_name} instead: {required_url_parts}"
            )
        else:
            print(
                f"Config URL {url_conf_name} was not provided and could not be built because one or more of"
                " these required parts were missing. Leaving blank for now. Please check that the parts are"
                f" completely configured, or provide a complete URL for {url_conf_name} to use it:"
                f" {required_url_parts}"
            )
    else:
        enough_parts = True
    return enough_parts


def validate_url_and_parts(url_conf_name, resource_conf_prefix, values):
    # Now validate the provided and/or built values are consistent between complete URL and URL config parts
    url_config_errors = {}
    url_parts, url_username, url_password = parse_http_url(values[url_conf_name])
    # Validate host
    if url_parts.hostname != values[f"{resource_conf_prefix}_HOST"]:
        url_config_errors[f"{resource_conf_prefix}_HOST"] = (
            values[f"{resource_conf_prefix}_HOST"],
            url_parts.hostname,
        )
    # Validate port
    if (
        (url_parts.port is not None and str(url_parts.port) != values[f"{resource_conf_prefix}_PORT"])
        or url_parts.port is None
        and values[f"{resource_conf_prefix}_PORT"] is not None
    ):
        url_config_errors[f"{resource_conf_prefix}_PORT"] = (values[f"{resource_conf_prefix}_PORT"], url_parts.port)
    # Validate resource name (path)
    if (
        (
            url_parts.path
            and url_parts.path != "/"
            and url_parts.path.lstrip("/") != values[f"{resource_conf_prefix}_NAME"]
        )
        or url_parts.path is None
        and values[f"{resource_conf_prefix}_NAME"] is not None
    ):
        url_config_errors[f"{resource_conf_prefix}_NAME"] = (
            values[f"{resource_conf_prefix}_NAME"],
            url_parts.path.lstrip("/") if url_parts.path and url_parts.path != "/" else None,
        )
    # Validate username
    if url_username != values[f"{resource_conf_prefix}_USER"]:
        url_config_errors[f"{resource_conf_prefix}_USER"] = (values[f"{resource_conf_prefix}_USER"], url_username)
    # Validate password
    if url_password != unveil(values[f"{resource_conf_prefix}_PASSWORD"]):
        # NOTE: Keeping password text obfuscated in the error output
        url_config_errors[f"{resource_conf_prefix}_PASSWORD"] = (
            values[f"{resource_conf_prefix}_PASSWORD"],
            "*" * len(url_password) if url_password is not None else None,
        )
    if len(url_config_errors) > 0:
        err_msg = (
            f"The URL config var named {url_conf_name} was provided along with one or more "
            f"{resource_conf_prefix}_* URL-part config var values, however they were not consistent. Differing "
            f"configuration sources that should match will cause confusion. Either provide all values "
            f"consistently, or only provide a complete {url_conf_name} and leave the others as None or unset, "
            f"or leave the {url_conf_name} None or unset and provide only the required URL-parts. "
            "Parts not matching:\n"
        )
        for k, v in url_config_errors.items():
            err_msg += f"\tPart: {k}, Part Value Provided: {v[0]}, Value found in {url_conf_name}: {v[1]}\n"
        raise ValueError(err_msg)
