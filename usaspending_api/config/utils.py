from typing import Any, Dict, Callable, Type

from pydantic.main import ModelMetaclass
from pydantic.fields import ModelField

# Placeholder sentinel value indicating a config var that is expected to be overridden in a runtime-env-specific
# config declaration. If this value emerges, it has not yet been set in the runtime env config and must be.
ENV_SPECIFIC_OVERRIDE = "?ENV_SPECIFIC_OVERRIDE?"

# Placeholder sentinel value indicating a config var that is expected to be overridden from a runtime env to
# accommodate a var value that is specific to an individual developer's local environment setup.
# If this value emerges, it has not yet been set in the user's config (e.g. .env file) and and must be.
USER_SPECIFIC_OVERRIDE = "?USER_SPECIFIC_OVERRIDE?"


def default_factory(
    config_class: Type[ModelMetaclass],
    assigned_or_sourced_value: Any,
    configured_vars: Dict[str, Any],
    config_var: ModelField,
    factory_func: Callable[[ModelField, Any, Any, Dict[str, Any]], Any],
):
    """A delegate function that acts as a default factory to produce/derive (or transform) a config var value

    NOTE: This functions's signature follows that of pydantic validator functions, because it is to be delegated to
    from validator functions.

    Args:
        config_var (ModelField): The pydantic setting aka field aka configuration variable for which to
            factory-derive a value

        assigned_or_sourced_value (Any): The value that pydantic found for this config var. It could be coming from
            an assignment, or from a config source, like environment variables.

        configured_vars (Dict[str, Any]): Dictionary of configuration variables that have been seen/processed (IN
            ORDER) by pydantic already. Note: due to the fact that validators are processed in the order of
            ModelFields in the model being seen, this dictionary may not be inclusive of ALL fields in the model (of
            all configuration variables), simply because they haven't been seen/processed yet.

        factory_func (Callable[[ModelField, Any, Any, Dict[str, Any]], Any]): A lambda or callable function passed in
            which will be invoked with the

        TODO: Can lambda functions use kwargs, so we pass everything to the lambda, but it only uses what it needs?
         If so how do you show that in typing?
        Example:
            >>> @validator("MY_COMPOSED_FIELD")
            >>> def _MY_COMPOSED_FIELD(cls, v, values, field: ModelField:
            >>>     factory_func = lambda c, v, values, field: return values["MY_FIELD_1"] + ":" + values["MY_FIELD_2"]
            >>>     return default_factory(cls, v, values, field, factory_func)
    """
    # This "default_value" is the value that this field was declared with in python code. Note if it is
    #  overridden in a subclass, this will be the value assigned to the field in the subclass, which could differ
    #  from any 'default' value assigned in a super class.
    default_value = config_var.default
    if default_value is not None:
        raise ValueError(
            "This type of default-factory based validator requires that the field be set to None by "
            "default. This is so that when a value is sourced or assigned elsewhere for this, "
            "it will stick,."
        )
    if assigned_or_sourced_value and assigned_or_sourced_value not in [ENV_SPECIFIC_OVERRIDE, USER_SPECIFIC_OVERRIDE]:
        # The value of this field is being explicitly set in some source (initializer param, env var, etc.)
        # So honor it and don't use the default below
        return assigned_or_sourced_value
    else:
        # Otherwise let this validator be the field's default factory
        # Lazily compose other fields' values
        # NOTE: pydantic orders annotated (even fields annotated with their typing Type, like field_name: str)
        #       BEFORE non-annotated fields. And the values dict ONLY has field values for fields it has seen,
        #       seeing them IN ORDER. So for this composition of other fields to work, the other fields MUST ALSO
        #       be type-annotated
        return factory_func(config_var, default_value, assigned_or_sourced_value, configured_vars)
