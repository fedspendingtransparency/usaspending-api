import logging
import copy

from django.utils.decorators import method_decorator

from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.validator.helpers import INVALID_TYPE_MSG, MAX_ITEMS
from usaspending_api.common.validator.helpers import SUPPORTED_TEXT_TYPES, TINY_SHIELD_SEPARATOR
from usaspending_api.common.validator.helpers import validate_array
from usaspending_api.common.validator.helpers import validate_boolean
from usaspending_api.common.validator.helpers import validate_datetime
from usaspending_api.common.validator.helpers import validate_enum
from usaspending_api.common.validator.helpers import validate_float
from usaspending_api.common.validator.helpers import validate_integer
from usaspending_api.common.validator.helpers import validate_object
from usaspending_api.common.validator.helpers import validate_text


'''

    TINYSHIELD V2


    TYPICAL USAGE

    Probably the most common pattern is to define "models" that describe the data expected from a Django or DRF
    request:

        models = [
           {'key': 'id', 'name': 'id', 'type': 'integer', 'optional': False},
           {'key': 'city', 'name': 'city', 'type': 'string', 'text_type': 'sql', 'default': None, 'optional': True},
        ]

    Then validate the request using the validate_request decorator, which gets applied to a class-based view
    and affixes itself to the 'post' function:

    e.g.,

        @validate_request(models)
        class ClassBasedViewSet(BaseViewSet):

            def post(request):
                ...
                -> use request.data here, which will have the validated request
                ...


    ALTERNATE USAGE

    If you want to use a method rather than a decorator, you can use 'validation_function' and
    pass it both the request and the model object:

    e.g.,

          class ClassBasedViewSet(BaseViewSet):

            def post(request):
                request = validation_function(request, models)
                ...
                -> use request.data here, which will have the validated request
                ...

    INPUT

    "model" is a dictionary representing a validator which is defined as follows:

        {
            name: this field doesn't seem to actually do anything, but it does need to be unique!
            key:  dict|subitem|all|split|by|pipes
            type: type of validator
                any *
                array
                boolean
                date
                datetime
                enum
                float
                integer
                object
                passthrough
                text
                schema
            models:
                {provide sub-models - used for "any" type}
            text_type:
                search
                raw
                sql
                url
                password
            default:
                the default value if no key-value is provided for this param
            allow_nulls:
                if True, a value of None is allowed
            optional:
                If False, then key-value must be present. Overrides `default`
        }

    * "any" is a special beast as it contains a collection of models, any one of which may match the value provided.
    The first model to match is the one that's used.  You could use "any" to, say, accept a field that could be either
    an integer or a string (like an internal award id vs. a generated award id).  Child models of an "any" rule are
    always optional and inherit their parent's name and key and, as such, those keys are always ignored.  For example:

        models = [
            {'key': 'id', 'name': 'id', 'type': 'any', 'optional': False, 'models': [
                {'type': 'integer'},
                {'type': 'text', 'text_type': 'search'}
            ]},
        ]

    RETURNS

    A modified Request object that has had its 'data'  attribute validated and sanitized.

'''

logger = logging.getLogger('console')

VALIDATORS = {
    'any': {
        # "any" does not use a "func", rather, it calls funcs for child models
        'required_fields': ['models'],
        'defaults': {},
    },
    'array': {
        'func': validate_array,
        'required_fields': ['array_type'],
        'defaults': {},
    },
    'boolean': {
        'func': validate_boolean,
        'required_fields': [],
        'defaults': {},
    },
    'date': {
        'func': validate_datetime,
        'required_fields': [],
        'defaults': {},
    },
    'datetime': {
        'func': validate_datetime,
        'required_fields': [],
        'defaults': {},
    },
    'enum': {
        'func': validate_enum,
        'required_fields': ['enum_values'],
        'defaults': {},
    },
    'float': {
        'func': validate_float,
        'required_fields': [],
        'defaults': {},
    },
    'integer': {
        'func': validate_integer,
        'required_fields': [],
        'defaults': {},
    },
    'object': {
        'func': validate_object,
        'required_fields': ['object_keys'],
        'defaults': {},
    },
    'passthrough': {
        'func': lambda k: k['value'],  # Allow any value provided. Not recommended for production code
        'required_fields': [],
        'defaults': {},
    },
    'schema': {
        'func': lambda k: None,
        'required_fields': [],
        'defaults': {},
    },
    'text': {
        'func': validate_text,
        'types': SUPPORTED_TEXT_TYPES,
        'required_fields': ['text_type'],
        'defaults': {
            'min': 1,
            'max': MAX_ITEMS
        }
    }
}


# Decorator
def validate_request(model_list):
    def class_based_decorator(ClassBasedView):
        def view_func(function):
            def wrap(request, *args, **kwargs):
                request = validation_function(request, model_list)
                return function(request, *args, **kwargs)
            return wrap
        ClassBasedView.post = method_decorator(view_func)(ClassBasedView.post)
        return ClassBasedView
    return class_based_decorator


# Main entrypoint
def validation_function(request, model_list):
    rules = check_models(model_list)
    new_request_data = block(request.data, rules)
    if hasattr(request.data, "_mutable"):
        mutable = request.data._mutable
        request.data._mutable = True
    for key in list(request.data.keys()):
        if key not in new_request_data.keys():
            del request.data[key]
    for key in new_request_data.keys():
        request.data[key] = new_request_data[key]
    if hasattr(request.data, "_mutable"):
        request.data._mutable = mutable
    return request


def block(request, rules):
    rules = parse_request(request, rules)
    data = enforce_rules(rules)
    return data


def check_model(model, in_any=False):
    # Confirm required fields (both baseline and type-specific) are in the model
    base_minimum_fields = ('name', 'key', 'type')

    if not all(field in model.keys() for field in base_minimum_fields):
        raise Exception('Model {} missing a base required field [{}]'.format(model, base_minimum_fields))

    if model['type'] not in VALIDATORS:
        raise Exception('Invalid model type [{}] provided in description'.format(model['type']))

    type_description = VALIDATORS[model['type']]
    required_fields = type_description['required_fields']

    for required_field in required_fields:
        if required_field not in model:
            raise Exception('Model {} missing a type required field: {}'.format(model, required_field))

    if model.get('text_type') and model['text_type'] not in SUPPORTED_TEXT_TYPES:
        msg = 'Invalid model \'{key}\': \'{text_type}\' is not a valid text_type'.format(**model)
        raise Exception(msg + ' Possible types: {}'.format(SUPPORTED_TEXT_TYPES))

    for default_key, default_value in type_description['defaults'].items():
        model[default_key] = model.get(default_key, default_value)

    model['optional'] = model.get('optional', True)

    if model['type'] == 'any':
        if in_any is True:
            raise Exception('Nested "any" rules are not supported.')
        # "any" child models are always optional and inherit their parent's name and key.
        for sub_model in model['models']:
            sub_model['name'] = model['name']
            sub_model['key'] = model['key']
            sub_model['optional'] = True
            check_model(sub_model, True)

    return model


def check_models(models):

    for model in models:
        check_model(model)

    # Check to ensure unique names for destination dictionary
    keys = [x['name'] for x in models if x['type'] != 'schema']  # ignore schema as they are schema-only
    if len(keys) != len(set(keys)):
        raise Exception('Duplicate destination keys provided. Name values must be unique')

    return models


def parse_request(request, rules):
    for item in rules:
        # Loop through the request to find the expected key
        value = request
        for subkey in item['key'].split(TINY_SHIELD_SEPARATOR):
            value = value.get(subkey, {})
        if value != {}:
            # Key found in provided request dictionary, use the value
            item['value'] = value
        elif item['optional'] is False:
            # If the value is required, raise exception since key wasn't found
            raise UnprocessableEntityException('Missing value: \'{}\' is a required field'.format(item['key']))
        elif 'default' in item:
            # If value wasn't found, and this is optional, use the default
            item['value'] = item['default']
        else:
            # This model/field is optional, no value provided, and no default value.
            # Use the "hidden" feature Ellipsis since None can be a valid value provided in the request
            item['value'] = ...
    return rules


def enforce_rules(rules):
    data = {}
    for item in rules:
        if item['value'] != ...:
            struct = item['key'].split(TINY_SHIELD_SEPARATOR)
            recurse_append(struct, data, apply_rule(item))
    return data


def apply_rule(rule):
    _return = None
    if rule.get('allow_nulls', False) and rule['value'] is None:
        _return = rule['value']
    elif rule['type'] not in ('array', 'object', 'any'):
        if rule['type'] in VALIDATORS:
            _return = VALIDATORS[rule['type']]['func'](rule)
        else:
            raise Exception('Invalid Type {} in rule'.format(rule['type']))
    # Array is a "special" type since it is a list of other types which need to be validated
    elif rule['type'] == 'array':
        _return = array_processor(rule)
    # Object is a "special" type since it is comprised of other types which need to be validated
    elif rule['type'] == 'object':
        _return = object_processor(rule)
    # Any is a "special" type since it is is really a collection of other rules.
    elif rule['type'] == 'any':
        for child_rule in rule['models']:
            child_rule['value'] = rule['value']
            try:
                # First successful rule wins.
                return apply_rule(child_rule)
            except Exception:
                pass
        # No rules succeeded.
        raise UnprocessableEntityException(INVALID_TYPE_MSG.format(
            key=rule['key'],
            value=rule['value'],
            type=', '.join(sorted([m['type'] for m in rule['models']]))
        ))
    return _return


def array_processor(rule):
    rule['array_min'] = rule.get('array_min', 1)
    rule['array_max'] = rule.get('array_max', MAX_ITEMS)
    value = VALIDATORS[rule['type']]['func'](rule)
    child_rule = copy.copy(rule)
    child_rule['type'] = rule['array_type']
    child_rule['min'] = rule.get('array_min')
    child_rule['max'] = rule.get('array_max')
    child_rule = promote_subrules(child_rule, child_rule)
    array_result = []
    for v in value:
        child_rule['value'] = v
        array_result.append(apply_rule(child_rule))
    return array_result


def object_processor(rule):
    rule['object_min'] = rule.get('object_min', 1)
    rule['object_max'] = rule.get('object_max', MAX_ITEMS)
    provided_object = VALIDATORS[rule['type']]['func'](rule)
    object_result = {}
    for k, v in rule['object_keys'].items():
        try:
            value = provided_object[k]
        except KeyError:
            if "optional" in v and v['optional'] is False:
                raise UnprocessableEntityException('Required object fields: {}'.format(k))
            else:
                continue
        # Start with the sub-rule definition and supplement with parent's key-values as needed
        child_rule = copy.copy(v)
        child_rule['key'] = rule['key']
        child_rule['value'] = value
        child_rule = promote_subrules(child_rule, v)
        object_result[k] = apply_rule(child_rule)
    return object_result


def promote_subrules(child_rule, source={}):
    param_type = child_rule['type']
    if "text_type" in source:
        child_rule['text_type'] = source['text_type']
        child_rule['min'] = source.get('text_min')
        child_rule['max'] = source.get('text_max')
    try:
        if param_type == "object":
            child_rule['object_keys'] = source['object_keys']  # TODO investigate why
            child_rule['object_min'] = source.get('object_min', 1)
            child_rule['object_max'] = source.get('object_max', MAX_ITEMS)
        if param_type == "enum":
            child_rule['enum_values'] = source['enum_values']
        if param_type == "array":
            child_rule['array_type'] = source['array_type']
            child_rule['min'] = child_rule.get('min') or source.get('array_min')
            child_rule['max'] = child_rule.get('max') or source.get('array_max')

    except KeyError as e:
        raise Exception("Invalid Rule: {} type requires {}".format(param_type, e))
    return child_rule


def recurse_append(struct, mydict, data):
    if len(struct) == 1:
        mydict[struct[0]] = data
        return mydict
    else:
        level = struct.pop(0)
        if level in mydict:
            recurse_append(struct, mydict[level], data)
        else:
            mydict[level] = {}
            recurse_append(struct, mydict[level], data)
