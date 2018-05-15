import logging
import copy

from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.core.validator.helpers import SUPPORTED_TEXT_TYPES
from usaspending_api.core.validator.helpers import TINY_SHIELD_SEPARATOR, MAX_ITEMS
from usaspending_api.core.validator.helpers import validate_array
from usaspending_api.core.validator.helpers import validate_boolean
from usaspending_api.core.validator.helpers import validate_datetime
from usaspending_api.core.validator.helpers import validate_enum
from usaspending_api.core.validator.helpers import validate_float
from usaspending_api.core.validator.helpers import validate_integer
from usaspending_api.core.validator.helpers import validate_object
from usaspending_api.core.validator.helpers import validate_text

logger = logging.getLogger('console')

VALIDATORS = {
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


class TinyShield:
    """
    Structure
    model- dictionary representing a validator
    {
        name:
        key: dict|subitem|all|split|by|pipes
        type: type of validator
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
        text_type:
            search
            raw
            sql
            url
            password
        default:
            the default value
    }

    Validated data is stored in self.data, which is a flat dictionary
    """

    def __init__(self, model_list):
        self.rules = self.check_models(model_list)
        self.data = {}

    def block(self, request):
        self.parse_request(request)
        self.enforce_rules()
        self.check_dependencies()
        return self.data

    def check_models(self, models):
        # Confirm required fields (both baseline and type-specific) are in the model
        base_minimum_fields = ('name', 'key', 'type')

        for model in models:
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

        # Check to ensure unique names for destination dictionary
        keys = [x['name'] for x in models if x['type'] != 'schema']  # ignore schema as they are schema-only
        if len(keys) != len(set(keys)):
            raise Exception('Duplicate destination keys provided. Name values must be unique')
        return models

    def parse_request(self, request):
        for item in self.rules:
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

    def enforce_rules(self):
        for item in self.rules:
            if item['value'] != ...:
                struct = item['key'].split(TINY_SHIELD_SEPARATOR)
                self.recurse_append(struct, self.data, self.apply_rule(item))

    def apply_rule(self, rule):
        if rule['type'] not in ('array', 'object'):
            if rule['type'] in VALIDATORS:
                return VALIDATORS[rule['type']]['func'](rule)
            else:
                raise Exception('Invalid Type {} in rule'.format(rule['type']))
        # Array is a "special" type since it is a list of other types which need to be validated
        elif rule['type'] == 'array':
            rule['array_min'] = rule.get('array_min', 1)
            rule['array_max'] = rule.get('array_max', MAX_ITEMS)
            value = VALIDATORS[rule['type']]['func'](rule)
            child_rule = copy.copy(rule)
            child_rule['type'] = rule['array_type']
            child_rule['min'] = rule.get('array_min')
            child_rule['max'] = rule.get('array_max')
            child_rule = self.promote_subrules(child_rule, child_rule)
            array_result = []
            for v in value:
                child_rule['value'] = v
                array_result.append(self.apply_rule(child_rule))
            return array_result
        # Object is a "special" type since it is comprised of other types which need to be validated
        elif rule['type'] == 'object':
            rule['object_min'] = rule.get('object_min', 1)
            rule['object_max'] = rule.get('object_max', MAX_ITEMS)
            provided_object = VALIDATORS[rule['type']]['func'](rule)
            object_result = {}
            for k, v in rule['object_keys'].items():
                try:
                    value = provided_object[k]
                except KeyError as e:
                    if "optional" in v and v['optional'] is False:
                        raise UnprocessableEntityException('Required object fields: {}'.format(k))
                    else:
                        continue
                # Start with the sub-rule definition and supplement with parent's key-values as needed
                child_rule = copy.copy(v)
                child_rule['key'] = rule['key']
                child_rule['value'] = value
                child_rule = self.promote_subrules(child_rule, v)
                object_result[k] = self.apply_rule(child_rule)
            return object_result

    def promote_subrules(self, child_rule, source={}):
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

    def recurse_append(self, struct, mydict, data):
        if len(struct) == 1:
            mydict[struct[0]] = data
            return
        else:
            level = struct.pop(0)
            if level in mydict:
                self.recurse_append(struct, mydict[level], data)
            else:
                mydict[level] = {}
                self.recurse_append(struct, mydict[level], data)
