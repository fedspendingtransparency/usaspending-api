import datetime
import logging
import sys
import urllib

################################################################################
# TEMP FOR TESTING..... Remove before PR
sys.path.insert(0, '/Users/tonysappe/dev/fedspendingtransparency/usaspending-api/')
from django.conf import settings
settings.configure()
################################################################################

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.exceptions import UnprocessableEntityException

logger = logging.getLogger('console')

MAX_INT = sys.maxsize  # == 2^(63-1) == 9223372036854775807
MIN_INT = -sys.maxsize - 1  # == -2^(63-1) - 1 == 9223372036854775808
MAX_FLOAT = sys.float_info.max  # 1.7976931348623157e+308
MIN_FLOAT = sys.float_info.min  # 2.2250738585072014e-308


class Heimdallr():
    '''
    https://en.wikipedia.org/wiki/Heimdallr
    '''

    def __init__(self, model_list):
        self.models = self.check_models(model_list)
        self.data = {}

    def shield(self, request):
        self.parse_request(request)
        self.validate_all_models()
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
            if type_description == 'object':
                required_fields = type_description['required_fields'].keys()
            else:
                required_fields = type_description['required_fields']
            for required_field in required_fields:
                if required_field not in model:
                    raise Exception('Model {} missing a type required field: {}'.format(model, required_field))

            for default_key, default_value in type_description['defaults'].items():
                model[default_key] = model.get(default_key, default_value)

            model['optional'] = model.get('optional', False)
            # model['on_error'] = model.get('on_error', 'warn')

        # Check to ensure unique names for destination dictionary
        keys = [x['name'] for x in models]
        if len(keys) != len(set(keys)):
            raise Exception('Duplicate destination keys provided. Name values must be unique')
        return models

    def parse_request(self, request):
        for item in self.models:
            # Loop through the request to find the expected key
            value = request
            for subkey in item['key'].split('|'):
                value = value.get(subkey, {})
            if value != {}:
                # Key found in provided request dictionary, use the value
                item['value'] = value
            elif item['optional'] is False:
                # If the value is required, raise exception since key wasn't found
                raise UnprocessableEntityException('{} is required but missing from request'.format(item['name']))
            elif 'default' in item:
                # If value wasn't found, and this is optional, use the default
                item['value'] = item['default']
            else:
                # This model/field is optional, no value provided, and no default value.
                # Use the "hidden" feature Ellipsis since None can be a valid value provided in the request
                item['value'] = ...

    def validate_all_models(self):
        for item in self.models:
            if item['value'] != ...:
                self.data[item['name']] = self.validate_model(item)

    def validate_model(self, model):
        # Array is a "special" type since it is a list of other types which need to be validated
        if model['type'] == 'array':
            value = _validate_array(model)
            temp_model = {
                'type': model['array_type'],
                'key': model['key'],
                'name': model['name'],
                'min': model.get('min', None),
                'max': model.get('max', None),
                'text_type': model.get('text_type', None),
                'object_keys': model.get('object_keys', None),
            }
            array_result = []
            for v in value:
                temp_model['value'] = v
                array_result.append(self.validate_model(temp_model))
            return array_result

        # Object is a "special" type since it is comprised of other types which need to be validated
        elif model['type'] == 'object':
            provided_object = _validate_object(model)

            # for k, v in provided_object.items():
            object_result = {}
            for k, v in model['object_keys'].items():
                if k not in provided_object:
                    if 'optional' in v and v['optional'] is False:
                        raise Exception('Object {} is missing required key {}'.format(provided_object, k))
                else:
                    temp_model = {
                        'value': provided_object[k],
                        'type': v['type'],
                        'key': model['key'],
                        'name': model['name']
                    }
                    object_result[k] = self.validate_model(temp_model)

            return object_result

        elif model['type'] in VALIDATORS:
            return VALIDATORS[model['type']]['func'](model)
        else:
            raise Exception('Invalid Type {} in model'.format(model['type']))


def _validate_boolean(model):
    if str(model['value']).lower() in ('1', 't', 'true'):
        return True
    elif str(model['value']).lower() in ('0', 'f', 'false'):
        return False
    else:
        msg = 'Incorrect value {} provided for boolean field {}. Use true/false'
        raise InvalidParameterException(msg.format(model['value'], model['key']))


def _validate_integer(model):
    model['min'] = model.get('min', MIN_INT)
    model['max'] = model.get('max', MAX_INT)
    model['value'] = _verify_int_value(model['value'])
    _check_max(model)
    _check_min(model)
    return model['value']


def _validate_float(model):
    model['min'] = model.get('min', MIN_FLOAT)
    model['max'] = model.get('max', MAX_FLOAT)
    model['value'] = _verify_float_value(model['value'])
    _check_max(model)
    _check_min(model)
    return model['value']


def _validate_datetime(model):
    # Utilizing the Python datetime strptime since format errors are already provided
    dt_format = '%Y-%m-%dT%H:%M:%S'
    if len(model['value']) == 10 or model['type'] == 'date':
        dt_format = '%Y-%m-%d'
    elif len(model['value']) > 19:
        dt_format = '%Y-%m-%dT%H:%M:%SZ'
    try:
        value = datetime.datetime.strptime(model['value'], dt_format)
    except ValueError as e:
        error_message = 'Value {} is invalid or does not conform to format ({})'.format(model['value'], dt_format)
        raise InvalidParameterException(error_message)

    if model['type'] == 'date':
        return value.date().isoformat()
    return value.isoformat() + 'Z'  # adding in "zulu" timezone to keep the datetime UTC. Can switch to "+0000"


def _validate_array(model):
    model['min'] = model.get('min', MIN_INT)
    model['max'] = model.get('max', MAX_INT)
    value = model['value']
    if type(value) is not list:
        raise InvalidParameterException()
    _check_max(model)
    _check_min(model)
    return value


def _validate_enum(model):
    value = model['value']
    if value not in model['enum_values']:
        raise InvalidParameterException('Value {} is in enumerated value list [{}]'.format(value, model['enum_values']))
    return value


def _validate_object(model):
    provided_object = model['value']

    if type(provided_object) is not dict:
        raise InvalidParameterException('Value "{}" is not an object'.format(provided_object))

    for field in provided_object.keys():
        if field not in model['object_keys'].keys():
            raise InvalidParameterException('Unexpected field "{}" in parameter {}'.format(field, model['key']))

    for key, value in model['object_keys'].items():
        if key not in provided_object:
            if 'optional' in value and value['optional'] is True:
                continue
            else:
                raise UnprocessableEntityException('Required object fields: {}'.format(model['object_keys'].keys()))

    return provided_object


def _validate_text(model):
    model['min'] = model.get('min', 1)
    model['max'] = model.get('max', MAX_INT)
    model['value'] = str(model['value'])
    _check_max(model)
    _check_min(model)
    search_remap = {
        ord('\t'): None,
        ord('\f'): None,
        ord('\r'): None,
        ord('\n'): None,
        ord('.'): None,
    }
    text_type = model['text_type']
    if text_type == 'raw':
        val = model['value']
    elif text_type == 'sql':
        logger.warn('text_type "sql" not implemented')
        val = model['value']
    elif text_type == 'url':
        val = urllib.parse.quote_plus(model['value'])
    elif text_type == 'password':
        logger.warn('text_type "password" not implemented')
        val = model['value']
    elif text_type == 'search':
        val = model['value'].translate(search_remap).strip()
    else:
        raise InvalidParameterException('{} is not a valid text type'.format(text_type))
    return val


def _check_max(model):
    value = model['value']
    if model['type'] in ('integer', 'float'):
        if value > model['max']:
            raise UnprocessableEntityException('Value {} is above maximum [{}]'.format(value, model['max']))

    if model['type'] in ('text', 'array', 'object', 'enum'):
        if len(value) > model['max']:
            raise UnprocessableEntityException('Value {} is too long. Maximum [{}]'.format(value, model['max']))


def _check_min(model):
    value = model['value']
    if model['type'] in ('integer', 'float'):
        if value < model['min']:
            raise UnprocessableEntityException('Value {} is below minimum [{}]'.format(value, model['min']))

    if model['type'] in ('text', 'array'):
        if len(value) < model['min']:
            raise UnprocessableEntityException('Value {} is below minimum [{}]'.format(value, model['min']))


def _verify_int_value(value):
    try:
        return int(value)
    except Exception as e:
        raise InvalidParameterException('Invalid value is not an integer: {}'.format(value))


def _verify_float_value(value):
    try:
        return float(value)
    except Exception as e:
        raise InvalidParameterException('Invalid value is not a float: {}'.format(value))


VALIDATORS = {
    'boolean': {
        'func': _validate_boolean,
        'required_fields': [],
        'defaults': {},
    },
    'integer': {
        'func': _validate_integer,
        'required_fields': [],
        'defaults': {},
    },
    'text': {
        'func': _validate_text,
        'types': ['search', 'raw', 'sql', 'url', 'password'],
        'required_fields': ['text_type'],
        'defaults': {
            'min': 1,
            'max': 500
        }
    },
    'float': {
        'func': _validate_float,
        'required_fields': [],
        'defaults': {},
    },
    'date': {
        'func': _validate_datetime,
        'required_fields': [],
        'defaults': {},
    },
    'datetime': {
        'func': _validate_datetime,
        'required_fields': [],
        'defaults': {},
    },
    'enum': {
        'func': _validate_enum,
        'required_fields': ['enum_values'],
        'defaults': {},
    },
    'object': {
        'func': _validate_object,
        'required_fields': ['object_keys'],
        'defaults': {},
    },
    'array': {
        # "special case" since an array is a list of other types
        'required_fields': ['array_type'],
        'defaults': {},
    },
    'passthrough': {
        'func': lambda k: k['value'],  # Allow any value provided. Not recommended for production code
        'required_fields': [],
        'defaults': {},
    }
}
