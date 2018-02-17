import logging
import sys
import datetime
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

template = {
    'name': '',
    'key': '||||',
    'optional': [True, False],
    'type': ['integer', 'text', 'boolean', 'float', 'enum', 'array', 'date'],  # add list of dicts
    'min': None,
    'max': None,
    'on_error': ['ignore', 'raise', 'warn'],
    'default': None,
    'enum_values': [0, 3, 5, 6],  # necessary for enum
    'text_type': ['search', 'raw', 'sql', 'url', 'password'],  # useful for sanitizing text
    'array_type': ['integer', 'text', 'boolean', 'float', 'passthrough'],
}

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
        self.validate()
        return self.data

    def check_models(self, models):
        # Confirm required fields are in the model
        req_fields = ('name', 'key', 'type')
        for model in models:
            # for field in req_fields:
            if not all(key in model.keys() for key in req_fields):
                raise Exception('Model {} missing required field [{}]'.format(model, req_fields))

        # Add "safe" defaults if not provided:
        for model in models:
            model['optional'] = model.get('optional', False)
            model['on_error'] = model.get('on_error', 'warn')

            if model['type'] == 'text':
                model['text_type'] = model.get('text_type', 'search')
                model['max'] = model.get('max', 500)

            if model['type'] == 'array':
                model['array_type'] = model.get('array_type', 'text')
                if model['array_type'] == 'text':
                    model['text_type'] = model.get('text_type', 'search')

            if model['type'] == 'enum':
                if 'enum_values' not in model:
                    raise Exception('Missing "enum_values" field')

        ####
        # Add checks for values
        ####
        # Check to ensure unique names for destination dictionary
        keys = [x['name'] for x in models]
        if len(keys) != len(set(keys)):
            raise Exception('Non-unique destination keys provided. Each name must be used only once')
        return models

    def parse_request(self, request):
        for item in self.models:
            # Loop through the request to find the expected key
            value = request
            for subkey in item['key'].split('|'):
                value = value.get(subkey, {})
            if value == {}:
                if item['optional'] is False:
                    raise UnprocessableEntityException('{} is required but missing from request'.format(item['name']))
                if item['optional'] and 'default' in item:
                    item['value'] = item['default']
                else:
                    # Use the "hidden" feature Ellipsis instead of None since None can be a valid provided value
                    item['value'] = ...
            else:
                item['value'] = value

    def validate(self):
        for item in self.models:
            if item['value'] == ...:
                continue
            if item['type'] == 'integer':
                self.data[item['name']] = validate_integer(item)
            elif item['type'] == 'boolean':
                self.data[item['name']] = validate_boolean(item)
            elif item['type'] == 'text':
                self.data[item['name']] = validate_text(item)
            elif item['type'] == 'float':
                self.data[item['name']] = validate_float(item)
            elif item['type'] == 'date':
                self.data[item['name']] = validate_date(item)
            elif item['type'] == 'enum':
                self.data[item['name']] = validate_enum(item)
            elif item['type'] == 'array':
                value = validate_array(item)
                if item['array_type'] == 'integer':
                    value = [validate_integer(
                        {
                            'value': v,
                            'type': item['array_type'],
                            'min': item.get('min', None),
                            'max': item.get('max', None),
                        }
                    ) for v in value]
                if item['array_type'] == 'text':
                    value = [validate_text(
                        {
                            'value': v,
                            'min': item.get('min', None),
                            'max': item.get('max', None),
                            'type': item['array_type'],
                            'text_type': item['text_type']
                        }
                    ) for v in value]
                if item['array_type'] == 'boolean':
                    pass
                if item['array_type'] == 'float':
                    pass
                if item['array_type'] == 'passthrough':
                    pass
                self.data[item['name']] = value
            else:
                raise Exception('Invalid Type {} in model'.format(item['type']))


def validate_boolean(model):
    if model['value'] in (True, 1, '1', 'true', 'True', 'TRUE'):
        return True
    elif model['value'] in (False, 0, '0', 'false', 'False', 'FALSE'):
        return False
    else:
        msg = 'Incorrect value provided for boolean field {}. Use True/False'
        raise InvalidParameterException(msg.format(model['name']))


def validate_integer(model):
    model['min'] = model.get('min', MIN_INT)
    model['max'] = model.get('max', MAX_INT)
    val = _verify_int_value(model['value'])
    _check_max(model, val)
    _check_min(model, val)
    return val


def validate_float(model):
    model['min'] = model.get('min', MIN_FLOAT)
    model['max'] = model.get('max', MAX_FLOAT)
    val = _verify_float_value(model['value'])
    _check_max(model, val)
    _check_min(model, val)
    return val


def validate_date(model):
    value = model['value']
    sep = None
    if '-' in value:
        sep = '-'
    elif '/' in value:
        sep = '/'

    return datetime.date(int(v) for v in value.split(sep)).isoformat()


def validate_array(model):
    model['min'] = model.get('min', MIN_INT)
    model['max'] = model.get('max', MAX_INT)
    value = model['value']
    if type(value) is not list:
        raise InvalidParameterException()
    _check_max(model, value)
    _check_min(model, value)
    return value


def validate_enum(model):
    model['min'] = model.get('min', 1)
    model['max'] = model.get('max', MAX_INT)
    values = model['value']
    if type(values) is not list:
        raise InvalidParameterException()
    _check_max(model, values)
    _check_min(model, values)
    for val in values:
        if val not in model['enum_values']:
            raise InvalidParameterException('Value {} in not a valid value'.format(val))
    return values


def validate_text(model):
    model['min'] = model.get('min', 1)
    model['max'] = model.get('max', MAX_INT)
    value = model['value']
    _check_max(model, value)
    _check_min(model, value)
    search_remap = {
        ord('\t'): None,
        ord('\f'): None,
        ord('\r'): None,
        ord('\n'): None,
        ord('.'): None,
    }
    text_type = model['text_type']
    if text_type == 'raw':
        val = value
    elif text_type == 'sql':
        logger.warn('text_type "sql" not implemented')
        val = value
    elif text_type == 'url':
        val = urllib.parse.quote_plus(value)
    elif text_type == 'password':
        logger.warn('text_type "password" not implemented')
        val = value
    elif text_type == 'search':
        val = value.translate(search_remap).strip()
    else:
        raise InvalidParameterException('{} is not a valid text type'.format(text_type))
    return val


def _check_max(model, value):
    if model['type'] in ('integer', 'float'):
        if value > model['max']:
            raise UnprocessableEntityException('Value {} is above maximum [{}]'.format(value, model['max']))

    if model['type'] in ('text', 'array'):
        if len(value) > model['max']:
            raise UnprocessableEntityException('Value {} is too long. Maximum [{}]'.format(value, model['max']))


def _check_min(model, value):
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


################################################################################
# TEMP FOR TESTING..... Remove before PR
if __name__ == '__main__':
    models = [
        {'name': 'keyword', 'key': 'filters|keyword', 'type': 'text', 'optional': False},
        {'name': 'page', 'key': 'page', 'type': 'integer', 'optional': True, 'default': 1, 'min': 1},
        {'name': 'limit', 'key': 'limit', 'type': 'integer', 'optional': True, 'default': 60, 'min': 1, 'max': 100},
        {'name': 'sort', 'key': 'sort', 'type': 'text', 'optional': False},
        {'name': 'order', 'key': 'order', 'type': 'text', 'optional': False},
        {'name': 'award_type_codes', 'key': 'filters|award_type_codes', 'type': 'array', 'optional': False},
        {'name': 'fields', 'key': 'fields', 'type': 'array', 'optional': False},
        {'name': 'url', 'key': 'url', 'type': 'text', 'text_type': 'url'},
    ]
    example_request = {
        "filters": {
            "keyword": "\tLockheed.     ",
            "award_type_codes": ["A", "B", "C", "D"]
        },
        "fields": [
            "Award ID", "Mod", "Recipient Name", "Action Date", "Transaction Amount", "Awarding Agency",
            "Awarding Sub Agency", "Award Type"],
        "page": 1,
        "limit": '100',
        "sort": "Transaction Amount",
        "order": "desc",
        "auditTrail": "Postman Test guid",
        "cacheMiss": "$guid",
        "url": 'http://google.com',
    }

    validated_data = Heimdallr(models).shield(example_request)
    print(validated_data)
################################################################################
