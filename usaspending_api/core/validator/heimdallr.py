import logging
import sys

from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException

logger = logging.getLogger('console')

template = {
    'name': '',
    'key': '||||',
    'optional': [True, False],
    'type': ['integer', 'text', 'boolean', 'float', 'enum', 'array'],
    'min': None,
    'max': None,
    'on_error': ['ignore', 'raise', 'warn'],
    'default': None,
    'valid_values': [0, 3, 5, 6],  # necessary for enum
    'text_type': ['raw', 'sql', 'url', 'password'],  # useful for sanitizing text
}

integer_max = sys.maxsize  # == 9223372036854775807 == 2^(63-1)
integer_min = -sys.maxsize - 1  # == 9223372036854775808 == -2^(63-1) - 1
float_max = sys.float_info.max  # 1.7976931348623157e+308


class Nothing():
    def __init__(self):
        pass

    def __hash__(self):
        pass

    def __eq__(self, other):
        pass

class Heimdallr():
    '''
    https://en.wikipedia.org/wiki/Heimdallr
    '''

    def __init__(self, model_list, request):
        self.models = self.check_models(model_list)
        self.data = {}

        self.parse_request(request)
        self.validate()

    def check_models(self, models):
        if not all(key in model for key in ['name', 'key', 'type'] for model in models):
            raise Exception('Missing Required field in model')

        ####
        # Add checks for values, add min and max if missing.
        ####
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
                    item['value'] = Nothing()

    def validate(self):
        for item in self.models:
            if item['type'] == 'integer':
                self.data[item['name']] = validate_integers(item)
            elif item['type'] == 'boolean':
                self.data[item['name']] = validate_boolean(item)
            # TODO add more


def validate_boolean(model):
    if model['value'] in (True, 1, '1', 'true', 'True', 'TRUE'):
        return True
    elif model['value'] in (False, 0, '0', 'false', 'False', 'FALSE'):
        return False
    else:
        msg = 'Incorrect value provided for boolean field {}. Use True/False'
        raise InvalidParameterException(msg.format(model['name']))


def validate_integers(model):
    model['min'] = model.get('min', integer_min)
    model['max'] = model.get('max', integer_max)
    val = _verify_is_int(model['value'])
    _check_max(model, val)
    _check_min(model, val)
    return val


def validate_array(model):
    model['min'] = model.get('min', integer_min)
    model['max'] = model.get('max', integer_max)
    value = model['value']
    if type(value) is not list:
        raise InvalidParameterException()
    _check_max(model, value)
    _check_min(model, value)
    return value


def validate_text(model):
    model['min'] = model.get('min', 0)
    model['max'] = model.get('max', integer_max)
    value = model['value']
    _check_max(model, value)
    _check_min(model, value)
    text_type = model.get('text_type', 'safe')
    if text_type == 'raw':
        val = value
    elif text_type == 'sql':
        logger.warn('text_type "url" not implemented')
        val = value
    elif text_type == 'url':
        logger.warn('text_type "url" not implemented')
        val = value
    elif text_type == 'password':
        logger.warn('text_type "password" not implemented')
        val = value

    return val


def _check_max(model, value):
    if model['type'] == 'integer':
        if value >= model['max']:
            raise UnprocessableEntityException()

    if model['type'] in ('text', 'array'):
        if len(value) >= model['max']:
            raise UnprocessableEntityException()


def _check_min(model, value):
    if model['type'] == 'integer':
        if value < model['min']:
            raise UnprocessableEntityException()

    if model['type'] in ('text', 'array'):
        if len(value) < model['min']:
            raise UnprocessableEntityException()


def _verify_is_int(value):
    try:
        return int(value)
    except Exception as e:
        raise InvalidParameterException()
