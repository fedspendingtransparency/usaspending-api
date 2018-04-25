from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.core.validator.helpers import TINY_SHIELD_SEPARATOR


AWARD_FILTER = [
    {'name': 'award_ids', 'type': 'array', 'array_type': 'integer'},
    {'name': 'award_type_codes', 'type': 'array', 'array_type': 'enum', 'enum_values': list(award_type_mapping.keys())},
    {'name': 'contract_pricing_type_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'extent_competed_type_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'keyword', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'legal_entities', 'type': 'array', 'array_type': 'integer'},
    {'name': 'naics_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'place_of_performance_scope', 'type': 'enum', 'enum_values': ['domestic', 'foreign']},
    {'name': 'program_numbers', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'psc_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'recipient_scope', 'type': 'enum', 'enum_values': ('domestic', 'foreign')},
    {'name': 'recipient_search_text', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'recipient_type_names', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'set_aside_type_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'time_period', 'type': 'array', 'array_type': 'object', 'object_keys': {
        'start_date': {'type': 'date', 'optional': True},
        'end_date': {'type': 'date', 'optional': True}
    }},
    {'name': 'award_amounts', 'type': 'array', 'array_type': 'object', 'object_keys': {
        'lower_bound': {'type': 'float', 'optional': True},
        'upper_bound': {'type': 'float', 'optional': True}
    }},
    {'name': 'agencies', 'type': 'array', 'array_type': 'object', 'object_keys': {
        'type': {'type': 'enum', 'enum_values': ['funding', 'awarding'], 'optional': False},
        'tier': {'type': 'enum', 'enum_values': ['toptier', 'subtier'], 'optional': False},
        'name': {'type': 'text', 'text_type': 'search', 'optional': False}
    }},
    {'name': 'recipient_locations', 'type': 'array', 'array_type': 'object', 'object_keys': {
        'country': {'type': 'text', 'text_type': 'search', 'optional': False},
        'state': {'type': 'text', 'text_type': 'search', 'optional': True},
        'zip': {'type': 'text', 'text_type': 'search', 'optional': True},
        'district': {'type': 'text', 'text_type': 'search', 'optional': True},
        'county': {'type': 'text', 'text_type': 'search', 'optional': True},
    }},
    {'name': 'place_of_performance_locations', 'type': 'array', 'array_type': 'object', 'object_keys': {
        'country': {'type': 'text', 'text_type': 'search', 'optional': False},
        'state': {'type': 'text', 'text_type': 'search', 'optional': True},
        'zip': {'type': 'text', 'text_type': 'search', 'optional': True},
        'district': {'type': 'text', 'text_type': 'search', 'optional': True},
        'county': {'type': 'text', 'text_type': 'search', 'optional': True},
    }},
]

for a in AWARD_FILTER:
    a['optional'] = a.get('optional', True)  # future TODO: want to make time_period required
    a['key'] = 'filters{sep}{name}'.format(sep=TINY_SHIELD_SEPARATOR, name=a['name'])
    if a['type'] == 'array':
        a['min'] = a.get('min', 1)


def reconstitute_filter(disassembled_obj, object_dict_with_keys):
    filter_obj = {}
    filters = [a['name'] for a in object_dict_with_keys]
    for k, v in disassembled_obj.items():
        if k in filters:
            filter_obj[k] = v
    return filter_obj if filter_obj is not {} else None
