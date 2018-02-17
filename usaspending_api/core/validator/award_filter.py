from usaspending_api.awards.v2.lookups.lookups import award_type_mapping

AWARD_FILTER = [
    {'name': 'award_ids', 'type': 'array', 'array_type': 'integer'},
    {'name': 'award_type_codes', 'type': 'enum', 'enum_values': award_type_mapping.keys()},
    {'name': 'contract_pricing_type_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'extent_competed_type_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'keyword', 'type': 'text', 'text_type': 'search'},
    {'name': 'legal_entities', 'type': 'array', 'array_type': 'integer'},
    {'name': 'naics_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'program_numbers', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'psc_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'recipient_scope', 'type': 'enum', 'enum_values': ('domestic', 'foreign')},
    {'name': 'recipient_search_text', 'type': 'array', 'max': 1, 'min': 1},
    {'name': 'recipient_type_names', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
    {'name': 'set_aside_type_codes', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
]

for a in AWARD_FILTER:
    a['optional'] = a.get('optional', True)  # want to make time_period non-optional
    a['key'] = 'filters|{}'.format(a['name'])


'''
TODO: Add these to the filter dictionary
========================================

'time_period', # list of dics [{"start_date": "2016-10-01", "end_date": "2017-09-30"}]

'recipient_locations', "recipient_locations": [{"country": "USA","state": "VA","county": "059"}]

'place_of_performance_scope',  # enum (county, state, congressional district)

'place_of_performance_locations', [ {"country": "USA", "state": "VA", "county": "059"}]

'award_amounts', # [{"lower_bound": 1000000.00,"upper_bound": 25000000.00},{"upper_bound": 1000000.00}, ...]

'agencies', [{ "type": "funding", "tier": "toptier", "name": "Office of Pizza"}, ...]
'''
