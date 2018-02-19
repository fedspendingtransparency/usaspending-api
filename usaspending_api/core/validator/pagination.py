PAGINATION = [
    {'name': 'page', 'type': 'integer', 'default': 1, 'min': 1},
    {'name': 'limit', 'type': 'integer', 'default': 10, 'min': 1, 'max': 100},
    {'name': 'sort', 'type': 'text', 'text_type': 'search'},
    {'name': 'order', 'type': 'enum', 'enum_values': ('asc', 'desc'), 'default': 'desc'},
]

for p in PAGINATION:
    p['optional'] = p.get('optional', True)
    p['key'] = p['name']
