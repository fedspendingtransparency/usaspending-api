# This file defines a series of constants that represent the values used in the
# API's "helper" tables. Rather than define the values in the db setup scripts
# and then make db calls to lookup the surrogate keys, we'll define everything
# here, in a file that can be used by the db setup scripts *and* the application
# code.
from collections import namedtuple

LookupType = namedtuple('LookupType', ['id', 'name', 'desc'])

EXTERNAL_DATA_TYPE = [
    LookupType(1, 'fpds', 'fpds external data type'),
    LookupType(2, 'fabs', 'fabs external data type')
]
EXTERNAL_DATA_TYPE_DICT = {item.name: item.id for item in EXTERNAL_DATA_TYPE}
EXTERNAL_DATA_TYPE_DICT_ID = {item.id: item.name for item in EXTERNAL_DATA_TYPE}
