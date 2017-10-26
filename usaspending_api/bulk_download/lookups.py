# This file defines a series of constants that represent the values used in the
# API's "helper" tables. Rather than define the values in the db setup scripts
# and then make db calls to lookup the surrogate keys, we'll define everything
# here, in a file that can be used by the db setup scripts *and* the application
# code.

from bidict import bidict
from collections import namedtuple

LookupType = namedtuple('LookupType', ['id', 'name', 'desc'])

JOB_STATUS = [
    LookupType(1, 'ready', 'job is ready to be run'),
    LookupType(2, 'running', 'job is currently in progress'),
    LookupType(3, 'finished', 'job is complete'),
    LookupType(4, 'failed', 'job failed to complete')
]
JOB_STATUS_DICT = {item.name: item.id for item in JOB_STATUS}
JOB_STATUS_DICT_ID = {item.id: item.name for item in JOB_STATUS}
