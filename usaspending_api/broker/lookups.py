# This file defines a series of constants that represent the values used in the
# API's "helper" tables. Rather than define the values in the db setup scripts
# and then make db calls to lookup the surrogate keys, we'll define everything
# here, in a file that can be used by the db setup scripts *and* the application
# code.
from collections import namedtuple

LookupType = namedtuple("LookupType", ["id", "name", "desc"])

EXTERNAL_DATA_TYPE = [
    LookupType(1, "fpds", "FPDS transactions from Broker"),
    LookupType(2, "fabs", "FABS transactions from Broker"),
    LookupType(3, "exec_comp", "Executive Compensation from Broker"),
    # "opposite" side of the broker data load, data from USAspending DB -> Elasticsearch
    LookupType(100, "es_transactions", "Load elasticsearch with transactions from USAspending"),
]
EXTERNAL_DATA_TYPE_DICT = {item.name: item.id for item in EXTERNAL_DATA_TYPE}
EXTERNAL_DATA_TYPE_DICT_ID = {item.id: item.name for item in EXTERNAL_DATA_TYPE}
