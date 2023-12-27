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
    LookupType(4, "zips", "Zip code data from broker"),
    LookupType(10, "source_procurement_transaction", "source FPDS transaction records"),
    LookupType(11, "source_assistance_transaction", "source FABS transaction records"),
    # "opposite" side of the broker data load, data from USAspending DB -> Elasticsearch
    LookupType(100, "es_transactions", "Load elasticsearch with transactions from USAspending"),
    LookupType(101, "es_awards", "Load elasticsearch with awards from USAspending"),
    LookupType(102, "es_deletes", "Award and Transaction deletions from elasticsearch"),
    # Additional times to keep track of
    LookupType(120, "touch_last_period_awards", "Touch awards from last period, so they will be updated in ES"),
    # Transaction ETL types for delta
    LookupType(201, "transaction_fpds", "FPDS transactions in Delta"),
    LookupType(202, "transaction_fabs", "FABS transactions in Delta"),
    LookupType(203, "transaction_normalized", "Normalized transactions in Delta"),
    LookupType(204, "awards", "Awards in Delta"),
    LookupType(205, "transaction_id_lookup", "Transaction IDs in Delta"),
    LookupType(206, "award_id_lookup", "Award IDs in Delta"),
]
EXTERNAL_DATA_TYPE_DICT = {item.name: item.id for item in EXTERNAL_DATA_TYPE}
EXTERNAL_DATA_TYPE_DICT_ID = {item.id: item.name for item in EXTERNAL_DATA_TYPE}
