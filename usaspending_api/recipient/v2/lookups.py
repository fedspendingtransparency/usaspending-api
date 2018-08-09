import os
import django
import csv

# Recipient Levels
#   - P = Parent Recipient, There is at least one child recipient that lists this recipient as a parent
#   - C = Child Recipient, References a parent recipient
#   - R = Recipient, No parent info provided
RECIPIENT_LEVELS = ['P', 'C', 'R']

# Special Cases - Recipients that cover a group of recipients
SPECIAL_CASES = [
    'MULTIPLE RECIPIENTS',
    'REDACTED DUE TO PII',
    'MULTIPLE FOREIGN RECIPIENTS',
    'PRIVATE INDIVIDUAL',
    'INDIVIDUAL RECIPIENT'
]

# Based on SAM Functional Data Dictionary
SAM_FUNCTIONAL_DATA_DICTIONARY_CSV = os.path.join(django.conf.settings.BASE_DIR, 'usaspending_api', 'data',
                                                  'sam_functional_data_dictionary.csv')
DUNS_BUSINESS_TYPES_MAPPING = {}

with open(SAM_FUNCTIONAL_DATA_DICTIONARY_CSV, 'r') as sam_data_dict_csv:
    reader = csv.DictReader(sam_data_dict_csv, delimiter=',')
    rows = list(reader)
    for index, row in enumerate(rows, 1):
        DUNS_BUSINESS_TYPES_MAPPING[row['code']]= row['terse_label']