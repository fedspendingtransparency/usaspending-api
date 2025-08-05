# based from all_award_types_mappings in usaspending_api.awards.v2.lookups.lookups
AWARD_TYPES = {
    "contracts": {"amount": "last_12_contracts", "filter": "contract"},
    "grants": {"amount": "last_12_grants", "filter": "grant"},
    "direct_payments": {"amount": "last_12_direct_payments", "filter": "direct payment"},
    "loans": {"amount": "last_12_loans", "filter": "loans"},
    "other_financial_assistance": {"amount": "last_12_other", "filter": "other"},
}

# Recipient Levels
#   - P = Parent Recipient, There is at least one child recipient that lists this recipient as a parent
#   - C = Child Recipient, References a parent recipient
#   - R = Recipient, No parent info provided
RECIPIENT_LEVELS = ["P", "C", "R"]

# Special Cases - Recipients that cover a group of recipients
SPECIAL_CASES = [
    "MULTIPLE RECIPIENTS",
    "REDACTED DUE TO PII",
    "MULTIPLE FOREIGN RECIPIENTS",
    "PRIVATE INDIVIDUAL",
    "INDIVIDUAL RECIPIENT",
    "MISCELLANEOUS FOREIGN AWARDEES",
]
