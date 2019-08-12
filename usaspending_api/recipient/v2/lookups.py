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
