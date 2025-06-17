from enum import Enum


class SpendingLevel(Enum):
    AWARD = "awards"
    SUBAWARD = "subawards"
    TRANSACTION = "transactions"

    # This SpendingLevel builds on Award level data by supplementing with File C.
    # For example, capturing DEFC obligations on File C records that join to filtered Awards.
    FILE_C = "award_financial"
