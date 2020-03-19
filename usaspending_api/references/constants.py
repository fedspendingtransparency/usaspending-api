TOTAL_BUDGET_AUTHORITY = 8361447130497.72
TOTAL_OBLIGATIONS_INCURRED = 4690484214947.31
WEBSITE_AWARD_BINS = {
    "<1M": {"lower": None, "upper": 1000000, "enums": ["<1M", "1M"]},
    "1M": {"lower": 1000000, "upper": 1000000, "enums": ["1M"]},
    "1M..25M": {"lower": 1000000, "upper": 25000000, "enums": ["1M", "1M..25M", "25M"]},
    "25M": {"lower": 25000000, "upper": 25000000, "enums": ["25M"]},
    "25M..100M": {"lower": 25000000, "upper": 100000000, "enums": ["25M", "25M..100M", "100M"]},
    "100M": {"lower": 100000000, "upper": 100000000, "enums": ["100M"]},
    "100M..500M": {"lower": 100000000, "upper": 500000000, "enums": ["100M", "100M..500M", "500M"]},
    "500M": {"lower": 500000000, "upper": 500000000, "enums": ["500M"]},
    ">500M": {"lower": 500000000, "upper": None, "enums": ["500M", ">500M"]},
}

DOD_CGAC = "097"  # DoD's toptier identifier.
DOD_SUBSUMED_CGAC = ["017", "021", "057"]  # Air Force, Army, and Navy are to be reported under DoD.
DOD_ARMED_FORCES_CGAC = [DOD_CGAC] + DOD_SUBSUMED_CGAC  # The list of ALL agencies reported under DoD.
DOD_ARMED_FORCES_TAS_CGAC_FREC = [("011", "1137"), ("011", "DE00")]  # TAS (CGAC, FREC)s for additional DoD agencies.
DOD_FEDERAL_ACCOUNTS = [
    ("011", "1081"),
    ("011", "1082"),
    ("011", "1085"),
    ("011", "4116"),
    ("011", "4121"),
    ("011", "4122"),
    ("011", "4174"),
    ("011", "8238"),
    ("011", "8242"),
]  # Federal Account (AID, MAIN)s that are to be reported under DoD.

DHS_CGAC = "070"
DHS_SUBSUMED_CGAC = ["058"]

# Agencies which should be excluded from dropdowns.
EXCLUDE_CGAC = ["000", "067"]
