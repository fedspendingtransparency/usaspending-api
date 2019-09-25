TOTAL_BUDGET_AUTHORITY = 8361447130497.72
TOTAL_OBLIGATIONS_INCURRED = 4690484214947.31
WEBSITE_AWARD_BINS = {
    "<1M": {"lower": None, "upper": 1000000},
    "1M..25M": {"lower": 1000000, "upper": 25000000},
    "25M..100M": {"lower": 25000000, "upper": 100000000},
    "100M..500M": {"lower": 100000000, "upper": 500000000},
    ">500M": {"lower": 500000000, "upper": None},
}

# Air Force, Army, and Navy are to be reported under DoD.  096 has a ticket to be removed.
DOD_CGAC = "097"  # DoD's toptier identifier.
DOD_SUBSUMED_CGAC = ["017", "021", "057", "096"]  # Toptier identifiers for Air Force, Army, and Navy (ignoring 096).
DOD_ARMED_FORCES_CGAC = [DOD_CGAC] + DOD_SUBSUMED_CGAC  # The list of ALL agencies reported under DoD.
