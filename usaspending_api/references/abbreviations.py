code_to_state = {
    "AK": {"name": "ALASKA", "fips": "02"},
    "AL": {"name": "ALABAMA", "fips": "01"},
    "AR": {"name": "ARKANSAS", "fips": "05"},
    "AS": {"name": "AMERICAN SAMOA", "fips": "60"},
    "AZ": {"name": "ARIZONA", "fips": "04"},
    "CA": {"name": "CALIFORNIA", "fips": "06"},
    "CO": {"name": "COLORADO", "fips": "08"},
    "CT": {"name": "CONNECTICUT", "fips": "09"},
    "DC": {"name": "DISTRICT OF COLUMBIA", "fips": "11"},
    "DE": {"name": "DELAWARE", "fips": "10"},
    "FL": {"name": "FLORIDA", "fips": "12"},
    "FM": {"name": "FEDERATED STATES OF MICRONESIA", "fips": "64"},
    "GA": {"name": "GEORGIA", "fips": "13"},
    "GU": {"name": "GUAM", "fips": "66"},
    "HI": {"name": "HAWAII", "fips": "15"},
    "IA": {"name": "IOWA", "fips": "19"},
    "ID": {"name": "IDAHO", "fips": "16"},
    "IL": {"name": "ILLINOIS", "fips": "17"},
    "IN": {"name": "INDIANA", "fips": "18"},
    "KS": {"name": "KANSAS", "fips": "20"},
    "KY": {"name": "KENTUCKY", "fips": "21"},
    "LA": {"name": "LOUISIANA", "fips": "22"},
    "MA": {"name": "MASSACHUSETTS", "fips": "25"},
    "MD": {"name": "MARYLAND", "fips": "24"},
    "ME": {"name": "MAINE", "fips": "23"},
    "MH": {"name": "MARSHALL ISLANDS", "fips": "68"},
    "MI": {"name": "MICHIGAN", "fips": "26"},
    "MN": {"name": "MINNESOTA", "fips": "27"},
    "MO": {"name": "MISSOURI", "fips": "29"},
    "MP": {"name": "NORTHERN MARIANA ISLANDS", "fips": "69"},
    "MS": {"name": "MISSISSIPPI", "fips": "28"},
    "MT": {"name": "MONTANA", "fips": "30"},
    "NC": {"name": "NORTH CAROLINA", "fips": "37"},
    "ND": {"name": "NORTH DAKOTA", "fips": "38"},
    "NE": {"name": "NEBRASKA", "fips": "31"},
    "NH": {"name": "NEW HAMPSHIRE", "fips": "33"},
    "NJ": {"name": "NEW JERSEY", "fips": "34"},
    "NM": {"name": "NEW MEXICO", "fips": "35"},
    "NV": {"name": "NEVADA", "fips": "32"},
    "NY": {"name": "NEW YORK", "fips": "36"},
    "OH": {"name": "OHIO", "fips": "39"},
    "OK": {"name": "OKLAHOMA", "fips": "40"},
    "OR": {"name": "OREGON", "fips": "41"},
    "PA": {"name": "PENNSYLVANIA", "fips": "42"},
    "PR": {"name": "PUERTO RICO", "fips": "72"},
    "PW": {"name": "PALAU", "fips": "70"},
    "RI": {"name": "RHODE ISLAND", "fips": "44"},
    "SC": {"name": "SOUTH CAROLINA", "fips": "45"},
    "SD": {"name": "SOUTH DAKOTA", "fips": "46"},
    "TN": {"name": "TENNESSEE", "fips": "47"},
    "TX": {"name": "TEXAS", "fips": "48"},
    "UT": {"name": "UTAH", "fips": "49"},
    "UM": {"name": "U.S. MINOR OUTLYING ISLANDS", "fips": "74"},
    "VA": {"name": "VIRGINIA", "fips": "51"},
    "VI": {"name": "VIRGIN ISLANDS", "fips": "78"},
    "VT": {"name": "VERMONT", "fips": "50"},
    "WA": {"name": "WASHINGTON", "fips": "53"},
    "WI": {"name": "WISCONSIN", "fips": "55"},
    "WV": {"name": "WEST VIRGINIA", "fips": "54"},
    "WY": {"name": "WYOMING", "fips": "56"},
}


territory_country_codes = {
    "ASM": "American Samoa",
    "GUM": "Guam",
    "MNP": "Northern Mariana Islands",
    "PRI": "Puerto Rico",
    "VIR": "Virgin Islands of the U.S.",
    "UMI": "U.S. Minor Outlying Islands",
    "FSM": "Federated States of Micronesia",
    "MHL": "Marshall Islands",
    "PLW": "Palau",
}

state_to_code = {v["name"]: k for (k, v) in code_to_state.items()}

fips_to_code = {fips["fips"]: code for (code, fips) in code_to_state.items()}


def pad_codes(location_scope, code_as_float):
    # Used to convert int/float back to text code: 9.0 -> '09'
    code_as_float = str(code_as_float).replace(".0", "")
    if location_scope == "county":
        return code_as_float.zfill(3)
    else:
        return code_as_float.zfill(2)
