import usaspending_api.etl.helpers as h


def evaluate_contract_award_type(row):
    first_element = h.up2colon(row["contractactiontype"].split()[0])

    if len(first_element) == 1:
        return first_element
    else:
        cat = row["contractactiontype"].lower()
        # Not using DAIMS enumeration . . .
        if "bpa" in cat:
            return "A"
        elif "purchase" in cat:
            return "B"
        elif "delivery" in cat:
            return "C"
        elif "definitive" in cat:
            return "D"
        else:
            return None


def location_mapper_place_of_performance(row):

    loc = {
        "city_name": row.get("placeofperformancecity", ""),
        "congressional_code": row.get("placeofperformancecongressionaldistrict", "")[
            2:
        ],  # Need to strip the state off the front
        "location_country_code": row.get("placeofperformancecountrycode", ""),
        "location_zip": row.get("placeofperformancezipcode", "").replace(
            "-", ""
        ),  # Either ZIP5, or ZIP5+4, sometimes with hypens
        "state_code": h.up2colon(
            row.get("pop_state_code", "")
        ),  # Format is VA: VIRGINIA, so we need to grab the first bit
    }
    return loc


def location_mapper_vendor(row):
    loc = {
        "city_name": row.get("city", ""),
        "congressional_code": row.get("vendor_cd", "").zfill(2),  # Need to add leading zeroes here
        "location_country_code": row.get(
            "vendorcountrycode", ""
        ),  # Never actually a country code, just the string name
        "location_zip": row.get("zipcode", "").replace("-", ""),
        "state_code": h.up2colon(row.get("vendor_state_code", "")),
        "address_line1": row.get("streetaddress", ""),
        "address_line2": row.get("streetaddress2", ""),
        "address_line3": row.get("streetaddress3", ""),
    }
    return loc
