def location_mapper_fin_assistance_principal_place(row):
    loc = {
        "county_name": row.get("principal_place_cc", ""),
        "location_country_code": row.get("principal_place_country_code", ""),
        "location_zip": row.get("principal_place_zip", "").replace(
            "-", ""
        ),  # Either ZIP5, or ZIP5+4, sometimes with hypens
        "state_code": row.get("principal_place_state_code", ""),
        "state_name": row.get("principal_place_state", ""),
    }
    return loc


def location_mapper_fin_assistance_recipient(row):
    loc = {
        "county_code": row.get("recipient_county_code", ""),
        "county_name": row.get("recipient_county_name", ""),
        "location_country_code": row.get("recipient_country_code", ""),
        "city_code": row.get("recipient_city_code", ""),
        "city_name": row.get("recipient_city_name", ""),
        "location_zip": row.get("recipient_zip", "").replace("-", ""),  # Either ZIP5, or ZIP5+4, sometimes with hypens
        "state_code": row.get("recipient_state_code"),
        "address_line1": row.get("receip_addr1"),
        "address_line2": row.get("receip_addr2"),
        "address_line3": row.get("receip_addr3"),
    }
    return loc
