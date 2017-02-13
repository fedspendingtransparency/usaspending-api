from datetime import datetime

from django.db.models import Q

from usaspending_api.references.models import Agency, Location, RefCountryCode


def cleanse_values(row):
    """
    Remove textual quirks from CSV values.
    """
    row = {k: v.strip() for (k, v) in row.items()}
    row = {k: (None if v.lower() == 'null' else v) for (k, v) in row.items()}
    return row


def convert_date(date):
    return datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')


def get_subtier_agency_dict():
    """Returns a dictionary with key = subtier agency code and value = agency id."""
    # there's no unique constraint on subtier_code, so the order by below ensures
    # that in the case of duplicate subtier codes, the dictionary we return will
    # reflect the most recently updated one
    agencies = Agency.objects.all().values('id', 'subtier_agency__subtier_code').order_by(
        'subtier_agency__update_date')
    subtier_agency_dict = {a['subtier_agency__subtier_code']: a['id'] for a in agencies}
    return subtier_agency_dict


def fetch_country_code(vendor_country_code):
    code_str = up2colon(vendor_country_code)

    country_code = RefCountryCode.objects.filter(
        Q(country_code=code_str) | Q(country_name__iexact=code_str)).first()
    if not country_code:
        # We don't have an exact match on the name or the code, so we need to
        # chain filter on the name
        query_set = RefCountryCode.objects
        for word in code_str.split():
            query_set = query_set.filter(country_name__icontains=word)
        country_code = query_set.first()

    return country_code


def get_or_create_location(row, mapper):
    location_dict = mapper(row)

    country_code = fetch_country_code(location_dict["location_country_code"])
    location_dict["location_country_code"] = country_code

    # Country-specific adjustments
    if country_code.country_code == "USA":
        location_dict.update(
            zip5=location_dict["location_zip"][:5],
            zip_last4=location_dict["location_zip"][5:])
        location_dict.pop("location_zip")
    else:
        location_dict.update(
            foreign_postal_code=location_dict.pop("location_zip",
                                                  None),
            foreign_province=location_dict.pop("state_code",
                                               None))
        if "city_name" in location_dict:
            location_dict['foreign_city_name'] = location_dict.pop(
                "city_name")

    location = Location.objects.filter(**location_dict).first()
    if not location:
        location = Location.objects.create(**location_dict)
    return location


def up2colon(input_string):
    'Takes the part of a string before `:`, if any.'

    if input_string:
        return input_string.split(':')[0].strip()
    return ''
