from datetime import datetime

from django.db.models import Q

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, Location, RefCountryCode


def up2colon(input_string):
    'Takes the part of a string before `:`, if any.'

    return input_string.split(':')[0].strip()


def get_agency(self, agency_string):
    agency_code = up2colon(agency_string)
    agency = Agency.objects.filter(
        subtier_agency__subtier_code=agency_code).first()
    if not agency:
        self.logger.error("Missing agency: " + agency_string)
    return agency


def convert_date(date):
    return datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')


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


def get_or_create_award(row):
    piid = row.get("piid", None)
    parent_award_id = row.get("idvpiid", None)
    award = Award.get_or_create_summary_award(
        piid=piid, fain=None, uri=None, parent_award_id=parent_award_id)
    return award


def get_or_create_location(row, mode="vendor"):
    mapping = {}

    if mode == "place_of_performance":
        mapping = {
            "city": row.get("placeofperformancecity", ""),
            "congressionaldistrict":
            row.get("placeofperformancecongressionaldistrict",
                    "")[2:],  # Need to strip the state off the front
            "country": row.get("placeofperformancecountrycode", ""),
            "zipcode": row.get("placeofperformancezipcode", "").replace(
                "-", ""),  # Either ZIP5, or ZIP5+4, sometimes with hypens
            "state_code": up2colon(
                row.get("pop_state_code", "")
            )  # Format is VA: VIRGINIA, so we need to grab the first bit
        }
    else:
        mapping = {
            "city": row.get("city", ""),
            "congressionaldistrict": row.get(
                "vendor_cd", "").zfill(2),  # Need to add leading zeroes here
            "country":
            row.get("vendorcountrycode",
                    ""),  # Never actually a country code, just the string name
            "zipcode": row.get("zipcode", "").replace("-", ""),
            "state_code": up2colon(row.get("vendor_state_code", "")),
            "street1": row.get("streetaddress", ""),
            "street2": row.get("streetaddress2", ""),
            "street3": row.get("streetaddress3", "")
        }

    country_code = fetch_country_code(mapping["country"])
    location_dict = {"location_country_code": country_code, }

    if country_code.country_code == "USA":
        location_dict.update(
            location_zip5=mapping["zipcode"][:5],
            location_zip_last4=mapping["zipcode"][5:],
            location_state_code=mapping["state_code"],
            location_city_name=mapping["city"],
            location_congressional_code=mapping["congressionaldistrict"])
    else:
        location_dict.update(
            location_foreign_postal_code=mapping["zipcode"],
            location_foreign_province=mapping["state_code"],
            location_foreign_city_name=row["city"], )

    if mode == "vendor":
        location_dict.update(
            location_address_line1=mapping["street1"],
            location_address_line2=mapping["street2"],
            location_address_line3=mapping["street3"], )

    location = Location.objects.filter(**location_dict).first()
    if not location:
        location = Location.objects.create(**location_dict)
    return location
