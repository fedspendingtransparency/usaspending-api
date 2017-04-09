from datetime import datetime
import warnings
import logging

from django.db.models import Q, F, Case, Value, When
from django.core.cache import caches, CacheKeyWarning
import django.apps

from usaspending_api.references.models import Agency, Location, RefCountryCode
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.data.daims_maps import daims_maps

warnings.simplefilter("ignore", CacheKeyWarning)


def clear_caches():
    for cache_name in ('default', 'locations', 'awards'):
        caches[cache_name].clear()


def cleanse_values(row):
    """
    Remove textual quirks from CSV values.
    """
    row = {k: v.strip() for (k, v) in row.items()}
    row = {k: (None if v.lower() == 'null' else v) for (k, v) in row.items()}
    return row


def convert_date(date):
    if date == "":
        return None
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
    if code_str == "":
        return None

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

location_cache = caches['locations']


def get_or_create_location(row, mapper):
    location_dict = mapper(row)

    country_code = fetch_country_code(location_dict["location_country_code"])
    location_dict["location_country_code"] = country_code

    # Country-specific adjustments
    if country_code and country_code.country_code == "USA":
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

    location_tup = tuple(location_dict.items())
    location = location_cache.get(location_tup)
    if location:
        return location

    location = Location.objects.filter(**location_dict).first()
    if not location:
        location = Location.objects.create(**location_dict)
        location_cache.set(location_tup, location)
    return location


def up2colon(input_string):
    'Takes the part of a string before `:`, if any.'

    if input_string:
        return input_string.split(':')[0].strip()
    return ''


def parse_numeric_value(string):
    try:
        return float(string)
    except:
        return None


def get_fiscal_quarter(fiscal_reporting_period):
    """
    Return the fiscal quarter.
    Note: the reporting period being passed should already be in
    "federal fiscal format", where period 1 = Oct. and period 12 = Sept.
    """
    if fiscal_reporting_period in [1, 2, 3]:
        return 1
    elif fiscal_reporting_period in [4, 5, 6]:
        return 2
    elif fiscal_reporting_period in [7, 8, 9]:
        return 3
    elif fiscal_reporting_period in [10, 11, 12]:
        return 4


def get_previous_submission(cgac_code, fiscal_year, fiscal_period):
    """
    For the specified CGAC (e.g., department/top-tier agency) and specified
    fiscal year and quarter, return the previous submission within the same fiscal
    year.
    """
    previous_submission = SubmissionAttributes.objects \
        .filter(
            cgac_code=cgac_code,
            reporting_fiscal_year=fiscal_year,
            reporting_fiscal_period__lt=fiscal_period,
            quarter_format_flag=True) \
        .order_by('-reporting_fiscal_period') \
        .first()
    return previous_submission


def update_model_description_fields():
    """
    This method searches through every model Django has registered, checks if it
    belongs to a list of apps we should update, and updates all fields with
    '_description' at the end with their relevant information.

    Dictionaries for DAIMS definitions should be stored in:
        usaspending_api/data/daims_maps.py

    Each map should be <field_name>_map for discoverability.
    If there are conflicting maps (i.e., two models use type_description, but
    different enumerations) prepend the map name with the model name and a dot.

    For examples of these situations, see the documentation in daims_maps.py
    """

    logger = logging.getLogger('console')

    # This is a list of apps whose models will be checked for description fields
    updatable_apps = [
        "accounts",
        "awards",
        "common",
        "financial_activities",
        "references",
        "submissions"
    ]

    # This iterates over every model that Django has registered
    for model in django.apps.apps.get_models():
        # This checks the app_label of the model, and thus we can skip it if it
        # is not in one of our updatable_apps. Thus, we'll skip any django admin
        # apps, like auth, corsheaders, etc.
        if model._meta.app_label not in updatable_apps:
            continue

        model_fields = [f.name for f in model._meta.get_fields()]

        # This dictionary stores an array of field-names > case objects which
        # we can then pass to a bulk update for a set-based update
        model_update_case_map = {}

        # Loop through each of the models fields to construct a case for each
        # applicable field
        for field in model_fields:
            # We're looking for field names ending in _description
            split_name = field.split("_")

            # If the last element in our split name isn't description, skip it
            if len(split_name) == 1 or split_name[-1] != "description":
                continue

            source_field = "_".join(split_name[:-1])
            destination_field = field
            # This is the map name, prefixed by model name for when there are
            # non-unique description fields
            model_map_name = "{}.{}_map".format(model.__name__, source_field)
            map_name = "{}_map".format(source_field)

            # This stores a direct reference to the enumeration mapping
            code_map = None

            # Validate we have the source field
            if source_field not in model_fields:
                logger.warn("Tried to update '{}' on model '{}', but source field '{}' does not exist.".format(destination_field, model.__name__, source_field))
                continue

            # Validate we have a map
            # Prefer model_map_name over map_name
            if model_map_name in daims_maps.keys():
                code_map = daims_maps[model_map_name]
            elif map_name in daims_maps.keys():
                code_map = daims_maps[map_name]
            else:
                logger.warn("Tried to update '{}' on model '{}', but neither map '{}' nor '{}' exists.".format(destination_field, model.__name__, model_map_name, map_name))
                continue

            # Construct the set of whens for this field
            when_list = []
            default = None
            for code in code_map.keys():
                when_args = {}
                when_args[source_field] = code
                when_args["then"] = Value(code_map[code])

                # If our code is blank, change the comparison to ""
                if code == "_BLANK":
                    when_args[source_field] = Value("")

                # We handle the default case later
                if code == "_DEFAULT":
                    default = Value(code_map[code])
                    continue

                # Append a new when to our when-list
                when_list.append(When(**when_args))

            # Now we have an array of When() objects, a default (if applicable)
            # We can now make our Case object
            case = Case(*when_list, default=default)

            # Add it to our dictionary
            model_update_case_map[field] = case

        # We are done looping over all fields, check if our case dictionary has anything in it
        if len(model_update_case_map.keys()) > 0:
            # Update all of the instances of this model with our case map
            # TODO: In the future, if this starts taking to long we can
            # filter the dataset to a subset of id's
            logger.info("Updating model {}, fields:\n\t{}".format(model.__name__, "\n\t".join(model_update_case_map.keys())))
            model.objects.update(**model_update_case_map)
