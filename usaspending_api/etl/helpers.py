from datetime import datetime
import warnings
import logging
import pandas as pd

from django.db.models import Q, F, Case, Value, When
from django.core.cache import caches, CacheKeyWarning
import django.apps

from usaspending_api.references.models import Agency, Location, RefCountryCode
from usaspending_api.references.helpers import canonicalize_location_dict
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
    agencies = Agency.objects.all().values(
        'id',
        'subtier_agency__subtier_code').order_by('subtier_agency__update_date')
    subtier_agency_dict = {
        a['subtier_agency__subtier_code']: a['id']
        for a in agencies
    }
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

        # Apparently zip codes are optional...
        if location_dict["location_zip"]:
            location_dict.update(
                zip5=location_dict["location_zip"][:5],
                zip_last4=location_dict["location_zip"][5:])

        location_dict.pop("location_zip")

    else:
        location_dict.update(
            foreign_postal_code=location_dict.pop("location_zip", None),
            foreign_province=location_dict.pop("state_code", None))
        if "city_name" in location_dict:
            location_dict['foreign_city_name'] = location_dict.pop("city_name")

    location_dict = canonicalize_location_dict(location_dict)

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

        if model.__name__[:10] == "Historical":
            continue

        model_fields = [f.name for f in model._meta.get_fields()]

        # This supports multi-case DAIMS
        # We must filter on the model level rather than add them to the
        # when clauses, because if there is a FK in the when clause Django is
        # not guaranteed to join on that table properly.
        #
        # This is an array of tuples of the following format
        # (Q object of filter, field_names -> case objects map for this filter)
        #
        # It is initialized with a blank filter and empty list, which is where
        # default updates are stored
        model_filtered_update_case_map = [(Q(), {})]

        desc_fields = [field for field in model_fields if field.split('_')[-1] == "description"[:len(field.split('_')[-1])]]
        non_desc_fields = [field for field in model_fields if field not in desc_fields]
        desc_fields_mapping = {}
        for desc_field in desc_fields:
            actual_field_short = "_".join(desc_field.split('_')[:-1])
            actual_field = None
            for field in non_desc_fields:
                if actual_field_short == field:
                    actual_field = field
                elif actual_field_short == field[:len(actual_field_short)]:
                    actual_field = field
            desc_fields_mapping[desc_field] = actual_field

        # Loop through each of the models fields to construct a case for each
        # applicable field
        for field in model_fields:
            # We're looking for field names ending in _description
            split_name = field.split("_")

            # If the last element in our split name isn't description, skip it
            if len(split_name) == 1 or split_name[-1] != "description"[:len(split_name[-1])]:
                continue

            source_field = "_".join(split_name[:-1])
            destination_field = field
            # This is the map name, prefixed by model name for when there are
            # non-unique description fields
            source_field = desc_fields_mapping[field] if field in desc_fields_mapping else source_field
            model_map_name = "{}.{}_map".format(model.__name__, source_field)
            map_name = "{}_map".format(source_field)

            # This stores a direct reference to the enumeration mapping
            code_map = None

            # Validate we have the source field
            if source_field not in model_fields:
                logger.debug("Tried to update '{}' on model '{}', but source field '{}' does not exist.".format(destination_field, model.__name__, source_field))
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

            # Cases start from 1
            case_number = 1
            case_name = "case_1"
            case_map = "case_1_map"
            while case_name in code_map.keys():
                case_object = create_case(code_map[case_map], source_field)
                # Construct a Q filter for this case
                case_filter = Q(**code_map[case_name])

                # See if we already have a tuple for this filter
                case_tuple = [x for x in model_filtered_update_case_map if x[0] == case_filter]
                if len(case_tuple) == 0:
                    # We don't, so create the tuple
                    temp_case_dict = {}
                    temp_case_dict[field] = case_object
                    model_filtered_update_case_map.append((case_filter, temp_case_dict))
                else:
                    # We do, so just add our case object to that dictionary
                    case_tuple[0][1][field] = case_object

                # Check for the next case
                case_number += 1
                case_name = "case_{}".format(case_number)
                case_map = "case_{}_map".format(case_number)

            # If our case number is still 1, then we didn't have any cases.
            # Therefore, we perform the default
            if case_number == 1:
                case_object = create_case(code_map, source_field)

                # Grab the first tuple, which has no filters
                case_tuple = model_filtered_update_case_map[0]

                # Add it to our dictionary
                case_tuple[1][field] = case_object

        for filter_tuple in model_filtered_update_case_map:
            # For each filter tuple, check if the dictionary has any entries
            if len(filter_tuple[1].keys()) > 0:
                print("Updating model {}\n  FILTERS:\n    {}\n  FIELDS:\n    {}".format(model.__name__, str(filter_tuple[0]), "\n    ".join(filter_tuple[1].keys())))
                try:
                    model.objects.filter(filter_tuple[0]).update(**filter_tuple[1])
                except django.db.utils.ProgrammingError as e:
                    logger.warn(str(e))
                    logger.warn("(OK if invoked from a migration,\n"
                                "when the table may not yet have been created)")


# Utility method for update_model_description_fields, creates the Case object
def create_case(code_map, source_field):
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

    return Case(*when_list, default=default)


def pad_function(field, pad_to, keep_null=False):
    """
    Pads fields with 0 or keeps field null if specified. Used to clean up csv data with alpha numeric codes.
    Function reused from broker cleaning
    """
    if pd.isnull(field) or not str(field).strip():
        if keep_null:
            return None
        else:
            return ''
    return str(field).strip().zfill(pad_to)


def merge_objects(primary_object, objects_to_merge=[], keep_old=False):
    """
    Preliminary merge function to combine objects into one field.

    Originally used to to fix duplicate agencies in the agency loader
    Can be further developed to merge relational data: relational data and foreign key relations

    Usage:
    agency_1 = Toptier.objects.get(name='Agency 1')
    agency_2 = Toptier.objects.get(namee='Agency 1 dupe')
    merge_objects(agency_1, agency_2)

    """

    primary_class = primary_object.__class__

    if not isinstance(objects_to_merge, list):
        objects_to_merge = [objects_to_merge]

    for merge_object in objects_to_merge:
        if not isinstance(merge_object, primary_class):
            raise TypeError('Only models of the same time can be merged')

    fields_to_merge = []

    for field in primary_object._meta.fields:
        if not field.primary_key and field.name not in ['create_date', 'update_date']:
            fields_to_merge.append(field.name)

    # Try to fill all missing values in primary object by values of duplicates
    for field_name in fields_to_merge:
        val = getattr(merge_object, field_name)
        setattr(primary_object, field_name, val)

    if not keep_old:
        merge_object.delete()

    primary_object.save()
    return primary_object
