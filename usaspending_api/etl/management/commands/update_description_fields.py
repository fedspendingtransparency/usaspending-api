import logging
import os
import django.apps
from django.core.management.base import BaseCommand
from django.db.models import F, Case, Value, When

from usaspending_api.data.daims_maps import daims_maps


class Command(BaseCommand):
    """
    This command will generate SQL using sqlsequencereset for each app, so that
    one can repair the primary key sequences of the listed models
    """
    help = "Update model description fields based on code"

    def handle(self, *args, **options):
        update_model_description_fields()


def update_model_description_fields():
    """
    This method searches through every model Django has registered, checks if it
    belongs to a list of apps we should update, and updates all fields with
    '_description' at the end with their relevant information.

    Dictionaries for DAIMS definitions should be stored in:
        usaspending_api/data/daims_maps.py

    Each map should be <field_name>_map for discoverability.
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
            map_name = "{}_map".format(source_field)

            # Validate we have the source field
            if source_field not in model_fields:
                logger.info("Tried to update '{}' on model '{}', but source field '{}' does not exist.".format(destination_field, model.__name__, source_field))
                continue

            # Validate we have a map
            if map_name not in daims_maps.keys():
                logger.info("Tried to update '{}' on model '{}', but map '{}' does not exist.".format(destination_field, model.__name__, map_name))
                continue

            # Grab the map
            code_map = daims_maps[map_name]

            # Construct the set of whens for this field
            when_list = []
            default = None
            for code in code_map.keys():
                when_args = {}
                when_args[source_field] = code
                when_args["then"] = Value(code_map[code])

                # If our code is blank, change the comparison to ""
                if code == "_BLANK":
                    when_args[source_field] = ""

                # We handle the default case later
                if code == "_DEFAULT":
                    default = code_map[code]
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
