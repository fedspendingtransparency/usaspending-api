"""
Code for loaders in management/commands to inherit from or share.
"""

import dateutil
import logging

from decimal import Decimal

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections

from usaspending_api.common.long_to_terse import LONG_TO_TERSE_LABELS
from usaspending_api.etl.broker_etl_helpers import PhonyCursor


# Lists to store for update_awards and update_procurement_awards
award_update_id_list = []
award_contract_update_id_list = []

logger = logging.getLogger("script")


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            "--test",
            action="store_true",
            dest="test",
            default=False,
            help="Runs the submission loader in test mode and uses stored data rather than pulling from a database",
        )

    def handle(self, *args, **options):

        # Grab data broker database connections
        if not options["test"]:
            try:
                db_conn = connections[settings.DATA_BROKER_DB_ALIAS]
                db_cursor = db_conn.cursor()
            except Exception as err:
                logger.critical("Could not connect to database. Is DATA_BROKER_DATABASE_URL set?")
                logger.critical(print(err))
                raise
        else:
            db_cursor = PhonyCursor()

        self.handle_loading(db_cursor=db_cursor, *args, **options)

        # Done!
        logger.info("FINISHED")


def format_date(date_string):
    try:
        return dateutil.parser.parse(date_string).date()
    except (TypeError, ValueError):
        return None


def load_data_into_model(model_instance, data, **kwargs):
    """
    Loads data into a model instance
    Data should be a row, a dict of field -> value pairs
    Keyword args:
        field_map - A map of field columns to data columns. This is so you can map
                    a field in the data to a different field in the model. For instance,
                    model.tas_rendering_label = data['tas'] could be set up like this:
                    field_map = {'tas_rendering_label': 'tas'}
                    The algorithm checks for a value map before a field map, so if the
                    column is present in both value_map and field_map, value map takes
                    precedence over the other
        value_map - Want to force or override a value? Specify the field name for the
                    instance and the data you want to load. Example:
                    {'update_date': timezone.now()}
                    The algorithm checks for a value map before a field map, so if the
                    column is present in both value_map and field_map, value map takes
                    precedence over the other
        save - Defaults to False, but when set to true will save the model at the end
        reverse -   Field names matching this regex should be reversed
                    (multiplied by -1) before saving.
        as_dict - If true, returns the model as a dict instead of saving or altering
    """
    field_map = kwargs.get("field_map")
    value_map = kwargs.get("value_map")
    save = kwargs.get("save", False)
    as_dict = kwargs.get("as_dict", False)
    reverse = kwargs.get("reverse")

    # Grab all the field names from the meta class of the model instance
    fields = [field.name for field in model_instance._meta.get_fields()]
    mod = model_instance

    if as_dict:
        mod = {}

    for field in fields:
        # Let's handle the data source field here for all objects
        if field == "data_source" and field not in value_map:
            store_value(mod, field, "DBR", reverse)

        broker_field = field
        # If our field is the 'long form' field, we need to get what it maps to
        # in the broker so we can map the data properly
        if broker_field in LONG_TO_TERSE_LABELS:
            broker_field = LONG_TO_TERSE_LABELS[broker_field]
        sts = False
        if value_map:
            if broker_field in value_map:
                store_value(mod, field, value_map[broker_field], reverse, data)
                sts = True
            elif field in value_map:
                store_value(mod, field, value_map[field], reverse, data)
                sts = True
        if field_map and not sts:
            if broker_field in field_map:
                try:
                    store_value(mod, field, data[field_map[broker_field]], reverse)
                    sts = True
                except KeyError:
                    print("column {} missing from data".format(field_map[broker_field]))
            elif field in field_map:
                store_value(mod, field, data[field_map[field]], reverse)
                sts = True
        if broker_field in data and not sts:
            store_value(mod, field, data[broker_field], reverse)
        elif field in data and not sts:
            store_value(mod, field, data[field], reverse)

    if save:
        model_instance.save()
    if as_dict:
        return mod
    else:
        return model_instance


def store_value(model_instance_or_dict, field, value, reverse=None, data=None):
    # turn datetimes into dates
    if field.endswith("date") and isinstance(value, str):
        try:
            value = dateutil.parser.parse(value).date()
        except (TypeError, ValueError):
            pass

    # handles the value_map containing a function
    if callable(value) and data:
        value = value(data)

    if reverse and reverse.search(field):
        try:
            value = -1 * Decimal(value)
        except TypeError:
            pass

    if isinstance(model_instance_or_dict, dict):
        model_instance_or_dict[field] = value
    else:
        setattr(model_instance_or_dict, field, value)
