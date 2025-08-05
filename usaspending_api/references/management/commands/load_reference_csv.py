import logging

from django.core.management.base import BaseCommand
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from usaspending_api.references.models import RefCountryCode, ObjectClass, RefProgramActivity


logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Loads a csv file into a reference table. Does not support mapping, so \
            the CSV must have the same column names as the reference model"

    def add_arguments(self, parser):
        parser.add_argument("model", nargs=1, help="the model of the reference to load", type=str)
        parser.add_argument("path", nargs=1, help="the path to the csv to load", type=str)
        parser.add_argument("encoding", nargs=1, help="the encoding to use to read this file", type=str)

    def handle(self, *args, **options):
        possible_models = {
            "RefCountryCode": RefCountryCode,
            "ObjectClass": ObjectClass,
            "RefProgramActivity": RefProgramActivity,
        }

        model = options["model"][0]
        path = options["path"][0]
        encoding = options["encoding"][0]

        if options["model"][0] not in possible_models.keys():
            logger.error("Model " + model + " is not supported")

        loader = ThreadedDataLoader(model_class=possible_models[model], collision_behavior="update")
        loader.load_from_file(path, encoding)
