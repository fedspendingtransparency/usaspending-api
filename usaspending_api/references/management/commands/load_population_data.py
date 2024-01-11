import logging

from django.core.management.base import BaseCommand

from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.references.management.commands.population_data_loaders.loader_factories import (
    CountryPopulationLoaderFactory,
    CountyPopulationLoaderFactory,
    DistrictPopulationLoaderFactory,
)


logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Load CSV files containing population data. "

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Path or URI of the raw object class CSV file to be loaded.")
        parser.add_argument(
            "--type",
            choices=["county", "district", "country"],
            required=True,
            help="Load county, congressional district, or country population data. Data sources vary between choices.",
        )

    def handle(self, *args, **options):
        self.type = options["type"]
        file = options["file"]
        logger.info(f"Loading {self.type} Population data from {file}")

        loader_factory = None
        if self.type == "county":
            loader_factory = CountyPopulationLoaderFactory()
        elif self.type == "district":
            loader_factory = DistrictPopulationLoaderFactory()
        elif self.type == "country":
            loader_factory = CountryPopulationLoaderFactory()
        else:
            raise RuntimeError(f'No loader factory found for the provided "--type" argument: {self.type}')
        loader = loader_factory.create_population_loader()

        csv_dict_list = read_csv_file_as_list_of_dictionaries(options["file"])
        if csv_dict_list:
            cols = csv_dict_list[0].keys()
        else:
            raise RuntimeError(f"No data in CSV {options['file']}")

        loader.drop_temp_tables()
        loader.create_tables(columns=cols)
        loader.load_data(csv_dict_list)
        loader.cleanup()
