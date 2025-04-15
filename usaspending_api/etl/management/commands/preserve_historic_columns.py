import logging
from django.db import connection
from django.core.management.base import BaseCommand

logger = logging.getLogger("script")

class Command(BaseCommand):

    help = """
    This command simply copies columns from a historic copy of a table (stored in the temp schema) back into the table
    after a reload. 
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--table",
            type=str,
            required=True,
            help="The table name to pull from and to copy to between schemas.",
            choices=[
                "awards",
                "transaction_fabs",
                "transaction_fpds",
                "transaction_normalized",
            ],
        )
        parser.add_argument(
            "--historic-schema",
            type=str,
            required=True,
            default='temp',
            help="The schema storing the historic copy.",
        )
        parser.add_argument(
            "--preserved-fields",
            type=str,
            nargs='+',
            required=True,
            help="A list of columns (separated by spaces) to copy over.",
        )

    def preserve_historic_columns(self):
        dest_alias = 'dest'
        historic_alias = 'historic'

        update_cols = ', '.join([f'{self.dest_schema}.{preserved_field} = {self.historic_schema}.{preserved_field}'
                                 for preserved_field in self.preserved_fields])

        if self.table == 'awards':
            unique_fields = ['generated_unique_award_id']
        else:
            unique_fields = ['unique_award_key', 'transaction_unique_id']
        join_condition = ' AND '.join([f'{self.dest_schema}.{unique_field} = {self.historic_schema}.{unique_field}'
                                       for unique_field in unique_fields])

        copy_sql = f"""
            UPDATE {self.dest_schema}.{self.table_name} AS {dest_alias}
            SET
                {update_cols}
            FROM {self.historic_schema}.{self.table_name} AS {historic_alias}
            WHERE
                {join_condition};
        """

        with connection.cursor() as cursor:
            logger.info(f"Copying {self.preserved_fields} from {self.historic_schema}.{self.table_name}"
                        f" to {self.dest_schema}.{self.table_name}.")
            cursor.execute(copy_sql)

    def handle(self, *args, **options):
        # Resolve Parameters
        self.table_name = options["table"]
        self.dest_schema = 'int'
        self.historic_schema = options["historic_schema"]
        self.preserved_fields = options["preserved_fields"]

        self.preserve_historic_columns()
