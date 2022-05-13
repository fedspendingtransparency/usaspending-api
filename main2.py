from django.core.management import execute_from_command_line
from usaspending_api.etl.management.commands.create_delta_table import Command
from usaspending_api import settings_databricks

import os

os.environ.setdefault("DDM_CONTAINER_NAME", "app")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "usaspending_api.settings_databricks")

# execute_from_command_line(["manage.py", "create_delta_table", "--destination-table", "sam_recipient"])
execute_from_command_line(["manage.py", "check"])
