from django.core.management.base import BaseCommand
from django.apps import apps
import logging


class Command(BaseCommand):
    help = "Generates a markdown file of a model's fields and help text \
            for use in documentation \
            Usage: `python manage.py generate_model_markdown <MODEL>`"
    logger = logging.getLogger("script")

    friendly_names = {
        "ForeignKey": "Relation",
        "OneToOneField": "Relation",
        "CharField": "String",
        "TextField": "String",
        "AutoField": "Integer",
        "DecimalField": "Float",
        "DateField": "Date",
        "DateTimeField": "Datetime",
        "BooleanField": "Boolean",
    }

    def add_arguments(self, parser):
        parser.add_argument("model", nargs=1, help="The model to generate markdown for")

    def handle(self, *args, **options):
        model_name = options["model"][0]
        model = apps.get_model(model_name)

        print("This data is represented internally as the model: `" + model.__name__ + "`")

        # Print the markdown header
        print("| Field | Type | Description |")
        print("| ----- | ----- | ----- |")

        for field in model._meta.get_fields():
            internal_type = field.get_internal_type()
            friendly_type = self.friendly_names.get(internal_type, internal_type)
            field_name = field.name
            if hasattr(field, "parent_link"):
                description = (
                    "Reverse look-up for relation from " + field.related_model.__name__ + "::" + field.field.name
                )
            elif field.primary_key and field.help_text == "":
                description = "Internal primary key. Guaranteed to be unique."
            else:
                description = field.help_text

            print("| " + field_name + " | " + friendly_type + " | " + description + " | ")
