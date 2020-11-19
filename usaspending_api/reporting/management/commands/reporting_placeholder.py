from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """ Dummy command. Remove after creating a command in this folder"""

    def handle(self, *args, **options):
        print("Success")
