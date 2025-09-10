from django.core.management.base import BaseCommand
from typing import List, Any
from elasticsearch_dsl import Q as ES_Q


class Command(BaseCommand):

    filter_values = List[str]
    options = dict[str, Any]

    def handle(self, *args, **options) -> ES_Q:
        self.filter_values = options["filter_values"]