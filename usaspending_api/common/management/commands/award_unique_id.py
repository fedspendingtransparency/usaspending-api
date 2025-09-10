from django.core.management.base import BaseCommand
from typing import List
from elasticsearch_dsl import Q as ES_Q

from usaspending_api.search.v2.es_sanitization import es_sanitize


class Command(BaseCommand):
    filter_values = List[str]
    field = str

    def add_arguments(self, parser):
        parser.add_argument("field", type=str, required=True)

    def handle(self, *args, **options):
        self.filter_values = options['filter_values']
        self.field = options['field']
        query = es_sanitize(self.filter_values)
        id_query = ES_Q("query_string", query=query, default_operator="AND", fields=self.field)
        return ES_Q("bool", should=id_query, minimum_should_match=1)
