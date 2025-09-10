from django.core.management.base import BaseCommand
from typing import List
from elasticsearch_dsl import Q as ES_Q

class Command(BaseCommand):
    filter_values = List[str]

    def handle(self, *args, **options):
        self.filter_values = options["filter_values"]
        nested_path = options.get("nested_path", "")
        non_zero_queries = []
        for field in self.filter_values:
            field_name = f"{nested_path}{'.' if nested_path else ''}{field}"
            non_zero_queries.append(ES_Q("range", **{field_name: {"gt": 0}}))
            non_zero_queries.append(ES_Q("range", **{field_name: {"lt": 0}}))
        return ES_Q("bool", should=non_zero_queries, minimum_should_match=1)