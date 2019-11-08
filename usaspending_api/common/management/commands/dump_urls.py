import re

from django.core.management.base import BaseCommand


class Command(BaseCommand):

    help = "Dump the list of URLs known by Django"

    def handle(self, *args, **options):
        from usaspending_api import urls

        trim_stuff = re.compile(r"^\^|\$$")

        def _dump_urls(base, urls):
            for url in urls:
                o = base + trim_stuff.sub("", url.regex.pattern)
                if hasattr(url, "url_patterns"):
                    _dump_urls(o, url.url_patterns)
                else:
                    print(o)

        _dump_urls("/", urls.urlpatterns)
