from django.core.management.base import BaseCommand
from django.core.serializers.json import json, DjangoJSONEncoder
from rest_framework_tracking.models import APIRequestLog
import datetime
from datetime import timedelta, tzinfo
import logging


class Command(BaseCommand):
    help = "Generates json formatted analytics log from the APIRequestLog object \
            provided by drf-tracking. From 12:00:01 AM on Start date to 11:59:59 PM on \
            the specified ending date. Defaults to the logs from the current day\
            Usage: `python manage.py generate_analytics_json_file <start date> <end date>`"
    logger = logging.getLogger("console")

    def add_arguments(self, parser):
        parser.add_argument(
            "start_date", nargs="?", default=str(datetime.date.today()), help="The start date, in YYYY-MM-DD"
        )
        parser.add_argument(
            "end_date", nargs="?", default=str(datetime.date.today()), help="The end date, in YYYY-MM-DD"
        )

    def handle(self, *args, **options):
        start_date = datetime.datetime.strptime(options["start_date"], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(options["end_date"], "%Y-%m-%d")

        start_date = start_date.replace(hour=0, minute=0, second=1, tzinfo=UTC())
        end_date = end_date.replace(hour=23, minute=59, second=59, tzinfo=UTC())

        # Get the APIRequestLogs for this time span
        analytics = APIRequestLog.objects.filter(requested_at__gte=start_date, requested_at__lte=end_date)

        print(analytics.count())

        for log in analytics:
            data = dict(log.__dict__)
            del data["_state"]  # Strip this out as (1) we don't need it and (2) it's not serializable
            print(json.dumps(data, indent=4, cls=DjangoJSONEncoder))
        # print(json.dumps(endpoints, indent=4, cls=DjangoJSONEncoder))


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)
