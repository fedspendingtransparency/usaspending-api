from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Case, F, Value, When, Q
from usaspending_api.references.models import Location
import logging
import django


class Command(BaseCommand):
    help = "Updates Location object usage flags based upon their current usage."
    logger = logging.getLogger('console')

    def handle(self, *args, **options):
        annotated_set = Location.objects.annotate(legalentity_count=Count('legalentity'))
        annotated_set = annotated_set.annotate(award_count=Count('award'))
        annotated_set = annotated_set.annotate(procurement_count=Count('procurement'))
        annotated_set = annotated_set.annotate(assistance_count=Count('financialassistanceaward'))

        # Locations have a recipient flag of true if the number of legal entities using
        # them is greater than or equal to 1
        # We could combine all of the following procedures into one line, but that would
        # greatly reduce readability
        q1 = Q(legalentity_count__gte=1)

        recipient_ids = annotated_set.all().filter(q1).values_list('location_id', flat=True)
        non_recipient_ids = annotated_set.all().filter(~q1).values_list('location_id', flat=True)

        Location.objects.filter(location_id__in=recipient_ids).update(recipient_flag=True)
        Location.objects.filter(location_id__in=non_recipient_ids).update(recipient_flag=False)

        # Locations have a pop flag if the number of the following models referencing them
        # is greater than or equal to 1
        # Referencing models: award, procurement, financialassistanceaward
        q1 = Q(award_count__gte=1)
        q2 = Q(procurement_count__gte=1)
        q3 = Q(assistance_count__gte=1)

        final_q = q1 | q2 | q3

        pop_ids = annotated_set.all().filter(final_q).values_list('location_id', flat=True)
        non_pop_ids = annotated_set.all().filter(~final_q).values_list('location_id', flat=True)

        Location.objects.filter(location_id__in=pop_ids).update(place_of_performance_flag=True)
        Location.objects.filter(location_id__in=non_pop_ids).update(place_of_performance_flag=False)
