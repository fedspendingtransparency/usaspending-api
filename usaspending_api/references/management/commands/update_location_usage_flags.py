from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Case, F, Value, When, Q
from usaspending_api.references.models import Location, LegalEntity
from usaspending_api.awards.models import Award, Procurement, FinancialAssistanceAward
import logging
import django


class Command(BaseCommand):
    help = "Updates Location object usage flags based upon their current usage."
    logger = logging.getLogger('console')

    def handle(self, *args, **options):
        # Locations have a recipient flag of true if the number of legal entities using
        # them is greater than or equal to 1
        # We could combine all of the following procedures into one line, but that would
        # greatly reduce readability
        q1 = Q(location_id__in=LegalEntity.objects.values('location'))

        Location.objects.filter(q1).update(recipient_flag=True)
        Location.objects.filter(~q1).update(recipient_flag=False)

        # Locations have a pop flag if the number of the following models referencing them
        # is greater than or equal to 1
        # Referencing models: award, procurement, financialassistanceaward
        q1 = Q(location_id__in=Award.objects.values('place_of_performance'))
        q2 = Q(location_id__in=Procurement.objects.values('place_of_performance'))
        q3 = Q(location_id__in=FinancialAssistanceAward.objects.values('place_of_performance'))

        final_q = q1 | q2 | q3

        Location.objects.filter(final_q).update(place_of_performance_flag=True)
        Location.objects.filter(~final_q).update(place_of_performance_flag=False)
