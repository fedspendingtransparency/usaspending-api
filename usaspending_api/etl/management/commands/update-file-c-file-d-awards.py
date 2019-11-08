from datetime import datetime
import logging
from django.db import transaction
from usaspending_api.awards.models import Award, FinancialAccountsByAwards
from usaspending_api.references.models import Agency
from django.core.management.base import BaseCommand

logger = logging.getLogger("console")


class Command(BaseCommand):
    """
    This command will grab a file c award and remap it to the correct file d award
    """

    help = "remap file c awards to their corresponding file d award"

    @transaction.atomic
    def handle(self, *args, **options):
        remapped_awards = []
        bad_faba = []
        logger.info("starting File C awards remapping")

        # 1) Get all file C awards
        faba_queryset = FinancialAccountsByAwards.objects.filter(
            award__isnull=False, award__recipient_id__isnull=True, treasury_account__federal_account_id=1411
        ).values(
            "piid",
            "fain",
            "uri",
            "award_id",
            "financial_accounts_by_awards_id",
            "award__awarding_agency__toptier_agency_id",
        )

        start_time = datetime.now()
        size_of = str(len(faba_queryset))
        awards = Award.objects  # this stops the property lookup each iteration, saving 3+ seconds every 100 rows!
        possible_agencies_cache = {}
        for index, faba in enumerate(faba_queryset, 1):
            if not (index % 1000):
                logger.info(
                    "command update: Loading row {} of {} ({})".format(str(index), size_of, datetime.now() - start_time)
                )
            file_c_award_id = faba["award_id"]

            ttaid = faba["award__awarding_agency__toptier_agency_id"]
            if ttaid not in possible_agencies_cache.keys():
                possible_agencies_cache[ttaid] = Agency.objects.filter(toptier_agency_id=ttaid).values("id")

            possible_agencies = possible_agencies_cache[ttaid]

            # split up the query based on null vals
            kwargs = {}
            kwargs["recipient_id__isnull"] = False
            if faba["piid"] is not None:
                kwargs["piid"] = faba["piid"]
            if faba["fain"] is not None:
                kwargs["fain"] = faba["fain"]
            if faba["uri"] is not None:
                kwargs["uri"] = faba["uri"]

            award_queryset = awards.filter(**kwargs).values("id", "awarding_agency_id")

            award_count = len(award_queryset)

            file_d_award = None
            if award_count == 1:
                file_d_award = award_queryset[0]
            if file_d_award and file_d_award["awarding_agency_id"] in map(lambda d: d["id"], possible_agencies):
                faba["award_id"] = file_d_award["id"]
                remapped_awards.append("{} to {}.".format(file_c_award_id, file_d_award["id"]))
            else:
                bad_faba.append(faba["financial_accounts_by_awards_id"])

        logger.info("ending File C awards remapping. Elapsed Time ({})".format(datetime.now() - start_time))
        logger.info("REMAPPED:  {}".format(remapped_awards))
        logger.info("--------------------------------------------")
        logger.info("Bad but not REMAPPED:  {}".format(bad_faba))
