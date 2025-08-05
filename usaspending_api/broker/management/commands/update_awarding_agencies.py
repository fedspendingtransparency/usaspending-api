import logging

from django.core.management.base import BaseCommand
from datetime import datetime
from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.common.helpers.timing_helpers import timer
from usaspending_api.references.models import Agency
from usaspending_api.search.models import AwardSearch

logger = logging.getLogger("script")


agency_no_sub_map = {
    (agency.toptier_agency.toptier_code, agency.subtier_agency.subtier_code): agency
    for agency in Agency.objects.filter(subtier_agency__isnull=False)
}
agency_cgac_only_map = {
    agency.toptier_agency.toptier_code: agency for agency in Agency.objects.filter(subtier_agency__isnull=True)
}


class Command(BaseCommand):

    help = "Updates empty awarding and funding agency fields on transactions and awards due to subtier/toptier mapping"

    @staticmethod
    def update_awarding_funding_agency(fiscal_year=None, file_type=None, page=1, limit=500000):
        """
        Uses the TransactionFPDS or TransactionFABS is present to update missing awarding and funding agency
        in TransactionNormalized and Awards
        """

        offset = (page - 1) * limit

        range_low = offset
        range_high = offset + limit

        if file_type == "D1":
            # List of Transaction FPDS mapping transaction ids, cgac code, and subtier code
            # Filters out FPDS transactions where the transaction is equal to the fiscal year
            transaction_cgac_subtier_map = [
                {
                    "transaction_id": transaction_FPDS["transaction_id"],
                    "awarding_toptier_code": transaction_FPDS["awarding_agency_code"],
                    "funding_toptier_code": transaction_FPDS["funding_agency_code"],
                    "awarding_subtier_code": transaction_FPDS["awarding_sub_tier_agency_c"],
                    "funding_subtier_code": transaction_FPDS["funding_sub_tier_agency_co"],
                }
                for transaction_FPDS in TransactionFPDS.objects.filter(transaction__fiscal_year=fiscal_year).values(
                    "transaction_id",
                    "awarding_agency_code",
                    "funding_agency_code",
                    "awarding_sub_tier_agency_c",
                    "funding_sub_tier_agency_co",
                )[range_low:range_high]
            ]
        elif file_type == "D2":
            # List of Transaction FABS mapping transaction ids, cgac code, and subtier code
            # Filters out FABS transactions where the where the transaction is equal to the fiscal year
            transaction_cgac_subtier_map = [
                {
                    "transaction_id": transaction_FABS["transaction_id"],
                    "awarding_toptier_code": transaction_FABS["awarding_agency_code"],
                    "funding_toptier_code": transaction_FABS["funding_agency_code"],
                    "awarding_subtier_code": transaction_FABS["awarding_sub_tier_agency_c"],
                    "funding_subtier_code": transaction_FABS["funding_sub_tier_agency_co"],
                }
                for transaction_FABS in TransactionFABS.objects.filter(transaction__fiscal_year=fiscal_year).values(
                    "transaction_id",
                    "awarding_agency_code",
                    "funding_agency_code",
                    "awarding_sub_tier_agency_c",
                    "funding_sub_tier_agency_co",
                )[range_low:range_high]
            ]

        total_rows = len(transaction_cgac_subtier_map)

        logger.info("Processing " + str(total_rows) + " rows of transaction data")
        logger.info("Rows range from {} to {}".format(range_low, range_high))

        # Go through each D1 or D2 transaction to update awarding/funding agency if missing

        index = 1

        start_time = datetime.now()
        for row in transaction_cgac_subtier_map:

            if not (index % 100):
                logger.info(
                    "Updating agencies: Loading row {} of {} ({})".format(
                        str(index), str(total_rows), datetime.now() - start_time
                    )
                )

            index += 1

            # Find corresponding transaction
            transaction = TransactionNormalized.objects.filter(id=row["transaction_id"]).first()

            # Skips transaction if unable to find it in Transaction Normalized
            if transaction is None:
                logger.error("Unable to find Transaction {}".format(str(row["transaction_id"])))
                continue

            # Find the agency that this award transaction belongs to. If it doesn't exist, create it.
            awarding_agency = agency_no_sub_map.get((row["awarding_toptier_code"], row["awarding_subtier_code"]))

            if awarding_agency is None:
                awarding_agency = agency_cgac_only_map.get(row["awarding_toptier_code"])

            funding_agency = agency_no_sub_map.get((row["funding_toptier_code"], row["funding_subtier_code"]))

            if funding_agency is None:
                funding_agency = agency_cgac_only_map.get(row["funding_toptier_code"])

            # If unable to get agency moves on to the next transaction
            if awarding_agency is None and funding_agency is None:
                logger.error(
                    "Unable to find awarding agency CGAC {} Subtier {} and funding agency CGAC {} Subtier {}".format(
                        row["awarding_toptier_code"],
                        row["awarding_subtier_code"],
                        row["funding_toptier_code"],
                        row["awarding_subtier_code"],
                    )
                )
                continue

            if awarding_agency is None:
                logger.error(
                    "Unable to find awarding agency for CGAC {} Subtier {}".format(
                        row["awarding_toptier_code"], row["awarding_subtier_code"]
                    )
                )

            elif funding_agency is None:
                pass

            transaction.awarding_agency = awarding_agency
            transaction.funding_agency = funding_agency

            award = AwardSearch.objects.filter(award_id=transaction.award.id).first()

            if award is None:
                logger.error("Unable to find Award {}".format(str(transaction.award.id)))
                continue

            award.awarding_agency_id = awarding_agency.id

            award.funding_agency_id = funding_agency.id

            try:
                transaction.save()
                award.save()

            except Exception as e:
                logger.error(
                    "Unable to save Transaction {} and Award {}:{}".format(str(transaction.id), str(award.id), str(e))
                )

    def add_arguments(self, parser):

        parser.add_argument(
            "--fiscal_year",
            dest="fiscal_year",
            nargs="+",
            type=int,
            help="Year for which to run awarding agency clean up on",
        )

        parser.add_argument(
            "--assistance",
            action="store_true",
            dest="assistance",
            default=False,
            help="Runs the award only for Award Financial Assistance (Assistance) data",
        )

        parser.add_argument(
            "--contracts",
            action="store_true",
            dest="contracts",
            default=False,
            help="Runs the historical loader only for Award Procurement (Contract) data",
        )

        parser.add_argument("--page", dest="page", nargs="+", type=int, help="Page for batching and parallelization")

        parser.add_argument("--limit", dest="limit", nargs="+", type=int, help="Limit for batching and parallelization")

    def handle(self, *args, **options):
        logger.info("Starting updating awarding agencies...")

        fiscal_year = options.get("fiscal_year")[0]

        page = options.get("page")
        limit = options.get("limit")

        page = page[0] if page else 1
        limit = limit[0] if limit else 500000

        if options.get("contracts", None):
            with timer("D1 (contracts/FPDS) awarding/funding agencies updates", logger.info):
                self.update_awarding_funding_agency(fiscal_year, "D1", page=page, limit=limit)

        elif options.get("assistance", None):
            with timer("D2 (assistance/FABS) awarding/funding agencies updates", logger.info):
                self.update_awarding_funding_agency(fiscal_year, "D2", page=page, limit=limit)

        else:
            logger.error("Not a valid data type: --assistance,--contracts")

        logger.info("Finished")
