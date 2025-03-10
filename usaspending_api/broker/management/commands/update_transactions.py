import logging

from datetime import datetime

from django.conf import settings

from django.core.management.base import BaseCommand
from django.db import connections, transaction as db_transaction, IntegrityError

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.timing_helpers import timer
from usaspending_api.etl.award_helpers import update_awards, update_procurement_awards, update_assistance_awards
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.load_base import format_date, load_data_into_model
from usaspending_api.references.models import Agency, SubtierAgency, ToptierAgency
from usaspending_api.search.models import AwardSearch

logger = logging.getLogger("script")
exception_logger = logging.getLogger("exceptions")

# Lists to store for update_awards and update_procurement_awards
award_update_id_list = []
award_contract_update_id_list = []
award_assistance_update_id_list = []

subtier_agency_map = {
    subtier_agency["subtier_code"]: subtier_agency["subtier_agency_id"]
    for subtier_agency in SubtierAgency.objects.values("subtier_code", "subtier_agency_id")
}
subtier_to_agency_map = {
    agency["subtier_agency_id"]: {"agency_id": agency["id"], "toptier_agency_id": agency["toptier_agency_id"]}
    for agency in Agency.objects.values("id", "toptier_agency_id", "subtier_agency_id")
}
toptier_agency_map = {
    toptier_agency["toptier_agency_id"]: toptier_agency["toptier_code"]
    for toptier_agency in ToptierAgency.objects.values("toptier_agency_id", "toptier_code")
}


class Command(BaseCommand):
    help = "Update historical transaction data for a fiscal year from the Broker."

    @staticmethod
    def update_transaction_assistance(db_cursor, fiscal_year=None, page=1, limit=500000):

        query = "SELECT * FROM published_fabs"
        arguments = []

        fy_begin = "10/01/" + str(fiscal_year - 1)
        fy_end = "09/30/" + str(fiscal_year)

        if fiscal_year:
            if arguments:
                query += " AND"
            else:
                query += " WHERE"
            query += " action_date::Date BETWEEN %s AND %s"
            arguments += [fy_begin]
            arguments += [fy_end]
        query += " ORDER BY published_fabs_id LIMIT %s OFFSET %s"
        arguments += [limit, (page - 1) * limit]

        logger.info(
            "Executing query on Broker DB => " + query % (arguments[0], arguments[1], arguments[2], arguments[3])
        )

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        award_financial_assistance_data = dictfetchall(db_cursor)

        fabs_normalized_field_map = {
            "type": "assistance_type",
            "description": "award_description",
            "funding_amount": "total_funding_amount",
        }

        fabs_field_map = {
            "officer_1_name": "high_comp_officer1_full_na",
            "officer_1_amount": "high_comp_officer1_amount",
            "officer_2_name": "high_comp_officer2_full_na",
            "officer_2_amount": "high_comp_officer2_amount",
            "officer_3_name": "high_comp_officer3_full_na",
            "officer_3_amount": "high_comp_officer3_amount",
            "officer_4_name": "high_comp_officer4_full_na",
            "officer_4_amount": "high_comp_officer4_amount",
            "officer_5_name": "high_comp_officer5_full_na",
            "officer_5_amount": "high_comp_officer5_amount",
        }

        logger.info("Getting total rows")

        total_rows = len(award_financial_assistance_data)  # - rows_loaded

        logger.info("Processing " + str(total_rows) + " rows of assistance data")

        # ROW ITERATION STARTS HERE

        award_bulk = []

        transaction_assistance_bulk = []
        transaction_normalized_bulk = []

        awarding_agency_list = []
        funding_agency_list = []

        logger.info("Getting award objects for {} rows...".format(len(award_financial_assistance_data)))
        for index, row in enumerate(award_financial_assistance_data, 1):
            # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
            # use the sub tier code to look it up. This code assumes that all incoming
            # records will supply an awarding subtier agency code
            if row["awarding_agency_code"] is None or len(row["awarding_agency_code"].strip()) < 1:
                awarding_subtier_agency_id = subtier_agency_map[row["awarding_sub_tier_agency_c"]]
                awarding_toptier_agency_id = subtier_to_agency_map[awarding_subtier_agency_id]["toptier_agency_id"]
                awarding_toptier_code = toptier_agency_map[awarding_toptier_agency_id]
                row["awarding_agency_code"] = awarding_toptier_code

            # If funding toptier agency code (aka CGAC) is empty, try using the sub
            # tier funding code to look it up. Unlike the awarding agency, we can't
            # assume that the funding agency subtier code will always be present.
            if row["funding_agency_code"] is None or len(row["funding_agency_code"].strip()) < 1:
                funding_subtier_agency_id = subtier_agency_map.get(row["funding_sub_tier_agency_co"])
                if funding_subtier_agency_id is not None:
                    funding_toptier_agency_id = subtier_to_agency_map[funding_subtier_agency_id]["toptier_agency_id"]
                    funding_toptier_code = toptier_agency_map[funding_toptier_agency_id]
                else:
                    funding_toptier_code = None
                row["funding_agency_code"] = funding_toptier_code

            # Find the award that this award transaction belongs to. If it doesn't exist, create it.
            awarding_agency = Agency.get_by_toptier_subtier(
                row["awarding_agency_code"], row["awarding_sub_tier_agency_c"]
            )
            funding_agency = Agency.get_by_toptier_subtier(
                row["funding_agency_code"], row["funding_sub_tier_agency_co"]
            )

            awarding_agency_list.append(awarding_agency)
            funding_agency_list.append(funding_agency)

            # award.save() is called in Award.get_or_create_summary_award by default
            created, award = AwardSearch.get_or_create_summary_award(
                awarding_agency_id=awarding_agency.id,
                fain=row.get("fain"),
                uri=row.get("uri"),
                generated_unique_award_id=row.get("unique_award_key"),
                save=False,
            )

            award_bulk.append(award)
            award_update_id_list.append(award.award_id)
            award_assistance_update_id_list.append(award.award_id)

        logger.info("Bulk creating {} award rows...".format(len(award_bulk)))
        try:
            AwardSearch.objects.bulk_create(award_bulk)
        except IntegrityError:
            logger.info("!!! DUPLICATES FOUND. Continuing... ")

        logger.info("Getting transaction_normalized for {} rows...".format(len(award_financial_assistance_data)))
        for index, row in enumerate(award_financial_assistance_data, 1):

            parent_txn_value_map = {
                "award": award_bulk[index - 1],
                "awarding_agency": awarding_agency_list[index - 1],
                "funding_agency": funding_agency_list[index - 1],
                "period_of_performance_start_date": format_date(row["period_of_performance_star"]),
                "period_of_performance_current_end_date": format_date(row["period_of_performance_curr"]),
                "action_date": format_date(row["action_date"]),
            }

            transaction_dict = load_data_into_model(
                TransactionNormalized(),  # thrown away
                row,
                field_map=fabs_normalized_field_map,
                value_map=parent_txn_value_map,
                as_dict=True,
            )

            transaction_normalized = TransactionNormalized.get_or_create_transaction(**transaction_dict)
            transaction_normalized.fiscal_year = fy(transaction_normalized.action_date)
            transaction_normalized_bulk.append(transaction_normalized)

        logger.info("Bulk creating {} TransactionNormalized rows...".format(len(transaction_normalized_bulk)))
        try:
            TransactionNormalized.objects.bulk_create(transaction_normalized_bulk)
        except IntegrityError:
            logger.info("Tried and failed to insert duplicate transaction_normalized row. Continuing... ")

        for index, row in enumerate(award_financial_assistance_data, 1):
            financial_assistance_data = load_data_into_model(
                TransactionFABS(), row, field_map=fabs_field_map, as_dict=True  # thrown away
            )

            transaction_assistance = TransactionFABS(
                transaction=transaction_normalized_bulk[index - 1], **financial_assistance_data
            )
            transaction_assistance_bulk.append(transaction_assistance)

        logger.info("Bulk creating TransactionFABS rows...")
        try:
            TransactionFABS.objects.bulk_create(transaction_assistance_bulk)
        except IntegrityError:
            logger.info("!!! DUPLICATES FOUND. Continuing... ")

    ######################################################

    @staticmethod
    def update_transaction_contract(db_cursor, fiscal_year=None, page=1, limit=500000):

        # logger.info("Getting IDs for what's currently in the DB...")
        # current_ids = TransactionFPDS.objects
        #
        # if fiscal_year:
        #     current_ids = current_ids.filter(action_date__fy=fiscal_year)
        #
        # current_ids = current_ids.values_list('detached_award_procurement_id', flat=True)

        query = "SELECT * FROM detached_award_procurement"
        arguments = []

        fy_begin = "10/01/" + str(fiscal_year - 1)
        fy_end = "09/30/" + str(fiscal_year)

        if fiscal_year:
            if arguments:
                query += " AND"
            else:
                query += " WHERE"
            query += " action_date::Date BETWEEN %s AND %s"
            arguments += [fy_begin]
            arguments += [fy_end]
        query += " ORDER BY detached_award_procurement_id LIMIT %s OFFSET %s"
        arguments += [limit, (page - 1) * limit]

        logger.info(
            "Executing query on Broker DB => " + query % (arguments[0], arguments[1], arguments[2], arguments[3])
        )

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        procurement_data = dictfetchall(db_cursor)

        fpds_normalized_field_map = {"type": "contract_award_type", "description": "award_description"}

        fpds_field_map = {
            "officer_1_name": "high_comp_officer1_full_na",
            "officer_1_amount": "high_comp_officer1_amount",
            "officer_2_name": "high_comp_officer2_full_na",
            "officer_2_amount": "high_comp_officer2_amount",
            "officer_3_name": "high_comp_officer3_full_na",
            "officer_3_amount": "high_comp_officer3_amount",
            "officer_4_name": "high_comp_officer4_full_na",
            "officer_4_amount": "high_comp_officer4_amount",
            "officer_5_name": "high_comp_officer5_full_na",
            "officer_5_amount": "high_comp_officer5_amount",
        }

        logger.info("Getting total rows")

        total_rows = len(procurement_data)  # - rows_loaded

        logger.info("Processing " + str(total_rows) + " rows of procurement data")

        start_time = datetime.now()
        for index, row in enumerate(procurement_data, 1):
            with db_transaction.atomic():

                if not (index % 100):
                    logger.info(
                        "D1 File Load: Loading row {} of {} ({})".format(
                            str(index), str(total_rows), datetime.now() - start_time
                        )
                    )

                # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
                # use the sub tier code to look it up. This code assumes that all incoming
                # records will supply an awarding subtier agency code
                if row["awarding_agency_code"] is None or len(row["awarding_agency_code"].strip()) < 1:
                    awarding_subtier_agency_id = subtier_agency_map[row["awarding_sub_tier_agency_c"]]
                    awarding_toptier_agency_id = subtier_to_agency_map[awarding_subtier_agency_id]["toptier_agency_id"]
                    awarding_toptier_code = toptier_agency_map[awarding_toptier_agency_id]
                    row["awarding_agency_code"] = awarding_toptier_code

                # If funding toptier agency code (aka CGAC) is empty, try using the sub
                # tier funding code to look it up. Unlike the awarding agency, we can't
                # assume that the funding agency subtier code will always be present.
                if row["funding_agency_code"] is None or len(row["funding_agency_code"].strip()) < 1:
                    funding_subtier_agency_id = subtier_agency_map.get(row["funding_sub_tier_agency_co"])
                    if funding_subtier_agency_id is not None:
                        funding_toptier_agency_id = subtier_to_agency_map[funding_subtier_agency_id][
                            "toptier_agency_id"
                        ]
                        funding_toptier_code = toptier_agency_map[funding_toptier_agency_id]
                    else:
                        funding_toptier_code = None
                    row["funding_agency_code"] = funding_toptier_code

                # Find the award that this award transaction belongs to. If it doesn't exist, create it.
                awarding_agency = Agency.get_by_toptier_subtier(
                    row["awarding_agency_code"], row["awarding_sub_tier_agency_c"]
                )
                created, award = AwardSearch.get_or_create_summary_award(
                    awarding_agency_id=awarding_agency.id,
                    piid=row.get("piid"),
                    fain=row.get("fain"),
                    uri=row.get("uri"),
                    parent_award_piid=row.get("parent_award_id"),
                    generated_unique_award_id=row.get("unique_award_key"),
                )
                award.save()

                award_update_id_list.append(award.award_id)
                award_contract_update_id_list.append(award.award_id)

                parent_txn_value_map = {
                    "award": award,
                    "awarding_agency": awarding_agency,
                    "funding_agency": Agency.get_by_toptier_subtier(
                        row["funding_agency_code"], row["funding_sub_tier_agency_co"]
                    ),
                    "period_of_performance_start_date": format_date(row["period_of_performance_star"]),
                    "period_of_performance_current_end_date": format_date(row["period_of_performance_curr"]),
                    "action_date": format_date(row["action_date"]),
                }

                transaction_dict = load_data_into_model(
                    TransactionNormalized(),  # thrown away
                    row,
                    field_map=fpds_normalized_field_map,
                    value_map=parent_txn_value_map,
                    as_dict=True,
                )

                transaction = TransactionNormalized.get_or_create_transaction(**transaction_dict)
                transaction.save()

                contract_instance = load_data_into_model(
                    TransactionFPDS(), row, field_map=fpds_field_map, as_dict=True  # thrown away
                )

                transaction_contract = TransactionFPDS(transaction=transaction, **contract_instance)
                # catch exception and do nothing if we see
                # "django.db.utils.IntegrityError: duplicate key value violates unique constraint"
                try:
                    transaction_contract.save()
                except IntegrityError:
                    pass

    def add_arguments(self, parser):

        parser.add_argument(
            "--fiscal_year", dest="fiscal_year", nargs="+", type=int, help="Year for which to run the historical load"
        )

        parser.add_argument(
            "--assistance",
            action="store_true",
            dest="assistance",
            default=False,
            help="Runs the historical loader only for Award Financial Assistance (Assistance) data",
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

    # @transaction.atomic
    def handle(self, *args, **options):
        logger.info("Starting historical data load...")

        db_cursor = connections[settings.DATA_BROKER_DB_ALIAS].cursor()
        fiscal_year = options.get("fiscal_year")
        page = options.get("page")
        limit = options.get("limit")

        if fiscal_year:
            fiscal_year = fiscal_year[0]
            logger.info("Processing data for Fiscal Year " + str(fiscal_year))
        else:
            fiscal_year = 2017

        page = page[0] if page else 1
        limit = limit[0] if limit else 500000

        if not options["assistance"]:
            with timer("D1 historical data load", logger.info):
                self.update_transaction_contract(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit)

        if not options["contracts"]:
            with timer("D2 historical data load", logger.info):
                self.update_transaction_assistance(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit)

        with timer("updating awards to reflect their latest associated transaction info", logger.info):
            update_awards(tuple(award_update_id_list))

        with timer("updating assistance-specific awards to reflect their latest transaction info", logger.info):
            update_assistance_awards(tuple(award_assistance_update_id_list))

        with timer("updating contract-specific awards to reflect their latest transaction info", logger.info):
            update_procurement_awards(tuple(award_contract_update_id_list))

        # Done!
        logger.info("FINISHED")
