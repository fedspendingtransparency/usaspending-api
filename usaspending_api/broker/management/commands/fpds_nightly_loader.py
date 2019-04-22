import boto3
import csv
import logging
import os
import re
import time

from datetime import datetime, timedelta, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.awards.models import TransactionFPDS, TransactionNormalized, Award
from usaspending_api.broker.helpers.find_related_awards import find_related_awards
from usaspending_api.broker.helpers.get_business_categories import get_business_categories
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.broker.helpers.set_legal_entity_boolean_fields import set_legal_entity_boolean_fields
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values
from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.common.helpers.generic_helper import fy, timer
from usaspending_api.etl.award_helpers import (
    update_awards,
    update_contract_awards,
    update_award_categories,
    award_types,
)
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, create_location
from usaspending_api.references.models import LegalEntity, Agency


logger = logging.getLogger("console")

AWARD_UPDATE_ID_LIST = []
BATCH_FETCH_SIZE = 25000


class Command(BaseCommand):
    help = "Sync USAspending DB FPDS data using Broker for new or modified records and S3 for deleted IDs"

    @staticmethod
    def get_deleted_fpds_data_from_s3(date):
        ids_to_delete = []
        regex_str = ".*_delete_records_(IDV|award).*"

        if settings.IS_LOCAL:
            for file in os.listdir(settings.CSV_LOCAL_PATH):
                if re.search(regex_str, file) and datetime.strptime(file[: file.find("_")], "%m-%d-%Y").date() >= date:
                    with open(settings.CSV_LOCAL_PATH + file, "r") as current_file:
                        # open file, split string to array, skip the header
                        reader = csv.reader(current_file.read().splitlines())
                        next(reader)
                        unique_key_list = [rows[0] for rows in reader]

                        ids_to_delete += unique_key_list
        else:
            # Connect to AWS
            aws_region = settings.USASPENDING_AWS_REGION
            fpds_bucket_name = settings.FPDS_BUCKET_NAME

            if not (aws_region and fpds_bucket_name):
                raise Exception("Missing required environment variables: USASPENDING_AWS_REGION, FPDS_BUCKET_NAME")

            s3client = boto3.client("s3", region_name=aws_region)
            s3resource = boto3.resource("s3", region_name=aws_region)
            s3_bucket = s3resource.Bucket(fpds_bucket_name)

            # make an array of all the keys in the bucket
            file_list = [item.key for item in s3_bucket.objects.all()]

            # Only use files that match the date we're currently checking
            for item in file_list:
                # if the date on the file is the same day as we're checking
                if (
                    re.search(regex_str, item)
                    and "/" not in item
                    and datetime.strptime(item[: item.find("_")], "%m-%d-%Y").date() >= date
                ):
                    s3_item = s3client.get_object(Bucket=fpds_bucket_name, Key=item)
                    reader = csv.reader(s3_item["Body"].read().decode("utf-8").splitlines())

                    # skip the header, the reader doesn't ignore it for some reason
                    next(reader)
                    # make an array of all the detached_award_procurement_ids
                    unique_key_list = [rows[0] for rows in reader]

                    ids_to_delete += unique_key_list

        logger.info("Number of records to delete: %s" % str(len(ids_to_delete)))
        return ids_to_delete

    @staticmethod
    def get_fpds_transaction_ids(date):
        db_cursor = connections["data_broker"].cursor()
        db_query = "SELECT detached_award_procurement_id FROM detached_award_procurement WHERE updated_at >= %s;"
        db_args = [date]

        db_cursor.execute(db_query, db_args)
        db_rows = [id[0] for id in db_cursor.fetchall()]

        logger.info("Number of records to insert/update: %s" % str(len(db_rows)))
        return db_rows

    @staticmethod
    def fetch_fpds_data_generator(dap_uid_list):
        start_time = datetime.now()

        db_cursor = connections["data_broker"].cursor()

        db_query = "SELECT * FROM detached_award_procurement WHERE detached_award_procurement_id IN ({});"

        total_uid_count = len(dap_uid_list)

        for i in range(0, total_uid_count, BATCH_FETCH_SIZE):
            max_index = i + BATCH_FETCH_SIZE if i + BATCH_FETCH_SIZE < total_uid_count else total_uid_count
            fpds_ids_batch = dap_uid_list[i:max_index]

            log_msg = "[{}] Fetching {}-{} out of {} records from broker"
            logger.info(log_msg.format(datetime.now() - start_time, i + 1, max_index, total_uid_count))

            db_cursor.execute(db_query.format(",".join(str(id) for id in fpds_ids_batch)))
            yield dictfetchall(db_cursor)  # this returns an OrderedDict

    def delete_stale_fpds(self, ids_to_delete):
        logger.info("Starting deletion of stale FPDS data")

        transactions = TransactionNormalized.objects.filter(
            contract_data__detached_award_procurement_id__in=ids_to_delete
        )
        update_award_ids, delete_award_ids = find_related_awards(transactions)

        delete_transaction_ids = [delete_result[0] for delete_result in transactions.values_list("id")]
        delete_transaction_str_ids = ",".join([str(deleted_result) for deleted_result in delete_transaction_ids])
        update_award_str_ids = ",".join([str(update_result) for update_result in update_award_ids])
        delete_award_str_ids = ",".join([str(deleted_result) for deleted_result in delete_award_ids])

        db_cursor = connections["default"].cursor()
        queries = []

        if delete_transaction_ids:
            fpds = "DELETE FROM transaction_fpds tf WHERE tf.transaction_id IN ({});".format(delete_transaction_str_ids)
            # Transaction Normalized
            tn = "DELETE FROM transaction_normalized tn WHERE tn.id IN ({});".format(delete_transaction_str_ids)
            queries.extend([fpds, tn])
        # Update Awards
        if update_award_ids:
            # Adding to AWARD_UPDATE_ID_LIST so the latest_transaction will be recalculated
            AWARD_UPDATE_ID_LIST.extend(update_award_ids)
            query_str = "UPDATE awards SET latest_transaction_id = null WHERE id IN ({});"
            update_awards_query = query_str.format(update_award_str_ids)
            queries.append(update_awards_query)
        if delete_award_ids:
            # Financial Accounts by Awards
            query_str = "UPDATE financial_accounts_by_awards SET award_id = null WHERE award_id IN ({});"
            fa = query_str.format(delete_award_str_ids)
            # Subawards
            sub = "UPDATE subaward SET award_id = null WHERE award_id IN ({});".format(delete_award_str_ids)
            # Parent Awards
            pa_updates = "UPDATE parent_award SET parent_award_id = null WHERE parent_award_id IN ({});".format(
                delete_award_str_ids
            )
            pa_deletes = "DELETE FROM parent_award WHERE award_id IN ({});".format(delete_award_str_ids)
            # Delete Subawards
            delete_awards_query = "DELETE FROM awards a WHERE a.id IN ({});".format(delete_award_str_ids)
            queries.extend([fa, sub, pa_updates, pa_deletes, delete_awards_query])
        if queries:
            db_query = "".join(queries)
            db_cursor.execute(db_query, [])

    def insert_all_new_fpds(self, total_insert):
        for to_insert in self.fetch_fpds_data_generator(total_insert):
            start = time.perf_counter()
            self.insert_new_fpds(to_insert=to_insert, total_rows=len(to_insert))
            logger.info("Insertion took {:.2f}s".format(time.perf_counter() - start))

    def insert_new_fpds(self, to_insert, total_rows):
        place_of_performance_field_map = {
            "location_country_code": "place_of_perform_country_c",
            "country_name": "place_of_perf_country_desc",
            "state_code": "place_of_performance_state",
            "state_name": "place_of_perfor_state_desc",
            "city_name": "place_of_perform_city_name",
            "county_name": "place_of_perform_county_na",
            "county_code": "place_of_perform_county_co",
            "zip_4a": "place_of_performance_zip4a",
            "congressional_code": "place_of_performance_congr",
            "zip_last4": "place_of_perform_zip_last4",
            "zip5": "place_of_performance_zip5",
        }

        legal_entity_location_field_map = {
            "location_country_code": "legal_entity_country_code",
            "country_name": "legal_entity_country_name",
            "state_code": "legal_entity_state_code",
            "state_name": "legal_entity_state_descrip",
            "city_name": "legal_entity_city_name",
            "county_name": "legal_entity_county_name",
            "county_code": "legal_entity_county_code",
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "zip4": "legal_entity_zip4",
            "congressional_code": "legal_entity_congressional",
            "zip_last4": "legal_entity_zip_last4",
            "zip5": "legal_entity_zip5",
        }

        for index, row in enumerate(to_insert, 1):
            upper_case_dict_values(row)

            # Create new LegalEntityLocation and LegalEntity from the row data
            legal_entity_location = create_location(
                legal_entity_location_field_map, row, {"recipient_flag": True, "is_fpds": True}
            )
            recipient_name = row["awardee_or_recipient_legal"]
            legal_entity = LegalEntity.objects.create(
                recipient_unique_id=row["awardee_or_recipient_uniqu"],
                recipient_name=recipient_name if recipient_name is not None else "",
            )
            legal_entity_value_map = {
                "location": legal_entity_location,
                "business_categories": get_business_categories(row=row, data_type="fpds"),
                "is_fpds": True,
            }
            set_legal_entity_boolean_fields(row)
            legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

            # Create the place of performance location
            pop_location = create_location(place_of_performance_field_map, row, {"place_of_performance_flag": True})

            # Find the toptier awards from the subtier awards
            awarding_agency = Agency.get_by_subtier_only(row["awarding_sub_tier_agency_c"])
            funding_agency = Agency.get_by_subtier_only(row["funding_sub_tier_agency_co"])

            # Generate the unique Award ID
            # "CONT_AW_" + agency_id + referenced_idv_agency_iden + piid + parent_award_id
            generated_unique_id = (
                "CONT_AW_"
                + (row["agency_id"] if row["agency_id"] else "-NONE-")
                + "_"
                + (row["referenced_idv_agency_iden"] if row["referenced_idv_agency_iden"] else "-NONE-")
                + "_"
                + (row["piid"] if row["piid"] else "-NONE-")
                + "_"
                + (row["parent_award_id"] if row["parent_award_id"] else "-NONE-")
            )

            # Create the summary Award
            (created, award) = Award.get_or_create_summary_award(
                generated_unique_award_id=generated_unique_id, piid=row["piid"]
            )
            award.parent_award_piid = row.get("parent_award_id")
            award.save()

            # Append row to list of Awards updated
            AWARD_UPDATE_ID_LIST.append(award.id)

            if row["last_modified"] and len(str(row["last_modified"])) == len("YYYY-MM-DD HH:MM:SS"):  # 19 characters
                dt_fmt = "%Y-%m-%d %H:%M:%S"
            else:
                dt_fmt = "%Y-%m-%d %H:%M:%S.%f"  # try using this even if last_modified isn't a valid string

            try:
                last_mod_date = datetime.strptime(str(row["last_modified"]), dt_fmt).date()
            except ValueError:  # handle odd-string formats and NULLs from the upstream FPDS-NG system
                info_message = "Invalid value '{}' does not match: '{}'".format(row["last_modified"], dt_fmt)
                logger.info(info_message)
                last_mod_date = None

            award_type, award_type_desc = award_types(row)

            parent_txn_value_map = {
                "award": award,
                "awarding_agency": awarding_agency,
                "funding_agency": funding_agency,
                "recipient": legal_entity,
                "place_of_performance": pop_location,
                "period_of_performance_start_date": format_date(row["period_of_performance_star"]),
                "period_of_performance_current_end_date": format_date(row["period_of_performance_curr"]),
                "action_date": format_date(row["action_date"]),
                "last_modified_date": last_mod_date,
                "transaction_unique_id": row["detached_award_proc_unique"],
                "generated_unique_award_id": generated_unique_id,
                "is_fpds": True,
                "type": award_type,
                "type_description": award_type_desc,
            }

            contract_field_map = {"description": "award_description"}

            transaction_normalized_dict = load_data_into_model(
                TransactionNormalized(),  # thrown away
                row,
                field_map=contract_field_map,
                value_map=parent_txn_value_map,
                as_dict=True,
            )

            contract_instance = load_data_into_model(TransactionFPDS(), row, as_dict=True)  # thrown away

            detached_award_proc_unique = contract_instance["detached_award_proc_unique"]
            unique_fpds = TransactionFPDS.objects.filter(detached_award_proc_unique=detached_award_proc_unique)

            if unique_fpds.first():
                transaction_normalized_dict["update_date"] = datetime.now(timezone.utc)
                transaction_normalized_dict["fiscal_year"] = fy(transaction_normalized_dict["action_date"])

                # update TransactionNormalized
                TransactionNormalized.objects.filter(id=unique_fpds.first().transaction.id).update(
                    **transaction_normalized_dict
                )

                # update TransactionFPDS
                unique_fpds.update(**contract_instance)
            else:
                # create TransactionNormalized
                transaction = TransactionNormalized(**transaction_normalized_dict)
                transaction.save()

                # create TransactionFPDS
                transaction_fpds = TransactionFPDS(transaction=transaction, **contract_instance)
                transaction_fpds.save()

            # Update legal entity to map back to transaction
            legal_entity.transaction_unique_id = detached_award_proc_unique
            legal_entity.save()

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            dest="date",
            nargs="+",
            type=str,
            help="(OPTIONAL) Date from which to start the nightly loader. Expected format: YYYY-MM-DD",
        )

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info("==== Starting FPDS nightly data load ====")

        if options.get("date"):
            date = options.get("date")[0]
            date = datetime.strptime(date, "%Y-%m-%d").date()
        else:
            default_last_load_date = datetime.now(timezone.utc) - timedelta(days=1)
            date = get_last_load_date("fpds", default=default_last_load_date).date()
        processing_start_datetime = datetime.now(timezone.utc)

        logger.info("Processing data for FPDS starting from %s" % date)

        with timer("retrieval of deleted FPDS IDs", logger.info):
            ids_to_delete = self.get_deleted_fpds_data_from_s3(date=date)

        if len(ids_to_delete) > 0:
            with timer("deletion of all stale FPDS data", logger.info):
                self.delete_stale_fpds(ids_to_delete=ids_to_delete)
        else:
            logger.info("No FPDS records to delete at this juncture")

        with timer("retrieval of new/modified FPDS data ID list", logger.info):
            total_insert = self.get_fpds_transaction_ids(date=date)

        if len(total_insert) > 0:
            # Add FPDS records
            with timer("insertion of new FPDS data in batches", logger.info):
                self.insert_all_new_fpds(total_insert)

            # Update Awards based on changed FPDS records
            with timer("updating awards to reflect their latest associated transaction info", logger.info):
                update_awards(tuple(AWARD_UPDATE_ID_LIST))

            # Update FPDS-specific Awards based on the info in child transactions
            with timer("updating contract-specific awards to reflect their latest transaction info", logger.info):
                update_contract_awards(tuple(AWARD_UPDATE_ID_LIST))

            # Update AwardCategories based on changed FPDS records
            with timer("updating award category variables", logger.info):
                update_award_categories(tuple(AWARD_UPDATE_ID_LIST))

            # Check the linkages from file C to FPDS records and update any that are missing
            with timer("updating C->D linkages", logger.info):
                update_c_to_d_linkages("contract")
        else:
            logger.info("No FPDS records to insert or modify at this juncture")

        # Update the date for the last time the data load was run
        update_last_load_date("fpds", processing_start_datetime)

        logger.info("FPDS NIGHTLY UPDATE COMPLETE")
