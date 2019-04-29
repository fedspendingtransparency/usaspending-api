import logging
import time

from copy import copy
from datetime import datetime, timezone
from django.db import connections, transaction

from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award
from usaspending_api.broker.helpers.get_business_categories import get_business_categories
from usaspending_api.common.helpers.date_helper import cast_datetime_to_utc
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values
from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.common.helpers.generic_helper import fy, timer
from usaspending_api.etl.award_helpers import update_awards, update_award_categories
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, create_location
from usaspending_api.references.models import LegalEntity, Agency


logger = logging.getLogger("console")

BATCH_FETCH_SIZE = 25000


def fetch_fabs_data_generator(dap_uid_list):
    db_cursor = connections["data_broker"].cursor()
    db_query = """
        SELECT * FROM published_award_financial_assistance
        WHERE published_award_financial_assistance_id IN %s;
    """

    total_uid_count = len(dap_uid_list)

    for i in range(0, total_uid_count, BATCH_FETCH_SIZE):
        start_time = time.perf_counter()
        max_index = i + BATCH_FETCH_SIZE if i + BATCH_FETCH_SIZE < total_uid_count else total_uid_count
        fabs_ids_batch = dap_uid_list[i:max_index]

        log_msg = "Fetching {}-{} out of {} records from broker"
        logger.info(log_msg.format(i + 1, max_index, total_uid_count))

        db_cursor.execute(db_query, [tuple(fabs_ids_batch)])
        logger.info("Fetching records took {:.2f}s".format(time.perf_counter() - start_time))
        yield dictfetchall(db_cursor)


@transaction.atomic
def insert_all_new_fabs(all_new_to_insert):
    update_award_ids = []
    for to_insert in fetch_fabs_data_generator(all_new_to_insert):
        start = time.perf_counter()
        update_award_ids.extend(insert_new_fabs(to_insert))
        logger.info("FABS insertions took {:.2f}s".format(time.perf_counter() - start))
    return update_award_ids


def insert_new_fabs(to_insert):
    place_of_performance_field_map = {
        "location_country_code": "place_of_perform_country_c",
        "country_name": "place_of_perform_country_n",
        "state_code": "place_of_perfor_state_code",
        "state_name": "place_of_perform_state_nam",
        "city_name": "place_of_performance_city",
        "county_name": "place_of_perform_county_na",
        "county_code": "place_of_perform_county_co",
        "foreign_location_description": "place_of_performance_forei",
        "zip_4a": "place_of_performance_zip4a",
        "congressional_code": "place_of_performance_congr",
        "performance_code": "place_of_performance_code",
        "zip_last4": "place_of_perform_zip_last4",
        "zip5": "place_of_performance_zip5",
    }

    legal_entity_location_field_map = {
        "location_country_code": "legal_entity_country_code",
        "country_name": "legal_entity_country_name",
        "state_code": "legal_entity_state_code",
        "state_name": "legal_entity_state_name",
        "city_name": "legal_entity_city_name",
        "city_code": "legal_entity_city_code",
        "county_name": "legal_entity_county_name",
        "county_code": "legal_entity_county_code",
        "address_line1": "legal_entity_address_line1",
        "address_line2": "legal_entity_address_line2",
        "address_line3": "legal_entity_address_line3",
        "foreign_location_description": "legal_entity_foreign_descr",
        "congressional_code": "legal_entity_congressional",
        "zip_last4": "legal_entity_zip_last4",
        "zip5": "legal_entity_zip5",
        "foreign_postal_code": "legal_entity_foreign_posta",
        "foreign_province": "legal_entity_foreign_provi",
        "foreign_city_name": "legal_entity_foreign_city",
    }

    update_award_ids = []
    for row in to_insert:
        upper_case_dict_values(row)

        # Create new LegalEntityLocation and LegalEntity from the row data
        legal_entity_location = create_location(legal_entity_location_field_map, row, {"recipient_flag": True})
        recipient_name = row['awardee_or_recipient_legal']
        legal_entity = LegalEntity.objects.create(
            recipient_unique_id=row['awardee_or_recipient_uniqu'],
            recipient_name=recipient_name if recipient_name is not None else "",
            parent_recipient_unique_id=row['ultimate_parent_unique_ide'],
        )
        legal_entity_value_map = {
            "location": legal_entity_location,
            "business_categories": get_business_categories(row=row, data_type='fabs'),
            "business_types_description": row['business_types_desc'],
        }
        legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

        # Create the place of performance location
        pop_location = create_location(place_of_performance_field_map, row, {"place_of_performance_flag": True})

        # Find the toptier awards from the subtier awards
        awarding_agency = Agency.get_by_subtier_only(row["awarding_sub_tier_agency_c"])
        funding_agency = Agency.get_by_subtier_only(row["funding_sub_tier_agency_co"])

        # Generate the unique Award ID
        # "ASST_AW_" + awarding_sub_tier_agency_c + fain + uri

        # this will raise an exception if the cast to an int fails, that's ok since we don't want to process
        # non-numeric record type values
        record_type_int = int(row['record_type'])
        if record_type_int == 1:
            uri = row['uri'] if row['uri'] else '-NONE-'
            fain = '-NONE-'
        elif record_type_int in (2, 3):
            uri = '-NONE-'
            fain = row['fain'] if row['fain'] else '-NONE-'
        else:
            msg = "Invalid record type encountered for the following afa_generated_unique record: {}"
            raise Exception(msg.format(row['afa_generated_unique']))

        astac = row["awarding_sub_tier_agency_c"] if row["awarding_sub_tier_agency_c"] else "-NONE-"
        generated_unique_id = "ASST_AW_{}_{}_{}".format(astac, fain, uri)

        # Create the summary Award
        (created, award) = Award.get_or_create_summary_award(
            generated_unique_award_id=generated_unique_id,
            fain=row['fain'],
            uri=row['uri'],
            record_type=row['record_type'],
        )
        award.save()

        # Append row to list of Awards updated
        update_award_ids.append(award.id)

        try:
            last_mod_date = datetime.strptime(str(row['modified_at']), "%Y-%m-%d %H:%M:%S.%f").date()
        except ValueError:
            last_mod_date = datetime.strptime(str(row['modified_at']), "%Y-%m-%d %H:%M:%S").date()
        parent_txn_value_map = {
            "award": award,
            "awarding_agency": awarding_agency,
            "funding_agency": funding_agency,
            "recipient": legal_entity,
            "place_of_performance": pop_location,
            "period_of_performance_start_date": format_date(row['period_of_performance_star']),
            "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
            "action_date": format_date(row['action_date']),
            "last_modified_date": last_mod_date,
            "type_description": row['assistance_type_desc'],
            "transaction_unique_id": row['afa_generated_unique'],
            "generated_unique_award_id": generated_unique_id,
        }

        fad_field_map = {
            "type": "assistance_type",
            "description": "award_description",
            "funding_amount": "total_funding_amount",
        }

        transaction_normalized_dict = load_data_into_model(
            TransactionNormalized(),  # thrown away
            row,
            field_map=fad_field_map,
            value_map=parent_txn_value_map,
            as_dict=True,
        )

        financial_assistance_data = load_data_into_model(TransactionFABS(), row, as_dict=True)  # thrown away

        # Hack to cut back on the number of warnings dumped to the log.
        financial_assistance_data['updated_at'] = cast_datetime_to_utc(financial_assistance_data['updated_at'])
        financial_assistance_data['created_at'] = cast_datetime_to_utc(financial_assistance_data['created_at'])
        financial_assistance_data['modified_at'] = cast_datetime_to_utc(financial_assistance_data['modified_at'])

        afa_generated_unique = financial_assistance_data['afa_generated_unique']
        unique_fabs = TransactionFABS.objects.filter(afa_generated_unique=afa_generated_unique)

        if unique_fabs.first():
            transaction_normalized_dict["update_date"] = datetime.now(timezone.utc)
            transaction_normalized_dict["fiscal_year"] = fy(transaction_normalized_dict["action_date"])

            # Update TransactionNormalized
            TransactionNormalized.objects.filter(id=unique_fabs.first().transaction.id).update(
                **transaction_normalized_dict
            )

            # Update TransactionFABS
            unique_fabs.update(**financial_assistance_data)
        else:
            # Create TransactionNormalized
            transaction_normalized = TransactionNormalized(**transaction_normalized_dict)
            transaction_normalized.save()

            # Create TransactionFABS
            transaction_fabs = TransactionFABS(transaction=transaction_normalized, **financial_assistance_data)
            transaction_fabs.save()

        # Update legal entity to map back to transaction
        legal_entity.transaction_unique_id = afa_generated_unique
        legal_entity.save()

    return update_award_ids


def upsert_fabs_transactions(ids_to_upsert, externally_updated_award_ids):
    if ids_to_upsert or externally_updated_award_ids:
        update_award_ids = copy(externally_updated_award_ids)

        if ids_to_upsert:
            with timer("inserting new FABS data", logger.info):
                update_award_ids.extend(insert_all_new_fabs(ids_to_upsert))

        if update_award_ids:
            update_award_ids = tuple(set(update_award_ids))  # Convert to tuple and remove duplicates.
            with timer("updating awards to reflect their latest associated transaction info", logger.info):
                update_awards(update_award_ids)
            with timer("updating award category variables", logger.info):
                update_award_categories(update_award_ids)

        with timer("updating C->D linkages", logger.info):
            update_c_to_d_linkages("assistance")

    else:
        logger.info("Nothing to insert...")
