import logging

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger('console')


class Command(BaseCommand):
    def handle(self, *args, **options):
        with connection.cursor() as db_cursor:

            logger.info("Starting uppercase fixes")

            for table, sql_string in UPPERCASE_UPDATES.items():
                logger.info("Updating {} table columns".format(table))
                db_cursor.execute(sql_string)

            logger.info("Completed uppercase fixes")


UPPERCASE_UPDATES = {
    'transaction_fabs': """
        UPDATE transaction_fabs
        SET award_description = UPPER(award_description),
            awardee_or_recipient_legal = UPPER(awardee_or_recipient_legal),
            business_types = UPPER(business_types),
            fain = UPPER(fain),
            legal_entity_foreign_city = UPPER(legal_entity_foreign_city),
            legal_entity_foreign_posta = UPPER(legal_entity_foreign_posta),
            legal_entity_foreign_provi = UPPER(legal_entity_foreign_provi),
            place_of_performance_forei = UPPER(place_of_performance_forei),
            place_of_performance_zip4a = UPPER(place_of_performance_zip4a),
            sai_number = UPPER(sai_number),
            cfda_title = UPPER(cfda_title),
            awarding_agency_name = UPPER(awarding_agency_name),
            awarding_sub_tier_agency_n = UPPER(awarding_sub_tier_agency_n),
            funding_agency_name = UPPER(funding_agency_name),
            funding_sub_tier_agency_na = UPPER(funding_sub_tier_agency_na),
            afa_generated_unique = UPPER(afa_generated_unique),
            place_of_performance_zip5 = UPPER(place_of_performance_zip5),
            place_of_perform_zip_last4 = UPPER(place_of_perform_zip_last4)
        WHERE created_at >= '2018-01-29'
            AND created_at <= '2018-02-14'
            AND (UPPER(award_description) != award_description
                OR UPPER(awardee_or_recipient_legal) != awardee_or_recipient_legal
                OR UPPER(business_types) != business_types
                OR UPPER(fain) != fain
                OR UPPER(legal_entity_foreign_city) != legal_entity_foreign_city
                OR UPPER(legal_entity_foreign_posta) != legal_entity_foreign_posta
                OR UPPER(legal_entity_foreign_provi) != legal_entity_foreign_provi
                OR UPPER(place_of_performance_forei) != place_of_performance_forei
                OR UPPER(place_of_performance_zip4a) != place_of_performance_zip4a
                OR UPPER(sai_number) != sai_number
                OR UPPER(cfda_title) != cfda_title
                OR UPPER(awarding_agency_name) != awarding_agency_name
                OR UPPER(awarding_sub_tier_agency_n) != awarding_sub_tier_agency_n
                OR UPPER(funding_agency_name) != funding_agency_name
                OR UPPER(funding_sub_tier_agency_na) != funding_sub_tier_agency_na
                OR UPPER(afa_generated_unique) != afa_generated_unique
                OR UPPER(place_of_performance_zip5) != place_of_performance_zip5
                OR UPPER(place_of_perform_zip_last4) != place_of_perform_zip_last4);
        """,
    'transaction_fpds': """
        UPDATE transaction_fpds
        SET detached_award_proc_unique = UPPER(detached_award_proc_unique),
            awarding_agency_name = UPPER(awarding_agency_name),
            ultimate_parent_legal_enti = UPPER(ultimate_parent_legal_enti),
            place_of_performance_zip4a = UPPER(place_of_performance_zip4a),
            awardee_or_recipient_legal = UPPER(awardee_or_recipient_legal),
            funding_office_name = UPPER(funding_office_name),
            awarding_office_name = UPPER(awarding_office_name),
            funding_agency_name = UPPER(funding_agency_name),
            vendor_fax_number = UPPER(vendor_fax_number),
            cost_or_pricing_data_desc = UPPER(cost_or_pricing_data_desc),
            government_furnished_desc = UPPER(government_furnished_desc),
            major_program = UPPER(major_program),
            product_or_service_co_desc = UPPER(product_or_service_co_desc),
            program_acronym = UPPER(program_acronym),
            pulled_from = UPPER(pulled_from)
        WHERE updated_at >= '2018-01-29'
            AND updated_at <= '2018-02-14'
            AND (UPPER(detached_award_proc_unique) != detached_award_proc_unique
                OR UPPER(awarding_agency_name) != awarding_agency_name
                OR UPPER(ultimate_parent_legal_enti) != ultimate_parent_legal_enti
                OR UPPER(place_of_performance_zip4a) != place_of_performance_zip4a
                OR UPPER(awardee_or_recipient_legal) != awardee_or_recipient_legal
                OR UPPER(funding_office_name) != funding_office_name
                OR UPPER(awarding_office_name) != awarding_office_name
                OR UPPER(funding_agency_name) != funding_agency_name
                OR UPPER(vendor_fax_number) != vendor_fax_number
                OR UPPER(cost_or_pricing_data_desc) != cost_or_pricing_data_desc
                OR UPPER(government_furnished_desc) != government_furnished_desc
                OR UPPER(major_program) != major_program
                OR UPPER(product_or_service_co_desc) != product_or_service_co_desc
                OR UPPER(program_acronym) != program_acronym
                OR UPPER(pulled_from) != pulled_from);
        """,
    'transaction_normalized': """
        UPDATE transaction_normalized
        SET transaction_unique_id = UPPER(transaction_unique_id),
            description = UPPER(description)
        WHERE update_date > '2018-02-02'
            AND update_date <= '2018-02-15'
            AND (UPPER(transaction_unique_id) != transaction_unique_id
                OR UPPER(description) != description);
        """,
    'references_location': """
        UPDATE references_location
        SET transaction_unique_id = UPPER(transaction_unique_id),
            foreign_city_name = UPPER(foreign_city_name),
            foreign_postal_code = UPPER(foreign_postal_code),
            foreign_province = UPPER(foreign_province),
            foreign_location_description = UPPER(foreign_location_description),
            zip5 = UPPER(zip5),
            zip_last4 = UPPER(zip_last4),
            zip_4a = UPPER(zip_4a),
            city_name = UPPER(city_name),
            address_line1 = UPPER(address_line1),
            congressional_code = UPPER(congressional_code),
            zip4 = UPPER(zip4)
        WHERE create_date >= '2018-02-08'
            AND create_date <= '2018-02-23'
            AND (UPPER(transaction_unique_id) != transaction_unique_id
                OR UPPER(foreign_city_name) != foreign_city_name
                OR UPPER(foreign_postal_code) != foreign_postal_code
                OR UPPER(foreign_province) != foreign_province
                OR UPPER(foreign_location_description) != foreign_location_description
                OR UPPER(zip5) != zip5
                OR UPPER(zip_last4) != zip_last4
                OR UPPER(zip_4a) != zip_4a
                OR UPPER(city_name) != city_name
                OR UPPER(address_line1) != address_line1
                OR UPPER(congressional_code) != congressional_code
                OR UPPER(zip4) != zip4);
        """,
    'legal_entity': """
        UPDATE legal_entity
        SET transaction_unique_id = UPPER(transaction_unique_id),
            recipient_name = UPPER(recipient_name),
            business_types = UPPER(business_types),
            vendor_fax_number = UPPER(vendor_fax_number)
        WHERE create_date >= '2018-02-08'
            AND create_date <= '2018-02-23'
            AND (UPPER(transaction_unique_id) != transaction_unique_id
                OR UPPER(recipient_name) != recipient_name
                OR UPPER(business_types) != business_types
                OR UPPER(vendor_fax_number) != vendor_fax_number);
        """,
    'awards': """
        UPDATE awards
        SET transaction_unique_id = UPPER(transaction_unique_id),
            generated_unique_award_id = UPPER(generated_unique_award_id),
            parent_award_piid = UPPER(parent_award_piid),
            piid = UPPER(piid),
            uri = UPPER(uri),
            fain = UPPER(fain),
            description = UPPER(description)
        WHERE update_date >= '2018-02-04'
            AND update_date <= '2018-02-23'
            AND (UPPER(transaction_unique_id) != transaction_unique_id
                OR UPPER(generated_unique_award_id) != generated_unique_award_id
                OR UPPER(parent_award_piid) != parent_award_piid
                OR UPPER(piid) != piid
                OR UPPER(uri) != uri
                OR UPPER(fain) != fain
                OR UPPER(description) != description);
        """}
