"""
Jira Ticket Number(s): DEV-3753

    Move business_categories from legal_entity to transaction_normalized.

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/backfill_business_categories.py

Purpose:

    Recalculate business categories for all transactions.  We COULD just copy them from legal_entity,
    but I'd sleep better at night knowing we were getting a fresh start even though it'll take
    longer to update them this way.

Life expectancy:

    Once Sprint 95 has been rolled out to production this script is safe to delete... although I
    would recommend keeping it around for a few additional sprints for reference.

"""
import logging
import psycopg2

from os import environ
from pathlib import Path


# Import our USAspending Timer component.  This will not work if we ever add
# any Django specific stuff to the timing_helpers.py file.
exec(Path("usaspending_api/common/helpers/timing_helpers.py").read_text())


CONNECTION_STRING = environ["DATABASE_URL"]
CHUNK_SIZE = 2000000

# Modified from usaspending_api/broker/management/sql/update_business_categories.sql
UPDATE_SQL = """
    update
        transaction_normalized as tnu
    set
        business_categories = case
            when fpds.transaction_id is not null then
                compile_fpds_business_categories(
                    fpds.contracting_officers_deter,
                    fpds.corporate_entity_tax_exemp,
                    fpds.corporate_entity_not_tax_e,
                    fpds.partnership_or_limited_lia,
                    fpds.sole_proprietorship,
                    fpds.manufacturer_of_goods,
                    fpds.subchapter_s_corporation,
                    fpds.limited_liability_corporat,
                    fpds.for_profit_organization,
                    fpds.alaskan_native_owned_corpo,
                    fpds.american_indian_owned_busi,
                    fpds.asian_pacific_american_own,
                    fpds.black_american_owned_busin,
                    fpds.hispanic_american_owned_bu,
                    fpds.native_american_owned_busi,
                    fpds.native_hawaiian_owned_busi,
                    fpds.subcontinent_asian_asian_i,
                    fpds.tribally_owned_business,
                    fpds.other_minority_owned_busin,
                    fpds.minority_owned_business,
                    fpds.women_owned_small_business,
                    fpds.economically_disadvantaged,
                    fpds.joint_venture_women_owned,
                    fpds.joint_venture_economically,
                    fpds.woman_owned_business,
                    fpds.service_disabled_veteran_o,
                    fpds.veteran_owned_business,
                    fpds.c8a_program_participant,
                    fpds.the_ability_one_program,
                    fpds.dot_certified_disadvantage,
                    fpds.emerging_small_business,
                    fpds.federally_funded_research,
                    fpds.historically_underutilized,
                    fpds.labor_surplus_area_firm,
                    fpds.sba_certified_8_a_joint_ve,
                    fpds.self_certified_small_disad,
                    fpds.small_agricultural_coopera,
                    fpds.small_disadvantaged_busine,
                    fpds.community_developed_corpor,
                    fpds.domestic_or_foreign_entity,
                    fpds.foreign_owned_and_located,
                    fpds.foreign_government,
                    fpds.international_organization,
                    fpds.domestic_shelter,
                    fpds.hospital_flag,
                    fpds.veterinary_hospital,
                    fpds.foundation,
                    fpds.community_development_corp,
                    fpds.nonprofit_organization,
                    fpds.educational_institution,
                    fpds.other_not_for_profit_organ,
                    fpds.state_controlled_instituti,
                    fpds.c1862_land_grant_college,
                    fpds.c1890_land_grant_college,
                    fpds.c1994_land_grant_college,
                    fpds.private_university_or_coll,
                    fpds.minority_institution,
                    fpds.historically_black_college,
                    fpds.tribal_college,
                    fpds.alaskan_native_servicing_i,
                    fpds.native_hawaiian_servicing,
                    fpds.hispanic_servicing_institu,
                    fpds.school_of_forestry,
                    fpds.veterinary_college,
                    fpds.us_federal_government,
                    fpds.federal_agency,
                    fpds.us_government_entity,
                    fpds.interstate_entity,
                    fpds.us_state_government,
                    fpds.council_of_governments,
                    fpds.city_local_government,
                    fpds.county_local_government,
                    fpds.inter_municipal_local_gove,
                    fpds.municipality_local_governm,
                    fpds.township_local_government,
                    fpds.us_local_government,
                    fpds.local_government_owned,
                    fpds.school_district_local_gove,
                    fpds.us_tribal_government,
                    fpds.indian_tribe_federally_rec,
                    fpds.housing_authorities_public,
                    fpds.airport_authority,
                    fpds.port_authority,
                    fpds.transit_authority,
                    fpds.planning_commission
                )
            else
                compile_fabs_business_categories(
                    fabs.business_types
                )
            end
    from
        transaction_normalized as tn
        left outer join transaction_fpds as fpds on fpds.transaction_id = tn.id and tn.is_fpds is true
        left outer join transaction_fabs as fabs on fabs.transaction_id = tn.id and tn.is_fpds is false
    where
        tnu.id between %s and %s and
        (tnu.business_categories is null or tnu.business_categories = '{}'::text[]) and
        tn.id = tnu.id
"""

logging.basicConfig(
    level=logging.DEBUG, format="[%(asctime)s] [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S %Z"
)


# Simplify instantiations of Timer to automatically use the correct logger.
class Timer(Timer):  # noqa - because we're using trickery to import this
    def __init__(self, message=None):
        super().__init__(message=message, success_logger=logging.info, failure_logger=logging.error)


def execute(message, sql, fetcher=None):
    with Timer(message):
        with connection.cursor() as cursor:
            cursor.execute(sql)
            if cursor.rowcount > -1:
                logging.info("{:,} rows affected".format(cursor.rowcount))
            if fetcher is not None:
                return fetcher(cursor)
        return cursor


def get_min_and_max_transaction_ids():
    def fetcher(cursor):
        min_id, max_id = cursor.fetchall()[0]
        logging.info("Found transaction min/max ids: {} / {}".format(min_id, max_id))
        return min_id, max_id

    sql = "select min(id), max(id) from transaction_normalized"
    return execute("Get min/max transaction ids", sql, fetcher=fetcher)


def update_sql_functions():
    sql = Path("usaspending_api/broker/management/sql/create_business_categories_functions.sql").read_text()
    execute("Update SQL functions", sql)


def disable_autovacuum():
    sql = "alter table transaction_normalized set (autovacuum_enabled = false, toast.autovacuum_enabled = false)"
    execute("Disable autovacuum", sql)


def enable_autovacuum():
    sql = "alter table transaction_normalized set (autovacuum_enabled = true, toast.autovacuum_enabled = true)"
    execute("Enable autovacuum", sql)


def id_ranges():
    for n in range(min_id, max_id + 1, CHUNK_SIZE + 1):
        yield n, min(n + CHUNK_SIZE, max_id)


def process_chunk():
    sql = UPDATE_SQL % (chunk_min_id, chunk_max_id)
    execute("Update transactions from {} through {}".format(chunk_min_id, chunk_max_id), sql)
    ratio = (chunk_max_id - min_id + 1) / (max_id - min_id + 1)
    logging.info(
        "Estimated remaining runtime: {}".format(overall.as_string(overall.estimated_remaining_runtime(ratio)))
    )


with Timer("Overall run") as overall:
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True
        update_sql_functions()
        disable_autovacuum()
        try:
            min_id, max_id = get_min_and_max_transaction_ids()
            for chunk_min_id, chunk_max_id in id_ranges():
                process_chunk()
        finally:
            enable_autovacuum()
