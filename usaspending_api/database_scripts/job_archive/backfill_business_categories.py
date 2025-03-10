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

import asyncio
import asyncpg
import logging

from os import environ
from pathlib import Path


# Import our USAspending Timer component.  This will not work if we ever add
# any Django specific stuff to the timing_helpers.py file.
exec(Path("usaspending_api/common/helpers/timing_helpers.py").read_text())


CONNECTION_STRING = environ["DATABASE_URL"]
CHUNK_SIZE = 500000
PIPELINES = 5
END_OF_QUEUE = (None, None)

# Modified from usaspending_api/broker/management/sql/update_business_categories.sql
SQL = """
    drop table if exists temp_dev_3753_business_categories;


    create temporary table
        temp_dev_3753_business_categories (
            transaction_id bigint not null,
            business_categories text[] not null
        );


    insert into
        temp_dev_3753_business_categories (
            transaction_id,
            business_categories
        )
    select
        tn.id,
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
    from
        transaction_normalized as tn
        inner join transaction_fpds as fpds on
            fpds.transaction_id = tn.id and
            tn.is_fpds is true
    where
        tn.id between {min_id} and {max_id};


    insert into
        temp_dev_3753_business_categories (
            transaction_id,
            business_categories
        )
    select
        tn.id,
        compile_fabs_business_categories(
            fabs.business_types
        )
    from
        transaction_normalized as tn
        inner join transaction_fabs as fabs on
            fabs.transaction_id = tn.id and
            tn.is_fpds is false
    where
        tn.id between {min_id} and {max_id};


    alter table temp_dev_3753_business_categories add primary key (transaction_id);


    update  subaward as s
    set     business_categories = t.business_categories
    from    vw_awards as a
            inner join temp_dev_3753_business_categories as t on t.transaction_id = a.latest_transaction_id
    where   s.award_id = a.id and
            s.business_categories is distinct from t.business_categories;


    update  transaction_normalized as tn
    set     business_categories = t.business_categories
    from    temp_dev_3753_business_categories as t
    where   tn.id = t.transaction_id and
            tn.business_categories is distinct from t.business_categories;
"""


logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S %Z"
)


# Simplify instantiations of Timer to automatically use the correct logger.
class Timer(Timer):  # noqa - because we're using trickery to import this
    def __init__(self, message=None):
        super().__init__(message=message, success_logger=logging.info, failure_logger=logging.error)


async def update_sql_functions(pool):
    with Timer() as t:
        sql = Path("usaspending_api/broker/management/sql/create_business_categories_functions.sql").read_text()
        async with pool.acquire() as connection:
            await connection.execute(sql)
    logging.info("Updated SQL functions in {}".format(t))


def id_ranges(min_id, max_id):
    for n in range(min_id, max_id + 1, CHUNK_SIZE + 1):
        yield n, min(n + CHUNK_SIZE, max_id)


async def get_min_max_ids(pool):
    with Timer() as t:
        sql = "select min(id), max(id) from transaction_normalized"
        async with pool.acquire() as connection:
            min_id, max_id = await connection.fetchrow(sql)
    logging.info("Found min transaction id = {} and max transaction id = {} in {}".format(min_id, max_id, t))
    return min_id, max_id


async def create_chunk_queue(min_id, max_id, loop):
    queue = asyncio.Queue(loop=loop)
    for min_id, max_id in id_ranges(min_id, max_id):
        await queue.put((min_id, max_id))

    # Effectively a sentinel value to let chunk processors know they've reached the end of the queue.
    await queue.put(END_OF_QUEUE)

    return queue


async def process_queue(queue, original_queue_size, processed_queue, pool):
    while True:

        min_id, max_id = await queue.get()
        if min_id is None:

            # Need to add the sentinel value back to the queue so other pipelines can pick it up
            # else they'll wait forever for nothing.
            await queue.put(END_OF_QUEUE)
            break

        with Timer() as t:
            sql = SQL.format(min_id=min_id, max_id=max_id)
            async with pool.acquire() as connection:
                result = await connection.execute(sql)

        await processed_queue.put((min_id, max_id))

        affected = int(result.split()[-1])

        ratio = processed_queue.qsize() / original_queue_size
        estimated_remaining_runtime = overall.as_string(overall.estimated_remaining_runtime(ratio))
        message = "Updated {:,} transactions from {} through {} in {} - estimated remaining runtime is {}".format(
            affected, min_id, max_id, t, estimated_remaining_runtime
        )
        logging.info(message)


async def run_pipelines(queue, original_queue_size, processed_queue, loop, pool):
    tasks = [
        asyncio.ensure_future(process_queue(queue, original_queue_size, processed_queue, pool), loop=loop)
        for _ in range(PIPELINES)
    ]
    await asyncio.gather(*tasks)


async def main(loop):
    async with asyncpg.create_pool(dsn=CONNECTION_STRING, loop=loop) as pool:
        await update_sql_functions(pool)
        min_id, max_id = await get_min_max_ids(pool)
        to_process_queue = await create_chunk_queue(min_id, max_id, loop)
        processed_queue = asyncio.Queue(loop=loop)
        await run_pipelines(to_process_queue, to_process_queue.qsize() - 1, processed_queue, loop, pool)


if __name__ == "__main__":
    with Timer("Overall run") as overall:
        _loop = asyncio.get_event_loop()
        try:
            _loop.run_until_complete(main(_loop))
        finally:
            _loop.close()
