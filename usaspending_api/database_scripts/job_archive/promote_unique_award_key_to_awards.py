"""
Jira Ticket Number(s): DEV-2504, DEV-2791

    Migrate Broker's unique_award_key to USAspending's awards.generated_unique_award_id.  The unique_award_key
    has already been migrated to the transaction_* tables in USAspending so this is basically just promoting
    it and cleaning up the ensuing mess.

Expected CLI:

    $ python usasepending_api/database_scripts/job_archive/promote_unique_award_key_to_awards.py

Process:

    STEP 1 - There are some awards that have no relations anywhere in
             the database.  Eliminate them.

    STEP 2 - Back up old generated_unique_award_ids.

    STEP 3 - Create a unique_award_key mapping.  This is for debugging
             purposes.  It's harder to figure out what went wrong if we update
             the awards table directly.

    STEP 4 - Replace generated_unique_award_id with unique_award_key

    STEP 5 - If everything went well, remove temp_dev2504_unique_award_key_mapping.

"""
import contextlib
import math
import psycopg2
import time

from os import environ


# DEFINE THIS ENVIRONMENT VARIABLE BEFORE RUNNING!
# Obvs, this is the connection string to the database.
CONNECTION_STRING = environ['DATABASE_URL']


def chunk_ids(min_id, max_id, chunk_size):
    _min = min_id
    while _min <= max_id:
        _max = min(_min + chunk_size - 1, max_id)
        yield(_min, _max, (_max - min_id + 1) / (max_id - min_id + 1))
        _min = _max + 1


def test_chunk_ids():
    """To shut up codeclimate"""
    assert next(chunk_ids(1, 100, 10)) == (1, 10,  0.1)


def format_elapsed(elapsed):
    f, s = math.modf(elapsed)
    ms = round(f * 1000)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)

    fmt = ['{:,} d', '{:,} h', '{:,} m', '{:,} s', '{:,} ms']
    bits = tuple(int(n) for n in (d, h, m, s, ms))

    return ' '.join(
        fmt[i].format(n)
        for i, n in enumerate(bits)
        if n > 0) or 'less than a millisecond'


def test_format_elapsed():
    """To shut up codeclimate"""
    assert format_elapsed(1.1) == "1 s 100 ms"


@contextlib.contextmanager
def timer(msg):
    """
    Borrowed from USAspending.  This is a standalone script so can't reference
    the USAspending version.
    """
    start = time.perf_counter()
    print('Starting {}...'.format(msg), flush=True)
    try:
        yield {}
    finally:
        elapsed = time.perf_counter() - start
        s = format_elapsed(elapsed)
        print('Finished {} in {}'.format(msg, s), flush=True)


def delete_orphaned_awards(cursor):
    """
    There are orphaned awards that we need to remove.  This function will find
    and excise them.  As of testing, there are 198 of these in production, all
    of which are IDVs.  We will impose a sanity limit of, say, 300 just to be
    safe.  If we find more than 300 or any that aren't IDVs, we'll stop execution.

    This query takes about 30 minutes on DEV and about 20 on PROD.
    """
    cursor.execute("""
        create table if not exists temp_dev2504_orphaned_awards (award_id bigint not null, type text)
    """)

    cursor.execute("""
        insert into temp_dev2504_orphaned_awards (award_id)
        select      a.id
        from        awards a
                    left outer join transaction_normalized tn on tn.award_id = a.id
                    left outer join subaward sa on sa.award_id = a.id
                    left outer join financial_accounts_by_awards faba on faba.award_id = a.id
        where       tn.id is null and
                    sa.id is null and
                    faba.financial_accounts_by_awards_id is null
    """)
    _rowcount = cursor.rowcount

    # Believe it or not, it's faster to update this after the fact than to
    # try to include "type" in the create table script above.  Probably
    # something to do with index only scans.
    cursor.execute("""
        update      temp_dev2504_orphaned_awards
        set         type = a.type
        from        awards a
        where       a.id = temp_dev2504_orphaned_awards.award_id
    """)
    if cursor.rowcount != _rowcount:
        raise RuntimeError(
            "Error updating temp_dev2504_orphaned_awards.type - rowcount "
            "mismatch {} in table vs {} types updated".format(_rowcount, cursor.rowcount)
        )

    if _rowcount > -1:
        print("{:,} orphans found".format(cursor.rowcount))
    else:
        cursor.execute("select count(*) from temp_dev2504_orphaned_awards")
        results = cursor.fetchall()
        _rowcount = results[0][0]
        print("Using existing table which contains {:,} orphans".format(_rowcount))

    if _rowcount >= 300:
        raise ValueError(
            "While running 'delete_orphaned_awards', found {:,} orphans to be "
            "deleted.  Expected less than 300.".format(_rowcount)
        )

    cursor.execute("select count(*) from temp_dev2504_orphaned_awards where type not like 'IDV%'")
    results = cursor.fetchall()
    if results[0][0] > 0:
        raise ValueError(
            "While running 'delete_orphaned_awards', found {:,} non-IDV.  "
            "Expected none.  Check temp_dev2504_orphaned_awards table for the "
            "list.".format(results[0][0])
        )

    # Ok.  We're good.  Delete these awards if there are any.
    # if _rowcount > 0:
    cursor.execute(
        'delete from parent_award where award_id in (select award_id from temp_dev2504_orphaned_awards)')
    print("{:,} parent_awards deleted".format(cursor.rowcount))
    cursor.execute('delete from awards where id in (select award_id from temp_dev2504_orphaned_awards)')
    print("{:,} awards deleted".format(cursor.rowcount))


def back_up_generated_unique_award_ids(cursor):
    """
    "Just in case", let's back up the current generated_unique_award_ids.
    78,456,037 rows affected in 4 m 31 s 834 ms
    """
    cursor.execute("""
        create table if not exists
            temp_dev2504_generated_unique_award_id_backup
        as select
            id award_id, generated_unique_award_id
        from
            awards
    """)
    if cursor.rowcount > -1:
        print("{:,} rows affected".format(cursor.rowcount))
    else:
        print("Table already exists")


def create_unique_award_key_mapping(cursor):
    """
    Create the mapping between award ids and unique_award_keys.
    78,455,846 rows affected in 17 m 18 s 10 ms
    5 m 1 s 900 ms
    """
    cursor.execute("""
        create table if not exists
            temp_dev2504_unique_award_key_mapping
        as select
            a.id award_id, tn.unique_award_key
        from
            awards a
            inner join transaction_normalized tn on
                tn.award_id = a.id and tn.id = a.latest_transaction_id
    """)
    if cursor.rowcount > -1:
        print("{:,} rows affected".format(cursor.rowcount))
        cursor.execute("""
            create unique index
                idx_temp_dev2504_unique_award_key_mapping
            on
                temp_dev2504_unique_award_key_mapping (award_id, unique_award_key)
        """)
    else:
        print("Table already exists")


def replace_generated_unique_award_id(cursor):
    """
    This performs the actual promotion.  It will fail if we are unable to
    find an award key for an award id which can happen if any award to
    transaction mappings are bad.  In testing, all occurrences of bad mappings
    were cleaned up during the orphaned award removal.
    """
    cursor.execute("select min(id), max(id) from awards")
    results = cursor.fetchall()
    min_id, max_id = results[0]

    total_count = 0

    overall_start = time.perf_counter()
    for _min, _max, _ratio in chunk_ids(min_id, max_id, 200000):
        start = time.perf_counter()
        cursor.execute("""
            update
                awards
            set
                generated_unique_award_id = t.unique_award_key
            from
                awards a
                left outer join temp_dev2504_unique_award_key_mapping t on
                    t.award_id = a.id
            where
                awards.id between {min} and {max} and
                a.id = awards.id;
        """.format(min=_min, max=_max))

        _row_count = cursor.rowcount
        if _row_count > 0:
            total_count += _row_count

        elapsed = time.perf_counter() - start
        overall_elapsed = time.perf_counter() - overall_start
        print("[{:.2%}]: {} => {}: {:,} promotions in {}: estimated remaining: {}".format(
            _ratio, _min, _max, _row_count, format_elapsed(elapsed),
            format_elapsed((1 - _ratio) * (overall_elapsed / _ratio))))

    print("total {:,} rows affected".format(total_count))


if __name__ == "__main__":

    with timer("unique_award_key promotion"):

        # Open up a connection for all of our stuff.  Just autocommit everything.
        # The database will never be in a broken state, so there's no need for a
        # ginormous single transaction.
        with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:

                # STEP 1 - There are some awards that have no relations anywhere in
                # the database.  Eliminate them.
                with timer("orphaned award cleanup"):
                    delete_orphaned_awards(cursor)

                # STEP 2 - Back up old generated_unique_award_ids.
                with timer("generated_unique_award_id backup"):
                    back_up_generated_unique_award_ids(cursor)

                # STEP 3 - Create a unique_award_key mapping.  This is for debugging
                # purposes.  It's harder to figure out what went wrong if we update
                # the awards table directly.
                with timer("unique_award_key mapping"):
                    create_unique_award_key_mapping(cursor)

                # STEP 4 - Replace generated_unique_award_id with unique_award_key
                with timer("replacing generated_unique_award_ids"):
                    replace_generated_unique_award_id(cursor)

                # STEP 5 - If everything went well, remove temp_dev2504_unique_award_key_mapping.
                cursor.execute("drop table temp_dev2504_unique_award_key_mapping")
