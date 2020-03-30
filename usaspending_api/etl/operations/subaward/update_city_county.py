from usaspending_api.awards.models import Subaward
from usaspending_api.common.helpers.sql_helpers import execute_dml_sql


def update_subaward_city_county(table_name=Subaward._meta.db_table):
    """
    Updates pop_county_code, pop_county_name, pop_city_code, recipient_location_county_code,
    recipient_location_county_name, and recipient_location_city_code in the Subaward (or a Subaward-like)
    table.  Returns the count of rows affected.
    """
    sql = f"""
        with
        address_info as (
            select distinct on (upper(feature_name), state_alpha)
                upper(feature_name) as feature_name,
                state_alpha,
                county_numeric,
                upper(county_name) as county_name,
                census_code

            from
                ref_city_county_state_code

            where
                feature_class = 'Populated Place' and
                coalesce(feature_name, '') !=  '' and
                coalesce(state_alpha, '') !=  ''

            order by
                upper(feature_name),
                state_alpha,
                county_sequence,
                coalesce(date_edited, date_created) desc,
                id desc
        )
        update
            "{table_name}" as s1

        set
            pop_county_code = pop.county_numeric,
            pop_county_name = pop.county_name,
            pop_city_code = pop.census_code,
            recipient_location_county_code = rec.county_numeric,
            recipient_location_county_name = rec.county_name,
            recipient_location_city_code = rec.census_code

        from
            "{table_name}" as s
            left outer join address_info as pop on
                pop.feature_name = s.pop_city_name and
                pop.state_alpha = s.pop_state_code
            left outer join address_info as rec on
                rec.feature_name = s.recipient_location_city_name and
                rec.state_alpha = s.recipient_location_state_code

        where
            s.id = s1.id and (
                pop.county_numeric is distinct from s.pop_county_code or
                pop.county_name is distinct from s.pop_county_name or
                pop.census_code is distinct from s.pop_city_code or
                rec.county_numeric is distinct from s.recipient_location_county_code or
                rec.county_name is distinct from s.recipient_location_county_name or
                rec.census_code is distinct from s.recipient_location_city_code
            )
    """

    return execute_dml_sql(sql)
