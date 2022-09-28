from usaspending_api.search.models import SubawardSearch
from usaspending_api.common.helpers.sql_helpers import execute_dml_sql


def update_subaward_city_county(table_name=SubawardSearch._meta.db_table):
    """
    Updates sub_place_of_perform_county_code, sub_place_of_perform_county_name, sub_place_of_perform_city_code,
    sub_legal_entity_county_code, sub_legal_entity_county_name, and sub_legal_entity_city_code in SubawardSearch table.
    Returns the count of rows affected.
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
            sub_place_of_perform_county_code = LPAD(CAST(CAST((REGEXP_MATCH(pop.county_numeric, '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS smallint) AS text), 3, '0'),
            sub_place_of_perform_county_name = pop.county_name,
            sub_place_of_perform_city_code = pop.census_code,
            sub_legal_entity_county_code = LPAD(CAST(CAST((REGEXP_MATCH(rec.county_numeric, '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS smallint) AS text), 3, '0'),
            sub_legal_entity_county_name = rec.county_name,
            sub_legal_entity_city_code = rec.census_code

        from
            "{table_name}" as s
            left outer join address_info as pop on
                pop.feature_name = UPPER(s.sub_place_of_perform_city_name) and
                pop.state_alpha = UPPER(s.sub_place_of_perform_state_code)
            left outer join address_info as rec on
                rec.feature_name = UPPER(s.sub_legal_entity_city_name) and
                rec.state_alpha = UPPER(s.sub_legal_entity_state_code)

        where
            s.broker_subaward_id = s1.broker_subaward_id and (
                pop.county_numeric is distinct from s.sub_place_of_perform_county_code or
                pop.county_name is distinct from s.sub_place_of_perform_county_name or
                pop.census_code is distinct from s.sub_place_of_perform_city_code or
                rec.county_numeric is distinct from s.sub_legal_entity_county_code or
                rec.county_name is distinct from s.sub_legal_entity_county_name or
                rec.census_code is distinct from s.sub_legal_entity_city_code
            )
    """

    return execute_dml_sql(sql)
