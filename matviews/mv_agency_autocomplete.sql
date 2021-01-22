DROP MATERIALIZED VIEW IF EXISTS mv_agency_autocomplete_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_agency_autocomplete_old CASCADE;

CREATE MATERIALIZED VIEW mv_agency_autocomplete_temp AS
with 
cited_agencies as ( 
    select 
        agency.id as agency_id, 
        agency.toptier_flag, 
        ta.toptier_agency_id, 
        ta.toptier_code, 
        ta.abbreviation as toptier_abbreviation, 
        ta.name as toptier_name, 
        sa.abbreviation as subtier_abbreviation, 
        sa.name as subtier_name, 
        ( 
            -- Subtier is cited as an awarding agency. 
            exists ( 
                select 
                from 
                    awards as a 
                    inner join transaction_normalized as tn on 
                        tn.id = a.latest_transaction_id and 
                        tn.action_date >= '2007-10-01'::date 
                where 
                    a.awarding_agency_id = agency.id 
            ) or 
            -- Toptier has at least one subtier that is cited as an awarding agency. 
            ( 
                agency.toptier_flag is true and 
                exists( 
                    select 
                    from 
                        agency as ag 
                        inner join awards as a on 
                            a.awarding_agency_id = ag.id 
                        inner join transaction_normalized as tn on 
                            tn.id = a.latest_transaction_id and 
                            tn.action_date >= '2007-10-01'::date 
                    where 
                        ag.toptier_agency_id = agency.toptier_agency_id 
                ) 
            ) 
        ) as has_awarding_data, 
        ( 
            -- Subtier is cited as a funding agency. 
            exists ( 
                select 
                from 
                    awards as a 
                    inner join transaction_normalized as tn on 
                        tn.id = a.latest_transaction_id and 
                        tn.action_date >= '2007-10-01'::date 
                where 
                    a.funding_agency_id = agency.id 
            ) or 
            -- Toptier has at least one subtier that is cited as an funding agency. 
            ( 
                agency.toptier_flag is true and 
                exists( 
                    select 
                    from 
                        agency as ag 
                        inner join awards as a on 
                            a.funding_agency_id = ag.id 
                        inner join transaction_normalized as tn on 
                            tn.id = a.latest_transaction_id and 
                            tn.action_date >= '2007-10-01'::date 
                    where 
                        ag.toptier_agency_id = agency.toptier_agency_id 
                ) 
            ) 
        ) as has_funding_data 
    from 
        agency 
        inner join toptier_agency as ta on ta.toptier_agency_id = agency.toptier_agency_id 
        inner join subtier_agency as sa on sa.subtier_agency_id = agency.subtier_agency_id 
    where 
        -- As per PO guidance, do not include these two agencies. 
        ta.toptier_code not in ('000', '067') 
) 
-- Advanced search is driven solely by subtier name so we need to collapse subtiers with the same 
-- name within their respective toptiers.  This leaves us in a weird situation with regard to 
-- other return values (such as abbreviations and the overall agency id).  To work around this issue 
-- we're going to group on names and use various aggregations to ensure a deterministic outcome and 
-- maximize our chances of returning a value (for example, if two subtiers share a name but only one 
-- of them has been cited as an awarding agency, we need to ensure the final outcome is flagged as 
-- an awarding agency). 
select 
    min(agency_id) as agency_autocomplete_id, 
    bool_or(toptier_flag) as toptier_flag, 
    toptier_agency_id, 
    toptier_code, 
    toptier_abbreviation, 
    toptier_name, 
    max(subtier_abbreviation) as subtier_abbreviation, 
    max(subtier_name) as subtier_name, 
    bool_or(has_awarding_data) as has_awarding_data, 
    bool_or(has_funding_data) as has_funding_data 
from 
    cited_agencies 
where 
    has_awarding_data is true or 
    has_funding_data is true 
group by 
    toptier_agency_id, 
    toptier_code, 
    toptier_abbreviation, 
    toptier_name, 
    upper(subtier_name)  WITH DATA;

CREATE UNIQUE INDEX idx_8790aea5$ba3_agency_autocomplete_id_temp ON mv_agency_autocomplete_temp USING BTREE(agency_autocomplete_id) WITH (fillfactor = 97);


ALTER MATERIALIZED VIEW IF EXISTS mv_agency_autocomplete RENAME TO mv_agency_autocomplete_old;
ALTER INDEX IF EXISTS idx_8790aea5$ba3_agency_autocomplete_id RENAME TO idx_8790aea5$ba3_agency_autocomplete_id_old;


ALTER MATERIALIZED VIEW mv_agency_autocomplete_temp RENAME TO mv_agency_autocomplete;
ALTER INDEX idx_8790aea5$ba3_agency_autocomplete_id_temp RENAME TO idx_8790aea5$ba3_agency_autocomplete_id;


ANALYZE VERBOSE mv_agency_autocomplete;
GRANT SELECT ON mv_agency_autocomplete TO readonly;
