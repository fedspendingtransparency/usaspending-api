--
--
-- Purpose of this script is to aggregate and count children of IDV Awards.
--
--
-- ASSUMPTIONS
--
--
--  1. An IDV in the Awards table will have a "type" value that begins with "IDV_".
--  2. A child of an IDV can be identified by:
--        child.fpds_parent_agency_id = parent.fpds_agency_id AND
--        child.parent_award_piid     = parent.piid
--  3. IDVs can have other IDVs as children, but IDVs cannot be grandchildren or have other IDVs as grandchildren.
--  4. IDVs can have non-IDVs (contracts) as children or grandchildren, but not great grandchildren (assuming 3-level
--     max-depth in an IDV tree from root-to-leaf)
--  5. Non-IDVs (contracts) can only have IDVs as parents.
--  6. IDV children can be of mixed types; IDVs and non-IDVs (contracts).
--  7. Dollar rollups include non-IDV (contract) values only.  The dollar figures for actual IDVs are ignored.
--        base_and_all_options_value
--        base_exercised_options_val
--        total_obligation
--  8. Financial assistance awards cannot be children of IDVs.
--
--
-- BY WAY OF EXAMPLE
--
--
-- |-------------| |-------------| |-------------|         |-------------|                 |-------------|
-- |    IDV A    | |    IDV B    | |    IDV C    |         |    IDV D    |                 |    IDV E    |
-- |      $5     | |      $5     | |      $5     |         |      $5     |                 |      $5     |
-- |-------------| |-------------| |-------------|         |-------------|                 |-------------|
--                        |               |                       |                               |
--                        |               |                ---------------                 ---------------
--                        |               |                |             |                 |             |
--                 |-------------| |-------------| |-------------| |-------------| |-------------| |-------------|
--                 |  CONTRACT 1 | |    IDV F    | |    IDV G    | |  CONTRACT 2 | |    IDV H    | |  CONTRACT 3 |
--                 |     $2      | |      $5     | |      $5     | |      $2     | |      $5     | |      $2     |
--                 |-------------| |-------------| |-------------| |-------------| |-------------| |-------------|
--                                                                                        |
--                                                                                 ---------------
--                                                                                 |             |
--                                                                         |-------------| |-------------|
--                                                                         |  CONTRACT 4 | |  CONTRACT 5 |
--                                                                         |      $2     | |      $2     |
--                                                                         |-------------| |-------------|
--
-- Using above model, values would be as follows.  Only showing total_obligation for dollar figures to shorten example.
-- base_and_all_options_value and base_exercised_options_val work the same way.
--
--                         | IDV A | IDV B | IDV C | IDV D | IDV E | IDV F | IDV G | IDV H |
-- -----------------------------------------------------------------------------------------
-- direct_contract_count   |     0 |     1 |     0 |     1 |     1 |     0 |     0 |     2 |
-- direct_idv_count        |     0 |     0 |     1 |     1 |     1 |     0 |     0 |     0 |
-- direct_total_obligation |    $0 |    $2 |    $0 |    $2 |    $2 |    $0 |    $0 |    $4 |
-- rollup_contract_count   |     0 |     1 |     0 |     1 |     3 |     0 |     0 |     2 |
-- rollup_idv_count        |     0 |     0 |     1 |     1 |     1 |     0 |     0 |     0 |
-- rollup_total_obligation |    $0 |    $2 |    $0 |    $2 |    $6 |    $0 |    $0 |    $4 |
--
--
-- SPECIAL TREATMENT
--
--
-- IDVs that refer to themselves or Awards that do not exist or are not IDVs are not provided with a parent_award_id.
-- This allows us to provide rollup values for those IDVs even though they may not be wholly accurate in the Awards
-- table.  As Award data is improved, the number of these outliers should diminish.
--
--
-- PROCESSING
--
--
--  1. Empty parent_award table
--  2. Initialize parent_award table with IDVs from Award table (zero all sums and counts)
--  3. Count and sum up direct children of IDVs
--  4. Roll up values for IDVs with child IDVs
--
--
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
---------------------------------------------------  BONUS QUERY!  ----------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
--
-- In step 4 below, we roll up child IDV values into their parent.  This step assumes AT MOST one level of child IDVs.
-- Any more than that and our rollups will be incomplete.  The query listed here utilizes a recursive CTE to roll
-- up ALL child IDVs, regardless of depth.  The downside to this method is that it may not scale well.  Performance
-- really drops off when we start getting into hundreds of thousands of parent IDVs.  I am including it here for
-- posterity.  If we find that our one level of parentage assumption is invalid, we can switch to the recursive CTE at
-- the cost of performance.  Enjoy!
--
-- with recursive hierarchical_sums as (
--
--     select distinct on (p.award_id)
--         p.award_id idv_id,
--         p.award_id,
--         array[p.award_id] path,
--
--         p.direct_idv_count,
--         p.direct_contract_count,
--         p.direct_total_obligation,
--         p.direct_base_and_all_options_value,
--         p.direct_base_exercised_options_val
--
--     from
--         parent_award p
--         inner join parent_award c on c.parent_award_id = p.award_id
--
--     union all
--
--     select
--         p.idv_id,
--         c.award_id,
--         p.path || c.award_id,
--
--         c.direct_idv_count,
--         c.direct_contract_count,
--         c.direct_total_obligation,
--         c.direct_base_and_all_options_value,
--         c.direct_base_exercised_options_val
--
--     from
--         hierarchical_sums p
--         inner join parent_award c on
--             c.parent_award_id = p.award_id and
--             c.award_id != any(p.path)
--
-- )
-- update
--     parent_award
--
-- set
--     rollup_idv_count                  = t.rollup_idv_count,
--     rollup_contract_count             = t.rollup_contract_count,
--     rollup_total_obligation           = t.rollup_total_obligation,
--     rollup_base_and_all_options_value = t.rollup_base_and_all_options_value,
--     rollup_base_exercised_options_val = t.rollup_base_exercised_options_val
--
-- from (
--         select
--             idv_id,
--             sum(direct_idv_count)                  rollup_idv_count,
--             sum(direct_contract_count)             rollup_contract_count,
--             sum(direct_total_obligation)           rollup_total_obligation,
--             sum(direct_base_and_all_options_value) rollup_base_and_all_options_value,
--             sum(direct_base_exercised_options_val) rollup_base_exercised_options_val
--         from
--             hierarchical_sums
--         group by
--             idv_id
--     ) t
--
-- where
--     award_id = t.idv_id;
--
-----------------------------------------------------------------------------------------------------------------------
begin transaction;



-- Clear out the parent_award table.  Delete doesn't lock the table for read.
-- Truncate does.
do $$ begin raise notice 'Emptying parent_award'; end $$;

delete from parent_award;



-- Add all IDVs to the parent_award table.
do $$ begin raise notice 'Adding IDVs to parent_award'; end $$;

insert into parent_award (
    award_id, parent_award_id, generated_unique_award_id,

    direct_idv_count, direct_contract_count,
    direct_total_obligation, direct_base_and_all_options_value,
    direct_base_exercised_options_val,

    rollup_idv_count, rollup_contract_count,
    rollup_total_obligation, rollup_base_and_all_options_value,
    rollup_base_exercised_options_val
)
select
    c.id, p.id, c.generated_unique_award_id,

    0, 0,
    0, 0,
    0,

    0, 0,
    0, 0,
    0

from
    vw_awards c
    left outer join vw_awards p on
        p.piid = c.parent_award_piid and
        p.fpds_agency_id = c.fpds_parent_agency_id and
        p.type like 'IDV%' and
        p.id != c.id

where
    c.type like 'IDV%';



-- Sum up direct children and seed rolled up values with direct values (just in
-- case an IDV has no children - which a lot don't).  Full roll ups will happen
-- below.
do $$ begin raise notice 'Summing and counting direct children in parent_award'; end $$;

update
    parent_award

set
    rollup_idv_count                  = t.direct_idv_count,
    rollup_contract_count             = t.direct_contract_count,
    rollup_total_obligation           = coalesce(t.direct_total_obligation, 0),
    rollup_base_and_all_options_value = coalesce(t.direct_base_and_all_options_value, 0),
    rollup_base_exercised_options_val = coalesce(t.direct_base_exercised_options_val, 0),

    direct_idv_count                  = t.direct_idv_count,
    direct_contract_count             = t.direct_contract_count,
    direct_total_obligation           = coalesce(t.direct_total_obligation, 0),
    direct_base_and_all_options_value = coalesce(t.direct_base_and_all_options_value, 0),
    direct_base_exercised_options_val = coalesce(t.direct_base_exercised_options_val, 0)

from (
        select
            p.id,
            sum(case when c.type like 'IDV%' then 1 else 0 end)                                            direct_idv_count,
            sum(case when c.type like 'IDV%' then 0 else 1 end)                                            direct_contract_count,
            sum(case when c.type like 'IDV%' then 0 else c.total_obligation end)::numeric(23, 2)           direct_total_obligation,
            sum(case when c.type like 'IDV%' then 0 else c.base_and_all_options_value end)::numeric(23, 2) direct_base_and_all_options_value,
            sum(case when c.type like 'IDV%' then 0 else c.base_exercised_options_val end)::numeric(23, 2) direct_base_exercised_options_val
        from
            vw_awards p
            inner join vw_awards c on
                c.fpds_parent_agency_id = p.fpds_agency_id and
                c.parent_award_piid = p.piid and
                c.id != p.id
        where
            p.type like 'IDV%'
        group by
            p.id
    ) t

where
    t.id = parent_award.award_id;



-- At this point, only direct children have been summed up.  We need to take
-- that one step further and roll up grandchildren.
do $$ begin raise notice 'Rolling up sums and counts in parent_award'; end $$;

update
    parent_award
set
    rollup_idv_count                  = t.rollup_idv_count                  + direct_idv_count,
    rollup_contract_count             = t.rollup_contract_count             + direct_contract_count,
    rollup_total_obligation           = t.rollup_total_obligation           + direct_total_obligation,
    rollup_base_and_all_options_value = t.rollup_base_and_all_options_value + direct_base_and_all_options_value,
    rollup_base_exercised_options_val = t.rollup_base_exercised_options_val + direct_base_exercised_options_val
from (
        select
            p.award_id,
            sum(c.direct_idv_count)                  rollup_idv_count,
            sum(c.direct_contract_count)             rollup_contract_count,
            sum(c.direct_total_obligation)           rollup_total_obligation,
            sum(c.direct_base_and_all_options_value) rollup_base_and_all_options_value,
            sum(c.direct_base_exercised_options_val) rollup_base_exercised_options_val
        from
            parent_award p
            inner join parent_award c on
                c.parent_award_id = p.award_id
        group by
            p.award_id
    ) t
where
    t.award_id = parent_award.award_id;



commit;



do $$ begin
    raise notice 'If running this script by hand, consider running "vacuum (full, analyze) parent_award;" now.';
end $$;
