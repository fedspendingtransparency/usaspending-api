create materialized view exec_comp_matview as
(
    select
        distinct(legal_entity.recipient_unique_id) as duns,
        exec_comp.officer_1_name as officer_1_name,
        exec_comp.officer_1_amount as officer_1_amount,
        exec_comp.officer_2_name as officer_2_name,
        exec_comp.officer_2_amount as officer_2_amount,
        exec_comp.officer_3_name as officer_3_name,
        exec_comp.officer_3_amount as officer_3_amount,
        exec_comp.officer_4_name as officer_4_name,
        exec_comp.officer_4_amount as officer_4_amount,
        exec_comp.officer_5_name as officer_5_name,
        exec_comp.officer_5_amount as officer_5_amount
    from
        references_legalentityofficers as exec_comp
        inner join
        legal_entity on legal_entity.legal_entity_id = exec_comp.legal_entity_id
    where
        exec_comp.officer_1_name is not null or
        exec_comp.officer_1_amount is not null or
        exec_comp.officer_2_name is not null or
        exec_comp.officer_2_amount is not null or
        exec_comp.officer_3_name is not null or
        exec_comp.officer_3_amount is not null or
        exec_comp.officer_4_name is not null or
        exec_comp.officer_4_amount is not null or
        exec_comp.officer_5_name is not null or
        exec_comp.officer_5_amount is not null
);

create index exec_comp_duns_idx on exec_comp_matview (duns);