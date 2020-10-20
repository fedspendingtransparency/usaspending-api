
DROP VIEW IF EXISTS account_delta_view;
CREATE VIEW account_delta_view AS
select
    -- Key
    faba.distinct_award_key,
    -- Award Data
    min(awd.total_loan_value) as total_loan_value,
    min(awd.type) as award_type,
    min(awd.generated_unique_award_id) as generated_unique_award_id,
    json_agg((
        select
            row_to_json(row)
        from
            (
                select
                    -- File C Data
                    faba.*,
                    -- Ref Data: Submission Attributes
                    sa.is_final_balances_for_fy,
                    -- Ref Data: DABS Submission Window
                    dabs.submission_fiscal_year,
                    dabs.submission_fiscal_quarter,
                    dabs.submission_fiscal_month,
                    dabs.is_quarter,
                    dabs.period_start_date,
                    dabs.period_end_date,
                    -- Ref Data: (Funding) Toptier Agency
                    a.id as funding_toptier_agency_id,
                    top_a.toptier_code as funding_toptier_agency_code,
                    top_a.name as funding_toptier_agency_name,

                    -- Ref Data: Treasury Account
                    taa.tas_rendering_label as treasury_account_symbol,
                    taa.account_title as treasury_account_title,

                    -- Ref Data: Federal Account
                    taa.federal_account_id,
                    fa.federal_account_code as federal_account_symbol,
                    fa.account_title as federal_account_title,

                    -- Ref Data: Program Activity
                    pa.program_activity_code,
                    pa.program_activity_name,

                    -- Ref Data: Object Class
                    oc.major_object_class,
                    oc.major_object_class_name,
                    oc.object_class,
                    oc.object_class_name,
                    oc.direct_reimbursable

                    -- Ref Data: DEF Code
--                    defc.group_name as disaster_emergency_fund_code_group_name
                from
                    financial_accounts_by_awards faba_inner
                left join treasury_appropriation_account taa on
                    taa.treasury_account_identifier = faba_inner.treasury_account_id
                    -- TODO: This toptier_agency + agency join is likely incorrect/incomplete
                    -- TOOD: NEEDS REVIEW. Existing COVID page uses DISTINCT ON to pick one of many eligble agency.agency_id values
                left join toptier_agency top_a on
                    top_a.toptier_agency_id = taa.funding_toptier_agency_id
                left join agency a on
                    a.toptier_agency_id = top_a.toptier_agency_id
                    and a.toptier_flag = true
                left join federal_account fa on
                    fa.id = taa.federal_account_id
                left join ref_program_activity pa on
                    pa.id = faba_inner.program_activity_id
                left join object_class oc on
                    oc.id = faba_inner.object_class_id
                where
                    faba.financial_accounts_by_awards_id = faba_inner.financial_accounts_by_awards_id
            ) row
    )) as financial_accounts_by_award
from
    financial_accounts_by_awards faba
inner join
    submission_attributes sa on (sa.submission_id = faba.submission_id)
--inner join
--    disaster_emergency_fund_code defc on (defc.code = faba.disaster_emergency_fund_code)
left join
    dabs_submission_window_schedule dabs on (
        sa.submission_window_id = dabs.id
--        and dabs.submission_reveal_date <= now()
    )
left join
    awards awd on (faba.award_id = awd.id)
group by
    faba.distinct_award_key;