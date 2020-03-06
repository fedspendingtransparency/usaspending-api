default_mapping = {
    "Recipient Name": "recipient_name",
    "Recipient DUNS Number": "recipient_unique_id",
    "recipient_id": "_recipient_id",  # recipient_id conflicts with another column in the model
    "Awarding Agency": "awarding_toptier_agency_name",
    "Awarding Agency Code": "awarding_toptier_agency_code",
    "Awarding Sub Agency": "awarding_subtier_agency_name",
    "Awarding Sub Agency Code": "awarding_subtier_agency_code",
    "Funding Agency": "funding_toptier_agency_name",  # Leave in for possible future use
    "Funding Agency Code": "funding_toptier_agency_code",  # Leave in for possible future use
    "Funding Sub Agency": "funding_subtier_agency_name",  # Leave in for possible future use
    "Funding Sub Agency Code": "funding_subtier_agency_code",  # Leave in for possible future use
    "Place of Performance City Code": "pop_city_code",
    "Place of Performance State Code": "pop_state_code",
    "Place of Performance Country Code": "pop_country_code",
    "Place of Performance Zip5": "pop_zip5",
    "Period of Performance Start Date": "period_of_performance_start_date",
    "Period of Performance Current End Date": "period_of_performance_current_end_date",
    "Description": "description",
    "Last Modified Date": "last_modified_date",
    "Base Obligation Date": "date_signed",
}

award_contracts_mapping = default_mapping.copy()
award_idv_mapping = default_mapping.copy()
grant_award_mapping = default_mapping.copy()
loan_award_mapping = default_mapping.copy()
direct_payment_award_mapping = default_mapping.copy()
other_award_mapping = default_mapping.copy()

award_contracts_mapping.update(
    {
        "Award ID": "piid",
        "Start Date": "period_of_performance_start_date",
        "End Date": "period_of_performance_current_end_date",
        "Award Amount": "award_amount",
        "Contract Award Type": "type_description",
    }
)

award_idv_mapping.update(
    {
        "Award ID": "piid",
        "Start Date": "period_of_performance_start_date",
        "Award Amount": "award_amount",
        "Contract Award Type": "type_description",
        "Last Date to Order": "ordering_period_end_date",
    }
)

grant_award_mapping.update(
    {
        "Award ID": "fain",
        "Start Date": "period_of_performance_start_date",
        "End Date": "period_of_performance_current_end_date",
        "Award Amount": "award_amount",
        "Award Type": "type_description",
        "SAI Number": "sai_number",
        "CFDA Number": "cfda_number",
    }
)

loan_award_mapping.update(
    {
        "Award ID": "fain",
        "Issued Date": "action_date",
        "Loan Value": "total_loan_value",
        "Subsidy Cost": "award_amount",
        "SAI Number": "sai_number",
        "CFDA Number": "cfda_number",
    }
)

direct_payment_award_mapping.update(
    {
        "Award ID": "fain",
        "Start Date": "period_of_performance_start_date",
        "End Date": "period_of_performance_current_end_date",
        "Award Amount": "award_amount",
        "Award Type": "type_description",
        "SAI Number": "sai_number",
        "CFDA Number": "cfda_number",
    }
)

other_award_mapping.update(
    {
        "Award ID": "fain",
        "Start Date": "period_of_performance_start_date",
        "End Date": "period_of_performance_current_end_date",
        "Award Amount": "award_amount",
        "Award Type": "type_description",
        "SAI Number": "sai_number",
        "CFDA Number": "cfda_number",
    }
)

award_assistance_mapping = {
    **grant_award_mapping,
    **loan_award_mapping,
    **direct_payment_award_mapping,
    **other_award_mapping,
}
non_loan_assistance_award_mapping = {**grant_award_mapping, **direct_payment_award_mapping, **other_award_mapping}
