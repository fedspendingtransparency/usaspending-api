award_contracts_mapping = {
    'Award ID': 'piid',
    'Recipient Name': 'recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Contract Award Type': 'type_description',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Funding Agency': 'funding_toptier_agency_name',  # Leave in for possible future use
    'Funding Sub Agency': 'funding_subtier_agency_name',  # Leave in for possible future use
}

grant_award_mapping = {
    'Award ID': 'fain',
    'Recipient Name': 'recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_toptier_agency_name',  # Leave in for possible future use
    'Funding Sub Agency': 'funding_subtier_agency_name',  # Leave in for possible future use
}

loan_award_mapping = {
    'Award ID': 'fain',
    'Recipient Name': 'recipient_name',
    'Issued Date': 'action_date',
    'Loan Value': 'face_value_loan_guarantee',
    'Subsidy Cost': 'original_loan_subsidy_cost',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Funding Agency': 'funding_toptier_agency_name',  # Leave in for possible future use
    'Funding Sub Agency': 'funding_subtier_agency_name',  # Leave in for possible future use
}

direct_payment_award_mapping = {
    'Award ID': 'fain',
    'Recipient Name': 'recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_toptier_agency_name',  # Leave in for possible future use
    'Funding Sub Agency': 'funding_subtier_agency_name',  # Leave in for possible future use
}

other_award_mapping = {
    'Award ID': 'fain',
    'Recipient Name': 'recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_toptier_agency_name',  # Leave in for possible future use
    'Funding Sub Agency': 'funding_subtier_agency_name',  # Leave in for possible future use
}

award_assistance_mapping = {**grant_award_mapping, **loan_award_mapping, **direct_payment_award_mapping,
                            **other_award_mapping}
non_loan_assistance_award_mapping = assistance_award_mapping = {**grant_award_mapping, **direct_payment_award_mapping,
                                                                **other_award_mapping}
