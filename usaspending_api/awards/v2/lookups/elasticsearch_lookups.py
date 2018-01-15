'''
Look ups for elasticsearch fields 
to be displayed for the front end
'''
TRANSACTIONS_LOOKUP = {"Recipient Name": "recipient_name",
                       "Action Date": "action_date",
                       "Transaction Amount": "transaction_amount",
                       "Contract Award Type": "type_description",
                       "Awarding Agency": "awarding_toptier_agency_name",
                       "Awarding Sub Agency": "awarding_subtier_agency_name",
                       "Funding Agency": "funding_toptier_agency_name",
                       "Funding Sub Agency": "funding_subtier_agency_name",
                       "Issued Date": "period_of_performance_start_date",
                       "Loan Value": "face_value_loan_guarantee",
                       "Subsidy Cost": "original_loan_subsidy_cost",
                       "Mod": "modification_number",
                       "Award ID": "award_id",
                       "Award Type": "type_description",
                       "awarding_agency_id": "awarding_agency_id"}

award_type_mapping = {
    '02': 'Grants',
    '03': 'Grants',
    '04': 'Grants',
    '05': 'Grants',
    '06': 'Direct Payments',
    '07': 'Loans',
    '08': 'Loans',
    '09': 'Other',
    '10': 'Direct Payments',
    '11': 'Other',
    'A': 'Contracts',
    'B': 'Contracts',
    'C': 'Contracts',
    'D': 'Contracts'
    # 'E': 'Unknown Type', TODO - check for allocation of these types
    # 'F': 'Cooperative Agreement',
    # 'G': 'Grant for Research',
    # 'S': 'Funded Space Act Agreement',
    # 'T': 'Training Grant'}
}