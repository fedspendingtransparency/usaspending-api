<ul class="nav nav-stacked" id="sidebar">
  <li><a href="/docs/intro-tutorial">Introductory Tutorial</a></li>
  <li><a href="/docs/using-the-api">Using this API</a></li>
  <li><a href="/docs/endpoints">Endpoints</a></li>
  <li><a href="/docs/data-dictionary">Data Dictionary</a>
  <!--<ul>
    <li><a href="#datadictionary">Data Dictionary</a>
    <li><a href="#type-description">Type Descriptions</a>
    <li><a href="#award">Award Endpoints</a>
    <li><a href="#transaction">Transaction Endpoints</a>
    <li><a href="#accounts">Accounts Endpoints</a>
    <li><a href="#references">Reference Endpoints</a>
  </ul>-->
  </li>
  <li><a href="/docs/recipes">Request Recipes</a></li>
</ul>

[//]: # (Begin Content)
## Data Dictionary <a name="datadictionary"></a>

Federal spending has a vocabulary all its own. This page is under construction, but will strive to help you understand the most commonly requested endpoints and fields returned.

You can also visit our [online spreadsheet](https://docs.google.com/spreadsheets/d/1KbP55qhrcrxtwyDXBfXnWAFhokAH2Gx4R_U0rYOyJVg/edit#gid=0) for additional term descriptions and references.

#### Type Descriptions <a name="type-description"></a>

| Type | Description |
| ---- | ---- |
| Nested Object | This field contains a set of its own fields. Where applicable, the description for Nested Objects will contain a link to the list of fields that it contains, and descriptions of those fields.|
| Integer | This field contains a whole number, with no decimals or fractions.|
| String | This field contains a 'string' of text--which can be anything from a few letters and numbers to multiple paragraphs. Special formatting is noted in the description, or the lexicon |
| Float | This field contains a 'floating point number,' which is a number that can contain one or more decimal places. |
| Date | This field contains a date, represented as a string in [YYYY]-[MM]-[DD] format |
| Datetime | This field contains a date, represented as a string in [YYYY]-[MM]-[DD] format, and time. Example: `2017-03-14T14:52:03.398918Z` |
| Boolean | This field contains a boolean value, either `true` or `false` |

#### Award Endpoints <a name="award"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| financial_set | Nested Object | An array of objects containing data from <a href="#accounts-by-award">Financial Accounts (by Award)</a> for which this award is the parent |
| child_award | Nested Object | An array of objects containing data from this endpoint, representing child awards of this award |
| transaction | Nested Object | An array of <a href="#transaction">transactions</a> for which this award is the parent |
| subaward | Nested Object | An array of subawards (NOT YET IMPLEMENTED) for which this award is the parent |
| id | Integer | Internal primary key. Guaranteed to be unique. |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| type | String | 	The mechanism used to distribute funding. The federal government can distribute funding in several forms. These award types include contracts, grants, loans, and direct payments. |
| type_description | String | The plain text description of the type of the award |
| piid | String | Procurement Instrument Identifier - A unique identifier assigned to a federal contract, purchase order, basic ordering agreement, basic agreement, and blanket purchase agreement. It is used to track the contract, and any modifications or transactions related to it. After October 2017, it is between 13 and 17 digits, both letters and numbers. |
| parent_award | Integer | The parent award's id, if applicable |
| fain | String | An identification code assigned to each financial assistance award tracking purposes. The FAIN is tied to that award (and all future modifications to that award) throughout the award’s life. Each FAIN is assigned by an agency. Within an agency, FAIN are unique: each new award must be issued a new FAIN. FAIN stands for Federal Award Identification Number, though the digits are letters, not numbers. |
| uri | String | The uri of the award |
| total_obligation | Float | The amount of money the government is obligated to pay for the award |
| total_outlay | Float | The total amount of money paid out for this award |
| awarding_agency | Nested Object | The awarding <a href="#agencies">agency</a> for the award |
| funding_agency | Nested Object | The funding <a href="#agencies">agency</a> for the award |
| date_signed | Date | The date the award was signed |
| recipient | Nested Object | The <a href="#recipients">recipient</a> of the award |
| description | String | A description of the award |
| period_of_performance_start_date | Date | The start date for the period of performance |
| period_of_performance_current_end_date | Date | The current, not original, period of performance end date |
| place_of_performance | Nested Object | The principal place of business, where the majority of the work is performed. For example, in a manufacturing contract, this would be the main plant where items are produced. The nested object uses the fields from <a href="#locations">locations</a> |
| potential_total_value_of_award | Float | The sum of the potential_value_of_award from associated transactions |
| last_modified_date | Date | The date this award was last modified |
| certified_date | Date | The date this record was certified |
| create_date | Datetime | The date this record was created in the API |
| update_date | Datetime | The last time this record was updated in the API |
| latest_transaction | Nested Object | The latest <a href="#transaction">transaction</a> by action_date associated with this award |

#### Transaction Endpoints <a name="transaction"></a>

###### Transaction

| Field | Type | Description |
| ----- | ----- | ----- |
| latest_for_award | Nested Object | The <a href="#award">award</a> for which this transaction is the latest |
| contract_data | Nested Object | The <a href="#transaction-contract">contract</a> data for this transaction, if applicable |
| assistance_data | Nested Object | The <a href="#transaction-assistance">assistance</a> data for this transaction, if applicable |
| id | Integer | Internal primary key. Guaranteed to be unique. |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| award | Integer | The id of the award which to which this transaction applies |
| usaspending_unique_transaction_id | String | If this record is legacy USASpending data, this is the unique transaction identifier from that system |
| submission | Integer | The id of the submission which created this submission |
| type | String | The type for this transaction. For example, A, B, C, D |
| type_description | String | The plain text description of the transaction type |
| period_of_performance_start_date | Date | The period of performance start date |
| period_of_performance_current_end_date | Date | The current end date of the period of performance |
| action_date | Date | The date this transaction was actioned |
| action_type | String | The type of transaction. For example, A, B, C, D |
| federal_action_obligation | Float | The obligation of the federal government for this transaction |
| modification_number | String | The modification number for this transaction |
| awarding_agency | Nested Object | The awarding <a href="#agencies">agency</a> for the transaction |
| funding_agency | Nested Object | The funding <a href="#agencies">agency</a> for the transaction |
| recipient | Nested Object | The <a href="#recipients">recipient</a> of the transaction |
| description | String | The description of this transaction |
| place_of_performance | Nested Object | The <a href="#locations">location</a> where the work on this transaction was performed |
| drv_award_transaction_usaspend | Float |  |
| drv_current_total_award_value_amount_adjustment | Float |  |
| drv_potential_total_award_value_amount_adjustment | Float |  |
| last_modified_date | Date | The date this transaction was last modified |
| certified_date | Date | The date this transaction was certified |
| create_date | Datetime | The date this transaction was created in the API |
| update_date | Datetime | The last time this transaction was updated in the API |


###### Transaction Contract Data <a name="transaction-contract"></a>


| Field | Type | Description |
| ----- | ----- | ----- |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| transaction | Nested Object | The <a href="#transaction">transaction</a> for which this is the contract data |
| submission | Integer | The id of the <a href="#submission">submission</a> which created this entity |
| piid | String | The PIID of this transaction |
| parent_award_id | String | The parent award id for this transaction. This is generally the piid of an IDV |
| cost_or_pricing_data | String |  |
| type_of_contract_pricing | String | The type of contract pricing data, as a code |
| type_of_contract_pricing_description | String | A plain text description of the type of contract pricing data |
| naics | String | Specified which industry the work for this transaction falls into. A 6-digit code |
| naics_description | String | A plain text description of the NAICS code |
| period_of_performance_potential_end_date | Date | The potential end date of the period of performance |
| ordering_period_end_date | String | The end date for the ordering period |
| current_total_value_award | Float | The current value of the award |
| potential_total_value_of_award | Float | The potential total value of the award |
| referenced_idv_agency_identifier | String | The agency identifier of the agency on the IDV |
| idv_type | String | The IDV type code |
| multiple_or_single_award_idv | String | Specifies whether the IDV is a single more multiple award vehicle |
| type_of_idc | String | Code representing the type of IDC |
| a76_fair_act_action | String | A-76 FAIR act action |
| dod_claimant_program_code | String |  |
| clinger_cohen_act_planning | String |  |
| commercial_item_acquisition_procedures | String |  |
| commercial_item_test_program | String |  |
| consolidated_contract | String |  |
| contingency_humanitarian_or_peacekeeping_operation | String |  |
| contract_bundling | String |  |
| contract_financing | String |  |
| contracting_officers_determination_of_business_size | String |  |
| cost_accounting_standards | String |  |
| country_of_product_or_service_origin | String |  |
| davis_bacon_act | String |  |
| evaluated_preference | String |  |
| extent_competed | String |  |
| fed_biz_opps | String |  |
| foreign_funding | String |  |
| gfe_gfp | String |  |
| information_technology_commercial_item_category | String |  |
| interagency_contracting_authority | String |  |
| local_area_set_aside | String |  |
| major_program | String |  |
| purchase_card_as_payment_method | String |  |
| multi_year_contract | String |  |
| national_interest_action | String |  |
| number_of_actions | String |  |
| number_of_offers_received | String |  |
| other_statutory_authority | String |  |
| performance_based_service_acquisition | String |  |
| place_of_manufacture | String |  |
| price_evaluation_adjustment_preference_percent_difference | Float |  |
| product_or_service_code | String |  |
| program_acronym | String |  |
| other_than_full_and_open_competition | String |  |
| recovered_materials_sustainability | String |  |
| research | String |  |
| sea_transportation | String |  |
| service_contract_act | String |  |
| small_business_competitiveness_demonstration_program | String |  |
| solicitation_identifier | String |  |
| solicitation_procedures | String |  |
| fair_opportunity_limited_sources | String |  |
| subcontracting_plan | String |  |
| program_system_or_equipment_code | String |  |
| type_set_aside | String |  |
| epa_designated_product | String |  |
| walsh_healey_act | String | Denotes whether this transaction is subject to the Walsh-Healey act |
| transaction_number | String | The transaction number for this transaction |
| referenced_idv_modification_number | String | The modification number for the referenced IDV |
| rec_flag | String | The rec flag |
| drv_parent_award_awarding_agency_code | String |  |
| drv_current_aggregated_total_value_of_award | Float |  |
| drv_current_total_value_of_award | Float |  |
| drv_potential_award_idv_amount_total_estimate | Float |  |
| drv_potential_aggregated_award_idv_amount_total_estimate | Float |  |
| drv_potential_aggregated_total_value_of_award | Float |  |
| drv_potential_total_value_of_award | Float |  |
| create_date | Datetime | The date this record was created in the API |
| update_date | Datetime | The last time this record was updated in the API |
| last_modified_date | Date | The last time this transaction was modified |
| certified_date | Date | The date this record was certified |
| reporting_period_start | Date | The date marking the start of the reporting period |
| reporting_period_end | Date | The date marking the end of the reporting period |


###### Transaction Assistance Data <a name="transaction-assistance"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| transaction | Nested Object | The <a href="#transaction">transaction</a> for which this is the assistance data |
| submission | Integer | The id of the <a href="#submission">submission</a> which created this entity |
| fain | String |  |
| uri | String |  |
| cfda_number | String |  |
| cfda_title | String |  |
| cfda | Nested Object | The <a href="#cfda-program">CFDA program</a> associated with this transaction  |
| business_funds_indicator | String |  |
| non_federal_funding_amount | Float |  |
| total_funding_amount | Float |  |
| face_value_loan_guarantee | Float |  |
| original_loan_subsidy_cost | Float |  |
| record_type | IntegerField |  |
| correction_late_delete_indicator | String |  |
| fiscal_year_and_quarter_correction | String |  |
| sai_number | String |  |
| drv_federal_funding_amount | Float |  |
| drv_award_finance_assistance_type_label | String |  |
| reporting_period_start | Date |  |
| reporting_period_end | Date |  |
| last_modified_date | Date |  |
| submitted_type | String |  |
| certified_date | Date |  |
| create_date | Datetime |  |
| update_date | Datetime |  |
| period_of_performance_start_date | Date |  |
| period_of_performance_current_end_date | Date |  |

#### Accounts Endpoints <a name="accounts"></a>

###### Appropriation Account Balances <a name="appropriation-account"></a>
| Field | Type | Description |
| ----- | ----- | ----- |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| appropriation_account_balances_id | Integer | Internal primary key. Guaranteed to be unique. |
| treasury_account_identifier | Nested Object | The <a href="#tas">Treasury Account</a> for this balance record |
| submission | Integer | The id of the <a href="#submission">submission</a> which created this entity |
| budget_authority_unobligated_balance_brought_forward_fyb | Float |  |
| adjustments_to_unobligated_balance_brought_forward_cpe | Float |  |
| budget_authority_appropriated_amount_cpe | Float |  |
| borrowing_authority_amount_total_cpe | Float |  |
| contract_authority_amount_total_cpe | Float |  |
| spending_authority_from_offsetting_collections_amount_cpe | Float |  |
| other_budgetary_resources_amount_cpe | Float |  |
| budget_authority_available_amount_total_cpe | Float |  |
| gross_outlay_amount_by_tas_cpe | Float |  |
| deobligations_recoveries_refunds_by_tas_cpe | Float |  |
| unobligated_balance_cpe | Float |  |
| status_of_budgetary_resources_total_cpe | Float |  |
| obligations_incurred_total_by_tas_cpe | Float |  |
| drv_appropriation_availability_period_start_date | Date |  |
| drv_appropriation_availability_period_end_date | Date |  |
| drv_appropriation_account_expired_status | String |  |
| tas_rendering_label | String |  |
| drv_obligations_unpaid_amount | Float |  |
| drv_other_obligated_amount | Float |  |
| reporting_period_start | Date |  |
| reporting_period_end | Date |  |
| last_modified_date | Date |  |
| certified_date | Date |  |
| create_date | Datetime |  |
| update_date | Datetime |  |

###### Appropriation Account Balances Quarterly <a name="appropriation-account-balances-quarterly"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| id | Integer | Internal primary key. Guaranteed to be unique. |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| treasury_account_identifier | Relation |  |
| submission | Relation |  |
| budget_authority_unobligated_balance_brought_forward_fyb | Float |  |
| adjustments_to_unobligated_balance_brought_forward_cpe | Float |  |
| budget_authority_appropriated_amount_cpe | Float |  |
| borrowing_authority_amount_total_cpe | Float |  |
| contract_authority_amount_total_cpe | Float |  |
| spending_authority_from_offsetting_collections_amount_cpe | Float |  |
| other_budgetary_resources_amount_cpe | Float |  |
| budget_authority_available_amount_total_cpe | Float |  |
| gross_outlay_amount_by_tas_cpe | Float |  |
| deobligations_recoveries_refunds_by_tas_cpe | Float |  |
| unobligated_balance_cpe | Float |  |
| status_of_budgetary_resources_total_cpe | Float |  |
| obligations_incurred_total_by_tas_cpe | Float |  |
| create_date | Datetime |  |
| update_date | Datetime |  |

##### Appropriation Account (by Category) <a name="accounts-prg-obj"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| financial_accounts_by_program_activity_object_class_id | Integer | Internal primary key. Guaranteed to be unique. |
| program_activity | Relation |  |
| submission | Relation |  |
| object_class | Relation |  |
| treasury_account | Relation |  |
| ussgl480100_undelivered_orders_obligations_unpaid_fyb | Float |  |
| ussgl480100_undelivered_orders_obligations_unpaid_cpe | Float |  |
| ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe | Float |  |
| ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe | Float |  |
| ussgl490100_delivered_orders_obligations_unpaid_fyb | Float |  |
| ussgl490100_delivered_orders_obligations_unpaid_cpe | Float |  |
| ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe | Float |  |
| ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe | Float |  |
| ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb | Float |  |
| ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe | Float |  |
| ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe | Float |  |
| ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe | Float |  |
| ussgl490200_delivered_orders_obligations_paid_cpe | Float |  |
| ussgl490800_authority_outlayed_not_yet_disbursed_fyb | Float |  |
| ussgl490800_authority_outlayed_not_yet_disbursed_cpe | Float |  |
| ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe | Float |  |
| obligations_undelivered_orders_unpaid_total_fyb | Float |  |
| obligations_undelivered_orders_unpaid_total_cpe | Float |  |
| obligations_delivered_orders_unpaid_total_fyb | Float |  |
| obligations_delivered_orders_unpaid_total_cpe | Float |  |
| gross_outlays_undelivered_orders_prepaid_total_fyb | Float |  |
| gross_outlays_undelivered_orders_prepaid_total_cpe | Float |  |
| gross_outlays_delivered_orders_paid_total_fyb | Float |  |
| gross_outlays_delivered_orders_paid_total_cpe | Float |  |
| gross_outlay_amount_by_program_object_class_fyb | Float |  |
| gross_outlay_amount_by_program_object_class_cpe | Float |  |
| obligations_incurred_by_program_object_class_cpe | Float |  |
| ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe | Float |  |
| ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe | Float |  |
| ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe | Float |  |
| ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe | Float |  |
| deobligations_recoveries_refund_pri_program_object_class_cpe | Float |  |
| drv_obligations_incurred_by_program_object_class | Float |  |
| drv_obligations_undelivered_orders_unpaid | Float |  |
| reporting_period_start | Date |  |
| reporting_period_end | Date |  |
| last_modified_date | Date |  |
| certified_date | Date |  |
| create_date | Datetime |  |
| update_date | Datetime |  |

##### Appropriation Account (by Category) Quarterly <a name="accounts-prg-obj-quarterly"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| id | Integer | Internal primary key. Guaranteed to be unique. |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| treasury_account | Relation |  |
| program_activity | Relation |  |
| object_class | Relation |  |
| submission | Relation |  |
| ussgl480100_undelivered_orders_obligations_unpaid_fyb | Float |  |
| ussgl480100_undelivered_orders_obligations_unpaid_cpe | Float |  |
| ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe | Float |  |
| ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe | Float |  |
| ussgl490100_delivered_orders_obligations_unpaid_fyb | Float |  |
| ussgl490100_delivered_orders_obligations_unpaid_cpe | Float |  |
| ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe | Float |  |
| ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe | Float |  |
| ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb | Float |  |
| ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe | Float |  |
| ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe | Float |  |
| ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe | Float |  |
| ussgl490200_delivered_orders_obligations_paid_cpe | Float |  |
| ussgl490800_authority_outlayed_not_yet_disbursed_fyb | Float |  |
| ussgl490800_authority_outlayed_not_yet_disbursed_cpe | Float |  |
| ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe | Float |  |
| obligations_undelivered_orders_unpaid_total_fyb | Float |  |
| obligations_undelivered_orders_unpaid_total_cpe | Float |  |
| obligations_delivered_orders_unpaid_total_fyb | Float |  |
| obligations_delivered_orders_unpaid_total_cpe | Float |  |
| gross_outlays_undelivered_orders_prepaid_total_fyb | Float |  |
| gross_outlays_undelivered_orders_prepaid_total_cpe | Float |  |
| gross_outlays_delivered_orders_paid_total_fyb | Float |  |
| gross_outlays_delivered_orders_paid_total_cpe | Float |  |
| gross_outlay_amount_by_program_object_class_fyb | Float |  |
| gross_outlay_amount_by_program_object_class_cpe | Float |  |
| obligations_incurred_by_program_object_class_cpe | Float |  |
| ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe | Float |  |
| ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe | Float |  |
| ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe | Float |  |
| ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe | Float |  |
| deobligations_recoveries_refund_pri_program_object_class_cpe | Float |  |
| create_date | Datetime |  |
| update_date | Datetime |  |


##### Federal Account <a name="federal-account"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| id | Integer | Internal primary key. Guaranteed to be unique. |
| agency_identifier | String |  |
| main_account_code | String |  |
| account_title | String |  |

##### TAS <a name="tas"></a> (Treasury Account Symbol)

| Field | Type | Description |
| ----- | ----- | ----- |
| financialaccountsbyawards | Nested Object | Array of <a href="#accounts-by-award">financial accounts (by award)</a> assosciated to this account_title |
| account_balances | Nested Object | Array of <a href="#appropriation-account">appropriation account balances</a> assosciated with this account |
| program_balances | Nested Object | Array of financial accounts (by program activity and object class) assosciated with this account |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| treasury_account_identifier | Integer | Internal primary key. Guaranteed to be unique. |
| tas_rendering_label | String |  |
| allocation_transfer_agency_id | String |  |
| agency_id | String |  |
| beginning_period_of_availability | String |  |
| ending_period_of_availability | String |  |
| availability_type_code | String |  |
| main_account_code | String |  |
| sub_account_code | String |  |
| account_title | String |  |
| reporting_agency_id | String |  |
| reporting_agency_name | String |  |
| budget_bureau_code | String |  |
| budget_bureau_name | String |  |
| fr_entity_code | String |  |
| fr_entity_description | String |  |
| budget_function_code | String |  |
| budget_function_title | String |  |
| budget_subfunction_code | String |  |
| budget_subfunction_title | String |  |
| drv_appropriation_availability_period_start_date | Date |  |
| drv_appropriation_availability_period_end_date | Date |  |
| drv_appropriation_account_expired_status | String |  |
| create_date | Datetime |  |
| update_date | Datetime |  |

##### Accounts by Award <a name="accounts-by-award"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| financial_accounts_by_awards_id | Integer | Internal primary key. Guaranteed to be unique. |
| treasury_account | Nested Object | The <a href="#tas">treasury account</a> for this entry |
| submission | Integer | The id of the <a href="#submission">submission</a> which created this entity |
| award | Integer | The id of the <a href="#award">award</a> to which this entity is associated |
| program_activity_name | String |  |
| program_activity_code | Nested Object |  |
| object_class | Nested Object |  |
| by_direct_reimbursable_funding_source | String |  |
| piid | String |  |
| parent_award_id | String |  |
| fain | String |  |
| uri | String |  |
| award_type | String |  |
| ussgl480100_undelivered_orders_obligations_unpaid_fyb | Float |  |
| ussgl480100_undelivered_orders_obligations_unpaid_cpe | Float |  |
| ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe | Float |  |
| ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe | Float |  |
| ussgl490100_delivered_orders_obligations_unpaid_fyb | Float |  |
| ussgl490100_delivered_orders_obligations_unpaid_cpe | Float |  |
| ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe | Float |  |
| ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe | Float |  |
| ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb | Float |  |
| ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe | Float |  |
| ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe | Float |  |
| ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe | Float |  |
| ussgl490200_delivered_orders_obligations_paid_cpe | Float |  |
| ussgl490800_authority_outlayed_not_yet_disbursed_fyb | Float |  |
| ussgl490800_authority_outlayed_not_yet_disbursed_cpe | Float |  |
| ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe | Float |  |
| obligations_undelivered_orders_unpaid_total_cpe | Float |  |
| obligations_delivered_orders_unpaid_total_fyb | Float |  |
| obligations_delivered_orders_unpaid_total_cpe | Float |  |
| gross_outlays_undelivered_orders_prepaid_total_fyb | Float |  |
| gross_outlays_undelivered_orders_prepaid_total_cpe | Float |  |
| gross_outlays_delivered_orders_paid_total_fyb | Float |  |
| gross_outlay_amount_by_award_fyb | Float |  |
| gross_outlay_amount_by_award_cpe | Float |  |
| obligations_incurred_total_by_award_cpe | Float |  |
| ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe | Float |  |
| ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe | Float |  |
| ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe | Float |  |
| ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe | Float |  |
| deobligations_recoveries_refunds_of_prior_year_by_award_cpe | Float |  |
| obligations_undelivered_orders_unpaid_total_fyb | Float |  |
| gross_outlays_delivered_orders_paid_total_cpe | Float |  |
| drv_award_id_field_type | String |  |
| drv_obligations_incurred_total_by_award | Float |  |
| reporting_period_start | Date |  |
| reporting_period_end | Date |  |
| last_modified_date | Date |  |
| certified_date | Date |  |
| create_date | Datetime |  |
| update_date | Datetime |  |

#### Reference Endpoints <a name="references"></a>

###### Locations <a name="locations"></a>


| Field | Type | Description |
| ----- | ----- | ----- |
| legalentity | Nested Object | Array of <a href="#recipients">recipients</a> located at this location |
| award | Nested Object | An array of <a href="#award">awards</a> for which this location is the place of performance |
| transaction | Nested Object | An array of <a href="#transaction">transaction</a> for which this location is the place of performance |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| location_id | Integer | Internal primary key. Guaranteed to be unique. |
| location_country_code | Relation |  |
| country_name | String |  |
| state_code | String |  |
| state_name | String |  |
| state_description | String |  |
| city_name | String |  |
| city_code | String |  |
| county_name | String |  |
| county_code | String |  |
| address_line1 | String |  |
| address_line2 | String |  |
| address_line3 | String |  |
| foreign_location_description | String |  |
| zip4 | String |  |
| zip_4a | String |  |
| congressional_code | String |  |
| performance_code | String |  |
| zip_last4 | String |  |
| zip5 | String |  |
| foreign_postal_code | String |  |
| foreign_province | String |  |
| foreign_city_name | String |  |
| reporting_period_start | Date |  |
| reporting_period_end | Date |  |
| last_modified_date | Date |  |
| certified_date | Date |  |
| create_date | Datetime |  |
| update_date | Datetime |  |
| place_of_performance_flag | BooleanField |  |
| recipient_flag | BooleanField |  |


###### Agencies <a name="agencies"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| awards_transaction_awarding_agency | Nested Object | An array of <a href="#transaction">transactions</a> for which this agency is the awarding agency |
| awards_transaction_funding_agency | Nested Object | An array of <a href="#transaction">transactions</a> for which this agency is the funding agency |
| id | Integer | Internal primary key. Guaranteed to be unique. |
| create_date | Datetime |  |
| update_date | Datetime |  |
| toptier_agency | Nested Object |  |
| subtier_agency | Nested Object |  |
| office_agency | Nested Object |  |


###### Toptier Agency <a name="agency-toptier"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| toptier_agency_id | Integer | Internal primary key. Guaranteed to be unique. |
| create_date | Datetime |  |
| update_date | Datetime |  |
| cgac_code | String |  |
| fpds_code | String |  |
| abbreviation | String |  |
| name | String |  |


###### Subtier Agency <a name="agency-subtier"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| subtier_agency_id | Integer | Internal primary key. Guaranteed to be unique. |
| create_date | Datetime |  |
| update_date | Datetime |  |
| subtier_code | String |  |
| abbreviation | String |  |
| name | String |  |


###### Agency Office <a name="agency-office"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| office_agency_id | Integer | Internal primary key. Guaranteed to be unique. |
| create_date | Datetime |  |
| update_date | Datetime |  |
| aac_code | String |  |
| name | String |  |

###### CFDA Programs <a name="cfda-programs"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| program_number | String | Internal primary key. Guaranteed to be unique. |
| program_title | String |  |
| popular_name | String |  |
| federal_agency | String |  |
| authorization | String |  |
| objectives | String |  |
| types_of_assistance | String |  |
| uses_and_use_restrictions | String |  |
| applicant_eligibility | String |  |
| beneficiary_eligibility | String |  |
| credentials_documentation | String |  |
| pre_application_coordination | String |  |
| application_procedures | String |  |
| award_procedure | String |  |
| deadlines | String |  |
| range_of_approval_disapproval_time | String |  |
| website_address | String |  |
| formula_and_matching_requirements | String |  |
| length_and_time_phasing_of_assistance | String |  |
| reports | String |  |
| audits | String |  |
| records | String |  |
| account_identification | String |  |
| obligations | String |  |
| range_and_average_of_financial_assistance | String |  |
| appeals | String |  |
| renewals | String |  |
| program_accomplishments | String |  |
| regulations_guidelines_and_literature | String |  |
| regional_or_local_office | String |  |
| headquarters_office | String |  |
| related_programs | String |  |
| examples_of_funded_projects | String |  |
| criteria_for_selecting_proposals | String |  |
| url | String |  |
| recovery | String |  |
| omb_agency_code | String |  |
| omb_bureau_code | String |  |
| published_date | String |  |
| archived_date | String |  |
| create_date | Datetime |  |
| update_date | Datetime |  |

###### Recipients <a name="recipients"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| data_source | String | The source of this entry, either Data Broker (DBR) or USASpending (USA) |
| legal_entity_id | Integer | Internal primary key. Guaranteed to be unique. |
| location | Nested Objects | The <a href="#locations">location</a> of this recipient |
| parent_recipient_unique_id | String |  |
| recipient_name | String |  |
| vendor_doing_as_business_name | String |  |
| vendor_phone_number | String |  |
| vendor_fax_number | String |  |
| business_types | String |  |
| business_types_description | String |  |
| business_categories | Array | An array of strings representing the high level categorization of the recipient. |
| recipient_unique_id | String |  |
| limited_liability_corporation | String |  |
| sole_proprietorship | String |  |
| partnership_or_limited_liability_partnership | String |  |
| subchapter_scorporation | String |  |
| foundation | String |  |
| for_profit_organization | String |  |
| nonprofit_organization | String |  |
| corporate_entity_tax_exempt | String |  |
| corporate_entity_not_tax_exempt | String |  |
| other_not_for_profit_organization | String |  |
| sam_exception | String |  |
| city_local_government | String |  |
| county_local_government | String |  |
| inter_municipal_local_government | String |  |
| local_government_owned | String |  |
| municipality_local_government | String |  |
| school_district_local_government | String |  |
| township_local_government | String |  |
| us_state_government | String |  |
| us_federal_government | String |  |
| federal_agency | String |  |
| federally_funded_research_and_development_corp | String |  |
| us_tribal_government | String |  |
| foreign_government | String |  |
| community_developed_corporation_owned_firm | String |  |
| labor_surplus_area_firm | String |  |
| small_agricultural_cooperative | String |  |
| international_organization | String |  |
| us_government_entity | String |  |
| emerging_small_business | String |  |
| c8a_program_participant | String |  |
| sba_certified_8a_joint_venture | String |  |
| dot_certified_disadvantage | String |  |
| self_certified_small_disadvantaged_business | String |  |
| historically_underutilized_business_zone | String |  |
| small_disadvantaged_business | String |  |
| the_ability_one_program | String |  |
| historically_black_college | String |  |
| c1862_land_grant_college | String |  |
| c1890_land_grant_college | String |  |
| c1994_land_grant_college | String |  |
| minority_institution | String |  |
| private_university_or_college | String |  |
| school_of_forestry | String |  |
| state_controlled_institution_of_higher_learning | String |  |
| tribal_college | String |  |
| veterinary_college | String |  |
| educational_institution | String |  |
| alaskan_native_servicing_institution | String |  |
| community_development_corporation | String |  |
| native_hawaiian_servicing_institution | String |  |
| domestic_shelter | String |  |
| manufacturer_of_goods | String |  |
| hospital_flag | String |  |
| veterinary_hospital | String |  |
| hispanic_servicing_institution | String |  |
| woman_owned_business | String |  |
| minority_owned_business | String |  |
| women_owned_small_business | String |  |
| economically_disadvantaged_women_owned_small_business | String |  |
| joint_venture_women_owned_small_business | String |  |
| joint_venture_economic_disadvantaged_women_owned_small_bus | String |  |
| veteran_owned_business | String |  |
| service_disabled_veteran_owned_business | String |  |
| contracts | String |  |
| grants | String |  |
| receives_contracts_and_grants | String |  |
| airport_authority | String |  |
| council_of_governments | String |  |
| housing_authorities_public_tribal | String |  |
| interstate_entity | String |  |
| planning_commission | String |  |
| port_authority | String |  |
| transit_authority | String |  |
| foreign_owned_and_located | String |  |
| american_indian_owned_business | String |  |
| alaskan_native_owned_corporation_or_firm | String |  |
| indian_tribe_federally_recognized | String |  |
| native_hawaiian_owned_business | String |  |
| tribally_owned_business | String |  |
| asian_pacific_american_owned_business | String |  |
| black_american_owned_business | String |  |
| hispanic_american_owned_business | String |  |
| native_american_owned_business | String |  |
| subcontinent_asian_asian_indian_american_owned_business | String |  |
| other_minority_owned_business | String |  |
| us_local_government | String |  |
| undefinitized_action | String |  |
| domestic_or_foreign_entity | String |  |
| division_name | String |  |
| division_number | String |  |
| last_modified_date | Date |  |
| certified_date | Date |  |
| reporting_period_start | Date |  |
| reporting_period_end | Date |  |
| create_date | Datetime |  |
| update_date | Datetime |  |
| city_township_government | String |  |
| special_district_government | String |  |
| small_business | String |  |
| individual | String |  |

#### Submission Endpoint <a name="submission"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| submission_id | Integer | Internal primary key. Guaranteed to be unique. |
| broker_submission_id | IntegerField |  |
| usaspending_update | Date |  |
| cgac_code | String |  |
| submitting_agency | String |  |
| submitter_name | String |  |
| submission_modification | NullBooleanField |  |
| version_number | IntegerField |  |
| reporting_period_start | Date |  |
| reporting_period_end | Date |  |
| last_modified_date | Date |  |
| certified_date | Date |  |
| create_date | Datetime |  |
| update_date | Datetime |  |

#### Definition <a name="definitions"></a>

| Field | Type | Description |
| ----- | ----- | ----- |
| id | Integer | Internal primary key. Guaranteed to be unique. |
| term | String| The word or phrase in question |
| data_act_term | String| The word or phrase as cited in the DATA Act Schema |
| plain | String| A plain-language definition |
| official | String| Definition from the DATA Act Schema |
