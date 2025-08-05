"""
This is a map of code enumerations, extracted from the GSDM

Each should be named following this pattern:
    - For a field that is unique among all models, <field>_map
    - For a field that is not unique among models, <model_name>.<field>_map

Example 1: For the "action_type_description" field to be automatically updated, we create a map called
"action_type_map" because that field is unique among all models.

Example 2: For the "type_description" field to be automatically updated, we need to uniquely identify the maps on a
per-model basis as this field exists on multiple models, but cannot share enumeration maps. Thus, we will make two maps:
"Award.type_map" and "TransactionNormalized.type_map".

Each map has a key of enumerable values, paired with the description field for that value.

There are a few special keys:
    _DEFAULT - A default value to use, if the incoming value is NoneType
    _BLANK - A value to use if the incoming value is blank

CASES:
If you have a need to specify a different mapping based upon other fields on the same model, you can do so by specify
cases. These must be numbered from 1, and store conditions in the "case_<INT>" and the mappings in "case_<INT>_map".
For an example, see TransactionNormalized.action_type_map
"""

change_reasons = {
    "A": "Additional Work (new agreement, FAR part 6 applies)",
    "B": "Supplemental Agreement for work within scope",
    "C": "Funding Only Action",
    "D": "Change Order",
    "E": "Terminate for Default (complete or partial)",
    "F": "Terminate for Convenience (complete or partial)",
    "G": "Exercise an Option",
    "H": "Definitize Letter Contract",
    "J": "Novation Agreement",
    "K": "Close Out",
    "L": "Definitize Change Order",
    "M": "Other Administrative Action",
    "N": "Legal Contract Cancellation",
    "P": "Representation of Non-Novated Merger/Acquisition",
    "R": "Representation",
    "S": "Change PIID",
    "T": "Transfer Action",
    "V": "Vendor DUNS or Name Change - Non-Novation",
    "W": "Vendor Address Change",
    "X": "Terminate for Cause",
}

gsdm_maps = {
    # D1, D2 GSDM MAPS - These are found in awards.models unless noted
    # This map is an example of how to have TWO different maps for the same field, on the same object, separated by a
    # case.
    "TransactionNormalized.action_type_map": {
        # When we are financial assistance
        "case_1": {"assistance_data__isnull": False, "contract_data__isnull": True},
        "case_1_map": {
            "A": "New Assistance Award",
            "B": "Continuation",
            "C": "Revision",
            "D": "Funding adjustment to completed project",
        },
        # When we are a contract
        "case_2": {"assistance_data__isnull": True, "contract_data__isnull": False},
        "case_2_map": {**change_reasons},
    },
    "business_funds_indicator_map": {
        "REC": "Funds are provided by the Recovery Act",
        "NON": "Funded by other (non-Recovery Act) sources",
    },
    "business_types_map": {
        "A": "State government",
        "B": "County Government",
        "C": "City or Township Government",
        "D": "Special District Government",
        "E": "Regional Organization",
        "F": "U.S. Territory or Possession",
        "G": "Independent School District",
        "H": "Public/State Controlled Institution of Higher Education",
        "I": "Indian/Native American Tribal Government (Federally Recognized)",
        "J": "Indian/Native American Tribal Government (Other than Federally Recognized)",
        "K": "Indian/Native American Tribal Designated Organization",
        "L": "Public/Indian Housing Authority",
        "M": "Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)",
        "N": "Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)",
        "O": "Private Institution of Higher Education",
        "P": "Individual",
        "Q": "For-Profit Organization (Other than Small Business)",
        "R": "Small Business",
        "S": "Hispanic-serving Institution",
        "T": "Historically Black Colleges and Universities (HBCUs)",
        "U": "Tribally Controlled Colleges and Universities (TCCUs)",
        "V": "Alaska Native and Native Hawaiian Serving Institutions",
        "W": "Non-domestic (non-US) Entity",
        "X": "Other",
        "_DEFAULT": "Unknown Types",
    },
    "small_business_map": {"S": "Small Business", "O": "Other Than Small Business"},
    "commercial_item_acquisition_procedures_map": {
        "A": "Commercial Item",
        "B": "Supplies or services pursuant to FAR 12.102(f)",
        "C": "Services pursuant to FAR 12.102(g)",
        "D": "Commercial Item - Procedure Not Used",
    },
    "contingency_humanitarian_or_peacekeeping_operation_map": {
        "A": "Contingency operation as defined in 10 U.S.C. 101(a)(13)",
        "B": "Humanitarian or peacekeeping operation as defined in 10 U.S.C. 2302(8)",
        "X": "Not Applicable",
    },
    "contract_bundling_map": {
        "A": "Mission Critical",
        "B": "OMB Circular A-76",
        "C": "Other",
        "D": "Not a Bundled Requirement",
    },
    "contract_financing_map": {
        "A": "FAR 52.232-16 Progress Payments",
        "C": "Percentage of Completion Progress Payments",
        "D": "Unusual Progress Payments or Advance Payments",
        "E": "Commercial Financing",
        "F": "Performance-Based Financing",
        "Z": "Not Applicable",
    },
    "correction_delete_indicator_map": {
        "C": "Current transaction record is a request to replace a previously submitted record that contained "
        "data submission errors. Record should contain replacement (not delta) values for all data fields "
        "that contain submission errors.",
        "D": "Current transaction record is a request to delete a previously submitted record that contained data "
        "submission errors.",
        "_BLANK": "Request to add the current transaction record.",
        "L": "Agencies can continue to send transaction records with this code based on previous FAADS guidance, "
        "however it will be ignored for USAspending.gov. Instead, the ‘Obligation/Action Date’ field will be used "
        "to determine the fiscal year in which the transaction will be reported. ",
    },
    "cost_accounting_standards_map": {"Y": "Yes", "N": "No", "X": "Not Applicable"},
    "cost_or_pricing_data_map": {"N": "No", "W": "Not Obtained - Waived", "Y": "Yes"},
    "construction_wage_rate_req_map": {"Y": "Yes", "N": "No", "X": "Not Applicable"},
    "domestic_or_foreign_entity_map": {
        "A": "U.S. Owned Business",
        "B": "Other U.S. Entity (e.g. Government)",
        "C": "Foreign-Owned Business Incorporated in the U.S.",
        "D": "Foreign-Owned Business Not Incorporated in the U.S.",
        "O": "Other Foreign Entity (e.g. Foreign Government)",
    },
    "epa_designated_product_map": {
        "A": "Meets Requirements",
        "B": "Justification - Time",
        "C": "Justification - Price",
        "D": "Justification - Performance",
        "E": "Not Required",
    },
    "evaluated_preference_map": {
        "NONE": "No Preference Used",
        "SDA": "SDB Price Evaluation Adjustment",
        "SPS": "SDB Preferential Consideration Partial SB Set Aside",
        "HZE": "HUBZone Price Evaluation Preference",
        "HSD": "Combined HUB/SDB Preference",
    },
    "extent_competed_map": {
        "A": "Full and Open Competition",
        "B": "Not Available for Competition",
        "C": "Not Competed",
        "D": "Full and Open Competition after exclusion of sources",
        "E Civ": "Follow On to Competed Action",
        "F": "Competed under SAP",
        "G": "Not Competed under SAP",
        "CDOCiv": "Competitive Delivery Order",
        "NDOCiv": "	Non-Competitive Delivery Order",
    },
    "fed_biz_opps_map": {"Y": "Yes", "N": "No", "X": "Not Applicable"},
    "foreign_funding_map": {"A": "Foreign Funds", "B": "Foreign Funds non-FMS", "X": "Not Applicable"},
    "idv_type_map": {
        "A": "Governmentwide Acquisition Contract (GWAC)",
        "B": "Indefinite Delivery Contract (IDC)",
        "C": "Federal Supply Schedule (FSS)",
        "D": "Basic Ordering Agreement (BOA)",
        "E": "Blanket Purchase Agreement (BPA)",
    },
    "information_technology_commercial_item_category_map": {
        "A": "Commercially Available",
        "B": "Other Commercial Item",
        "C": "Non-Developmental Item",
        "D": "Non-Commercial Item",
        "E": "Commercial Service",
        "F": "Non-Commercial Service",
        "Z": "Not IT Products or Services",
    },
    "interagency_contracting_authority_map": {
        "A": "Economy Act",
        "B": "Other Statutory Authority",
        "X": "Not Applicable",
    },
    "multiple_or_single_award_i_map": {"M": "Multiple Award", "S": "Single Award"},
    "national_interest_action_map": {
        "NONE": "None",
        "H05K": "Hurricane Katrina 2005",
        "H05O": "Hurricane Ophelia 2005",
        "H05R": "Hurricane Rita 2005",
        "H05W": "Hurricane Wilma 2005",
        "W081": "California Wildfires 2008",
        "F081": "Midwest Storms and Flooding 2008",
        "H08G": "Hurricane Gustav 2008",
        "H08H": "Hurricane Hanna 2008",
        "H08I": "Hurricane Ike 2008",
        "H06C": "Hurricane Chris 2006",
        "H06E": "Hurricane Ernesto 2006",
        "Inauguration 2009": "Inauguration Declaration 2009",
        "T10S": "American Samoa Earthquake, Tsunami and Flooding 2010",
        "Q10H": "Haiti Earthquake 2010",
        "H10E": "Hurricane Earl 2010",
        "O10G": "Gulf Oil Spill 0410",
        "H11I": "Hurricane Irene 2011",
        "Q11J": "Pacific Earthquake/Tsunami 2011",
        "O12F": "Operation Enduring Freedom (OEF)",
        "H12I": "Hurricane Isaac 2012",
        "H13S": "Hurricane Sandy 2013",
        "T13O": "Oklahoma Tornado and Storm 2013",
        "O14S": "Operations in Iraq and Syria",
        "O14E": "Operation United Assistance (OUA) - Ebola Outbreak West Africa",
        "O15F": "Operation Freedom's Sentinel (OFS) 2015",
    },
    "performance_based_service_acquisition_map": {"Y": "Yes", "N": "No", "X": "Not Applicable"},
    "place_of_manufacture_map": {
        "A": "Performed or Manufactured in US, but services performed by a foreign concern or more than 50% foreign "
        "content ",
        "B": "Performed or Manufactured outside US",
        "C": "Not a manufactured end product",
        "D": "Manufactured in U.S.",
        "E": "Manufactured outside U.S.",
        "F": "Manufactured outside U.S. - Resale",
        "G": "Manufactured outside U.S. - Trade Agreements",
        "H": "Manufactured outside U.S. - Commercial Information Technology",
        "I": "Manufactured outside U.S. - Public Interest Determination",
        "J": "Manufactured outside U.S. - Domestic Non-Availability",
        "K": "Manufactured outside U.S. - Unreasonable Cost",
        "L": "Manufactured outside U.S. - Qualifying County (DoD Only)",
    },
    # There is currently no field for this at the moment
    "reason_for_modification_map": {**change_reasons},
    # No field current exists for this
    "not_competed_reason_map": {
        "UNQ": "Unique Source (FAR 6.302-1(b)(1)) ",
        "FOC": "Follow-On Contract (FAR 6.302-1(a)(2)(ii/iii))",
        "UR": "Unsolicited Research Proposal (FAR 6.302-1(a)(2)(i))",
        "PDR": "Patent or Data Rights (FAR 6.302-1(b)(2))",
        "UT": "Utilities (FAR 6.302-1(b)(3))",
        "STD": "Standardization (FAR 6.302-1(b)(4))",
        "ONE": "Only One Source-Other (FAR 6.302-1 other)",
        "URG": "Urgency (FAR 6.302-2)",
        "MES": "Mobilization, Essential R&D (FAR 6.302-3)",
        "IA": "International Agreement (FAR 6.302-4)",
        "OTH": "Authorized by Statute (FAR 6.302-5(a)(2)(i))",
        "RES": "Authorized Resale (FAR 6.302-5(a)(2)(ii))",
        "NS": "National Security (FAR 6.302-6)",
        "PI": "Public Interest (FAR 6.302-7)",
        "MPTCiv": "Less than or equal to the Micro-Purchase Threshold",
        "SP2": "SAP Non-Competition (FAR 13)",
        "BND": "Brand Name Description (FAR 6.302-1(c))",
    },
    "record_type_map": {"1": "Action amount has been aggregated", "2": "Not aggregated Action by Action amounts"},
    "recovered_materials_sustainability_map": {
        "A": "FAR 52.223-4 Included - The solicitation included the provision at FAR 52.223-4, Recovered Material "
        "Certification.",
        "B": "FAR 52.223-4 and FAR 52.223-9 Included - The solicitation included the provision at FAR 52.223-4, "
        "Recovered Material Certification and the contract includes the clause at FAR 52.223-9, Estimate of "
        "Percentage of Recovered Material Content for EPA-Designated Products.",
        "C": "No Clauses Included and No Sustainability Included",
        "D": "Energy efficient",
        "E": "Bio-based",
        "F": "Environmentally preferable",
        "G": "FAR 52.223-4 & energy efficient",
        "H": "FAR 52.223-4 & bio-based",
        "I": "FAR 52.223-4 & environmentally preferable",
        "J": "FAR 52.223-4 & bio-based & energy efficient",
        "K": "FAR 52.223-4 & bio-based & environmentally preferable",
        "L": "FAR 52.223-4 & bio-based & energy efficient & environmentally preferable",
    },
    "sea_transportation_map": {"Y": "Yes", "N": "No", "U": "Unknown", "_BLANK": "Not Applicable"},
    "labor_standards_map": {"Y": "Yes", "N": "No", "X": "Not Applicable"},
    "research_map": {
        "SR1": "SBIR Program Phase I Action",
        "SR2": "SBIR Program Phase II Action",
        "SR3": "SBIR Program Phase III Action",
        "ST1": "STTR Phase I",
        "ST2": "STTR Phase II",
        "ST3": "STTR Phase III",
    },
    "solicitation_procedures_map": {
        "NP": "Negotiated Proposal/Quote",
        "SB": "Sealed Bid",
        "TS": "Two Step",
        "SP1": "Simplified Acquisition",
        "AE": "Architect-Engineer FAR 6.102",
        "BR": "Basic Research",
        "AS": "Alternative Sources",
        "SSS": "Only One Source",
        "MAFO": "Subject to Multiple Award Fair opportunity",
    },
    "fair_opportunity_limited_sources_map": {
        "URG": "Urgency",
        "ONE": "Only One Source - Other",
        "FOO": "Follow-on Action Following Competitive Initial Action",
        "MG": "Minimum Guarantee",
        "OSA": "Other Statutory Authority",
        "FAIR": "Fair Opportunity Given",
        "CSA": "Competitive Set Aside",
        "SSS": "Sole Source",
    },
    "subcontracting_plan_map": {
        "A": "Plan Not Included - No Subcontracting Possibilities",
        "B": "Plan Not Required",
        "C": "Plan Required - Incentive Not Included",
        "D": "Plan Required - Incentive Included",
        "E": "Plan Required (Pre 2004)",
        "F": "Individual Subcontract Plan",
        "G": "Commercial Subcontract Plan",
        "H": "DOD Comprehensive Subcontract Plan",
    },
    "type_of_idc_map": {
        "A": "Indefinite Delivery / Requirements",
        "B": "Indefinite Delivery / Indefinite Quantity",
        "C": "Indefinite Delivery / Definite Quantity",
    },
    "type_set_aside_map": {
        "NONE": "No Set Aside Used",
        "SBA": "Small Business Set-Aside - Total",
        "8A": "8A Competed",
        "SBP": "Small Business Set-Aside - Partial",
        "HMT": "HBCU or MI Set-Aside - Total",
        "HMP": "HBCU or MI Set-Aside - Partial",
        "VSBCiv": "Very Small Business Set-Aside",
        "ESB": "Emerging Small Business Set-Aside",
        "HZC": "HUBZone Set-Aside",
        "SDVOSBC": "Service-Disabled Veteran-Owned Small Business Set-Aside",
        "BICiv": "Buy Indian",
        "ISEE": "Indian Economic Enterprise",
        "ISBEE": "Indian Small Business Economic Enterprise",
        "HZS": "HUBZone Sole Source",
        "SDVOSBS": "SDVOSB Sole Source",
        "8AN": "8(a) Sole Source",
        "RSBCiv": "Reserved for Small Business $2,501 to $100K",
        "8ACCiv": "SDB Set-Aside 8(a)",
        "HS2Civ": "Combination HUBZone and 8(a)",
        "HS3": "8(a) with HUBZone Preference",
        "VSA": "Veteran Set-Aside",
        "VSS": "Veteran Sole Source",
        "WOSB": "Women-Owned Small Business",
        "EDWOSB": "Economically-Disadvantaged Women-Owned Small Business",
    },
    "type_of_contract_pricing_map": {
        "A": "Fixed Price Redetermination",
        "B": "Fixed Price Level of Effort",
        "J": "Firm Fixed Price",
        "K": "Fixed Price with Economic Price Adjustment",
        "L": "Fixed Price Incentive",
        "M": "Fixed Price Award Fee",
        "R": "Cost Plus Award Fee",
        "S": "Cost No Fee",
        "T": "Cost Sharing",
        "U": "Cost Plus Fixed Fee",
        "V": "Cost Plus Incentive Fee",
        "Y": "Time and Materials",
        "Z": "Labor Hours",
        "1": "Order Dependent",
        "2": "Combination",
        "3": "Other",
        "_DEFAULT": "Unknown Type",
    },
    # Used on both Award and Transaction, but the mapping is the same
    "type_map": {
        "02": "Block Grant",
        "03": "Formula Grant",
        "04": "Project Grant",
        "05": "Cooperative Agreement",
        "06": "Direct Payment for Specified Use",
        "07": "Direct Loan",
        "08": "Guaranteed/Insured Loan",
        "09": "Insurance",
        "10": "Direct Payment with Unrestricted Use",
        "11": "Other Financial Assistance",
        "A": "BPA Call",
        "B": "Purchase Order",
        "C": "Delivery Order",
        "D": "Definitive Contract",
        # Custom NASA Types
        "F": "Cooperative Agreement",
        "G": "Grant for Research",
        "O": "Other Transaction Order",
        "R": "Other Transaction Agreement",
        "S": "Funded Space Act Agreement",
        "T": "Training Grant",
        # End custom NASA types
        "_DEFAULT": "Unknown Type",
    },
    # File A, B, C fields
    "availability_type_code_map": {
        "X": "Appropriation account has an unlimited period to incur new obligations.",
        "_BLANK": "Appropriation account has a designated period of availability.",
    },
}
