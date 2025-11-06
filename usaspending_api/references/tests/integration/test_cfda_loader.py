import pytest

from django.conf import settings
from django.core.management import call_command
from django.forms.models import model_to_dict

from usaspending_api.references.models import Cfda


@pytest.fixture(scope="function")
def cfda_data(db):
    """Load from small test CSV to test database"""
    file_path = settings.APP_DIR / "references" / "tests" / "data" / "cfda_sample.csv"
    fullpath = f"file://{file_path}"

    call_command("loadcfda", fullpath)


@pytest.mark.django_db
def test_program_number(cfda_data):
    """
    Make sure an instance of a program number is properly created
    """
    actual_result = model_to_dict(
        Cfda.objects.get(program_number="10.054", program_title="Emergency Conservation Program")
    )
    actual_result.pop("id")
    expected_result = {
        "data_source": "USA",
        "program_number": "10.054",
        "program_title": "Emergency Conservation Program",
        "popular_name": "(ECP)",
        "federal_agency": "FARM SERVICE AGENCY, AGRICULTURE, DEPARTMENT OF",
        "authorization": '{"list":[{"act":{"description":"Agricultural Credit Act of 1978"},"publicLaw":{"congressCode":"95","number":"334"},"statute":{"page":"420","volume":"92"},"USC":{"title":"16","section":"2201-2205"},"authorizationTypes":{"USC":true,"act":true,"statute":true,"publicLaw":true,"executiveOrder":false},"usc":{"title":"16","section":"2201-2205"}}]}',
        "objectives": "To enable farmers to perform emergency conservation measures to control wind erosion on farmlands, to rehabilitate farmlands damaged by wind erosion, floods, hurricanes, or other natural disasters and to carry out emergency water conservation or water enhancing measures during periods of severe drought.",
        "types_of_assistance": "DIRECT PAYMENTS FOR SPECIFIED USE",
        "uses_and_use_restrictions": "Not Applicable",
        "applicant_eligibility": "Any agricultural producer who as owner, landlord, tenant, or sharecropper on a farm or ranch, including associated groups, and bears a part of the cost of an approved conservation practice in a disaster area, is eligible to apply for cost-share conservation assistance.  This program is also available in American Samoa, Guam, Commonwealth of the Northern Mariana Islands, Puerto Rico, and the Virgin Islands.",
        "beneficiary_eligibility": "Any agricultural producer who as owner, landlord, tenant, or sharecropper on a farm or ranch, including associated groups, and bears a part of the cost of an approved conservation practice in a disaster area, is eligible to apply for cost-share conservation assistance.  This program is also available in American Samoa, Guam, Commonwealth of the Northern Mariana Islands, Puerto Rico, and the Virgin Islands.",
        "credentials_documentation": '{"description":"Identification as an eligible person and proof of contribution to the cost of performing the conservation practice.  ","isApplicable":true}',
        "pre_application_coordination": "{}",
        "application_procedures": '{"description":"Eligible persons may submit an application on Form AD-245, for cost-sharing, at the county FSA office for the county in which the affected land is located.  ."}',
        "award_procedure": "The county FSA committee reviews, prioritizes, and may approve applications in whole or in part.  Approvals cannot exceed the county allocation of Federal funds for that purpose.",
        "deadlines": '{"flag":"no","list":[]}',
        "range_of_approval_disapproval_time": "From 2 to 3 weeks.",
        "website_address": "http://www.fsa.usda.gov/programs-and-services/conservation-programs/emergency-conservation/index",
        "formula_and_matching_requirements": '{"types":{"moe":false,"formula":false,"matching":false}}',
        "length_and_time_phasing_of_assistance": '{"awarded":"lump","description":"Practice cost-share approvals are given on a fiscal year basis.  The approvals specify the time that the practice must be carried out.  Payment is by check or electronic funds transfer following completion of the measure."}',
        "reports": '[{"code":"program","isSelected":false},{"code":"cash","isSelected":false},{"code":"progress","isSelected":false},{"code":"expenditure","isSelected":false},{"code":"performanceMonitoring","isSelected":false}]',
        "audits": '{"isApplicable":false}',
        "records": "Maintained in the county FSA office and Federal record centers for a specified number of years.",
        "account_identification": "12-3316-0-1-453;",
        "obligations": "(Direct Payments with Unrestricted Use) FY 17$104,312,000.00; FY 18 est $200,000,000.00; FY 19 est $70,000,000.00; FY 16$71,000,000.00; - ",
        "range_and_average_of_financial_assistance": "No Data Available. ",
        "appeals": "Participants may appeal to county FSA committee, State FSA committee, or National Appeals Division (NAD) on any determination. Matters that are generally applicable to all producers are not appealable.",
        "renewals": "Certain approvals may be extended by the FSA county committee, when necessary, with proper justification.",
        "program_accomplishments": '{"list":[],"isApplicable":false}',
        "regulations_guidelines_and_literature": "Program regulations published in the Federal Register at 7 CFR, Part 701.  Program is announced through the news media in the county area designated as a disaster area.",
        "regional_or_local_office": '{"flag":"appendix","description":"Farmers are advised to contact their local county FSA office after a natural disaster has occurred to determine whether the program is available in the county and to determine eligibility for emergency cost-share assistance.  Consult the local telephone directory for location of the county FSA office.  If no listing, get in touch with the appropriate State FSA office listed in the Farm Service Agency section of Appendix IV of the Catalog."}',
        "headquarters_office": "Office Placeholder 8",
        "related_programs": "10.404 Emergency Loans; 10.102 Emergency Forest Restoration Program ; ",
        "examples_of_funded_projects": "Not Applicable.",
        "criteria_for_selecting_proposals": "Not Applicable.",
        "url": "https://beta.sam.gov/fal/7887fed4bf574a8aa5cb3d958587ad7e/view",
        "recovery": '[{"code":"subpartB","isSelected":false},{"code":"subpartC","isSelected":false},{"code":"subpartD","isSelected":false},{"code":"subpartE","isSelected":false},{"code":"subpartF","isSelected":false}]',
        "omb_agency_code": "5",
        "omb_bureau_code": "49",
        "published_date": "Jan 01,1970",
        "archived_date": "",
    }
    assert actual_result == expected_result


@pytest.mark.django_db
def test_alphanumeric_program_number(cfda_data):
    """
    Make alphanumeric program numbers are supported and converted to uppercase
    """
    actual_result = model_to_dict(Cfda.objects.get(program_number="10.ABC", program_title="Sample Alphanumeric"))
    actual_result.pop("id")
    expected_result = {
        "account_identification": "",
        "appeals": "",
        "applicant_eligibility": "",
        "application_procedures": "",
        "archived_date": "",
        "audits": "",
        "authorization": "",
        "award_procedure": "",
        "beneficiary_eligibility": "",
        "credentials_documentation": "",
        "criteria_for_selecting_proposals": "",
        "data_source": "USA",
        "deadlines": "",
        "examples_of_funded_projects": "",
        "federal_agency": "",
        "formula_and_matching_requirements": "",
        "headquarters_office": "",
        "length_and_time_phasing_of_assistance": "",
        "objectives": "",
        "obligations": "",
        "omb_agency_code": "",
        "omb_bureau_code": "",
        "popular_name": "",
        "pre_application_coordination": "",
        "program_accomplishments": "",
        "program_number": "10.ABC",
        "program_title": "Sample Alphanumeric",
        "published_date": "",
        "range_and_average_of_financial_assistance": "",
        "range_of_approval_disapproval_time": "",
        "records": "",
        "recovery": "",
        "regional_or_local_office": "",
        "regulations_guidelines_and_literature": "",
        "related_programs": "",
        "renewals": "",
        "reports": "",
        "types_of_assistance": "",
        "url": "",
        "uses_and_use_restrictions": "",
        "website_address": "",
    }
    assert actual_result == expected_result


@pytest.mark.django_db
def test_account_identification(cfda_data):
    """
    Make sure an account identification is properly mapped to program_number
    """
    actual_result = model_to_dict(Cfda.objects.get(program_number="10.03", account_identification="12-1600-0-1-352;"))
    actual_result.pop("id")
    expected_result = {
        "account_identification": "12-1600-0-1-352;",
        "appeals": "Not Applicable",
        "applicant_eligibility": "N/A",
        "application_procedures": '{"description":"N/A"}',
        "archived_date": "",
        "audits": '{"isApplicable":false}',
        "authorization": '{"list":[{"act":{"description":"Plant Protection Act"},"publicLaw":{"number":"106-224"},"authorizationTypes":{"USC":false,"act":true,"statute":false,"publicLaw":true,"executiveOrder":false}},{"act":{"description":"9 CFR parts 50-54"},"authorizationTypes":{"USC":false,"act":true,"statute":false,"publicLaw":false,"executiveOrder":false}}]}',
        "award_procedure": "Required documentation will be specified in the Declaration of Emergency issued by the Secretary of Agriculture",
        "beneficiary_eligibility": "N/A",
        "credentials_documentation": '{"description":"Required documentation will be specified in the Declaration of Emergency issued by the Secretary of Agriculture","isApplicable":true}',
        "criteria_for_selecting_proposals": "Not Applicable.",
        "data_source": "USA",
        "deadlines": '{"flag":"no","list":[]}',
        "examples_of_funded_projects": "Not Applicable.",
        "federal_agency": "ANIMAL AND PLANT HEALTH INSPECTION SERVICE, AGRICULTURE, DEPARTMENT OF",
        "formula_and_matching_requirements": '{"types":{"moe":false,"formula":false,"matching":false}}',
        "headquarters_office": "Office Placeholder 5",
        "length_and_time_phasing_of_assistance": '{"awarded":"other","description":"Time period of availability will be specified in the Declaration of Emergency issued by the Secretary of Agriculture","awardedDescription":"Electronic Funds Transfer or paper check"}',
        "objectives": "Animal and Plant Health Inspection Service administers regulations at 9 CFR parts 50 to 54 that authorizes payment for indemnities.  This authority covers a wide variety of indemnity situations ranging from large livestock depopulations to small fowl depopulations, and there are various indemnity calculations and processes for determining the indemnity value for each specific species.  The Secretary of Agriculture offers an opinion that constitutes an emergency and threatens the U.S. animal population.  Payment for the destroyed animals is based on fair market value.  Also, under Section 415 (e) of the Plant Protection Act (Title IV of Public Law 106-224), under a declaration of extraordinary emergency because of the presence of a plant pest or noxious weed that is new to or not known to be widely prevalent in the United States, the Secretary may pay compensation for economic losses incurred by as a result of actions taken under the authorities in this section (415).\r\n",
        "obligations": "(Direct Payments with Unrestricted Use) FY 17$6,794,055.00; FY 18 est $5,988,018.00; FY 19 est $6,560,683.00; FY 16$30,472,730.00; - FY2018 estimate was made using FY2017 activities.  Data was reviewed from FY2015-FY2017 and there was no identifiable trend/pattern.  Also FY2015 and FY2016 had unforeseen outbreaks that cannot be anticipated for future years.",
        "omb_agency_code": "5",
        "omb_bureau_code": "32",
        "popular_name": "",
        "pre_application_coordination": "{}",
        "program_accomplishments": '{"list":[],"isApplicable":false}',
        "program_number": "10.03",
        "program_title": "Indemnity Program",
        "published_date": "Mar 16,2012",
        "range_and_average_of_financial_assistance": "No Data Available. ",
        "range_of_approval_disapproval_time": "Not Applicable",
        "records": "Record requirements will be specified in the Declaration of Emergency issued by the Secretary of Agriculture",
        "recovery": '[{"code":"subpartB","isSelected":false},{"code":"subpartC","isSelected":false},{"code":"subpartD","isSelected":false},{"code":"subpartE","isSelected":false},{"code":"subpartF","isSelected":false}]',
        "regional_or_local_office": '{"flag":"appendix"}',
        "regulations_guidelines_and_literature": "Not Applicable.",
        "related_programs": "Not Applicable.",
        "renewals": "Not Applicable",
        "reports": '[{"code":"program","isSelected":false},{"code":"cash","isSelected":false},{"code":"progress","isSelected":false},{"code":"expenditure","isSelected":false},{"code":"performanceMonitoring","isSelected":false}]',
        "types_of_assistance": "DIRECT PAYMENTS WITH UNRESTRICTED USE",
        "url": "https://beta.sam.gov/fal/a984be7ef3964d3db6180cf6a9213275/view",
        "uses_and_use_restrictions": "Not Applicable",
        "website_address": "Not Applicable",
    }
    assert actual_result == expected_result
