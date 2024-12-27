import pytest
import datetime
import zipfile
import os

from django.core.management import call_command
from csv import reader
from model_bakery import baker
from unittest.mock import Mock

from usaspending_api import settings
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.download.v2.download_column_historical_lookups import query_paths

# Make sure UTC or test will fail later in the day
TODAY = datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y%m%d")


def generate_contract_data(fiscal_year, i):
    return [
        i,
        i,
        f"piid{i}",
        ""  # "contract_transaction_unique_key", "contract_award_unique_key", "award_id_piid", "modification_number",
        "",
        "",
        "",
        "",  # "transaction_number", "parent_award_agency_id", "parent_award_agency_name", "parent_award_id_piid",
        "",
        "",
        "",  # "parent_award_modification_number", "federal_action_obligation", "total_dollars_obligated",
        "",
        "",
        "",  # "base_and_exercised_options_value", "current_total_value_of_award", "base_and_all_options_value",
        "",
        "",
        "",  # "potential_total_value_of_award", "disaster_emergency_fund_codes_for_overall_award",
        "",
        "",  # "outlayed_amount_from_COVID-19_supplementals_for_overall_award", "obligated_amount_from_COVID-19_supplementals_for_overall_award",
        "",
        "",  # "outlayed_amount_from_IIJA_supplemental_for_overall_award", "obligated_amount_from_IIJA_supplemental_for_overall_award",
        "",
        "",  # "action_date", "action_date_fiscal_year",
        f"05/07{fiscal_year}",
        f"05/07/{fiscal_year}",
        "",  # "period_of_performance_start_date", "period_of_performance_current_end_date", "period_of_performance_potential_end_date",
        "",
        "",
        "001",
        "Test_Agency",
        "001",  # "ordering_period_end_date", "solicitation_date", "awarding_agency_code", "awarding_toptier_agency_abbreviation", "awarding_sub_agency_code",
        "Test_Agency",
        "",
        "",
        "",
        "",  # "awarding_sub_agency_name", "awarding_office_code", "awarding_office_name", "funding_agency_code", "funding_agency_name",
        "",
        "",
        "",
        "",  # "funding_sub_agency_code", "funding_sub_agency_name", "funding_office_code", "funding_office_name",
        "",
        "",
        "",  # "treasury_accounts_funding_this_award", "federal_accounts_funding_this_award", "object_classes_funding_this_award",
        "",
        "",
        "",
        "",  # "program_activities_funding_this_award", "foreign_funding", "foreign_funding_description", "sam_exception",
        "",
        "",
        "",
        "",
        "",  # "sam_exception_description", "recipient_duns", "recipient_name", "recipient_doing_business_as_name", "cage_code",
        "",
        "",
        "",
        "",  # "recipient_parent_duns", "recipient_parent_name", "recipient_country_code", "recipient_country_name",
        "",
        "",
        "",
        "",  # "recipient_address_line_1", "recipient_address_line_2", "recipient_city_name", "recipient_county_name",
        "",
        "",
        "",
        "",  # "recipient_state_code", "recipient_state_name", "recipient_zip_4_code", "recipient_congressional_district",
        "",
        "",
        "",
        "",  # "recipient_phone_number", "recipient_fax_number", "primary_place_of_performance_country_code",
        "",
        "",  # "primary_place_of_performance_country_name", "primary_place_of_performance_city_name",
        "",
        "",  # "primary_place_of_performance_county_name", "primary_place_of_performance_state_code",
        "",
        "",  # "primary_place_of_performance_state_name", "primary_place_of_performance_zip_4",
        "",
        "",
        "",
        "",  # "primary_place_of_performance_congressional_district", "award_or_idv_flag", "award_type_code", "award_type",
        "",
        "",
        "",
        "",
        "",
        "",  # "idv_type_code", "idv_type", "multiple_or_single_award_idv_code", "multiple_or_single_award_idv", "type_of_idc_code",
        "",
        "",
        "",
        "",
        "",  # "type_of_idc", "type_of_contract_pricing_code", "type_of_contract_pricing", "award_description", "action_type_code",
        "",
        "",
        "",
        "",  # "action_type", "solicitation_identifier", "number_of_actions", "inherently_governmental_functions",
        "",
        "",
        "",  # "inherently_governmental_functions_description", "product_or_service_code", "product_or_service_code_description",
        "",
        "",
        "",
        "",  # "contract_bundling_code", "contract_bundling ", "dod_claimant_program_code", "dod_claimant_program_description",
        "",
        "",
        "",
        "",  # "naics_code", "naics_description", "recovered_materials_sustainability_code", "recovered_materials_sustainability",
        "",
        "",
        "",  # "domestic_or_foreign_entity_code", "domestic_or_foreign_entity", "dod_acquisition_program_code",
        "",
        "",  # "dod_acquisition_program_description", "information_technology_commercial_item_category_code",
        "",
        "",
        "",  # "information_technology_commercial_item_category ", "epa_designated_product_code", "epa_designated_product",
        "",
        "",
        "",  # "country_of_product_or_service_origin_code","country_of_product_or_service_origin", "place_of_manufacture_code ",
        "",
        "",
        "",
        "",
        "",  # place_of_manufacture", "subcontracting_plan_code", "subcontracting_plan", "extent_competed_code", "extent_competed",
        "",
        "",
        "",
        "",  # "solicitation_procedures_code", "solicitation_procedures", "type_of_set_aside_code", "type_of_set_aside",
        "",
        "",
        "",
        "",
        "",  # "evaluated_preference_code", "evaluated_preference", "research_code", "research", "fair_opportunity_limited_sources_code",
        "",
        "",
        "",  # "fair_opportunity_limited_sources", "other_than_full_and_open_competition_code", "other_than_full_and_open_competition",
        "",
        "",
        "",  # "number_of_offers_received", "commercial_item_acquisition_procedures_code", "commercial_item_acquisition_procedures",
        "",
        "",  # "small_business_competitiveness_demonstration_program", "simplified_procedures_for_certain_commercial_items_code",
        "",
        "",
        "",
        "",  # "simplified_procedures_for_certain_commercial_items", "a76_fair_act_action_code", "a76_fair_act_action", "fed_biz_opps_code",
        "",
        "",
        "",
        "",  # "fed_biz_opps", "local_area_set_aside_code", "local_area_set_aside", "price_evaluation_adjustment_preference_percent_difference",
        "",
        "",
        "",  # "clinger_cohen_act_planning_code", "clinger_cohen_act_planning", "materials_supplies_articles_equipment_code",
        "",
        "",
        "",
        "",  # "materials_supplies_articles_equipment", "labor_standards_code", "labor_standards", "construction_wage_rate_requirements_code",
        "",
        "",
        "",  # "construction_wage_rate_requirements", "interagency_contracting_authority_code", "interagency_contracting_authority",
        "",
        "",
        "",
        "",
        "",  # "other_statutory_authority", "program_acronym", "parent_award_type_code", "parent_award_type", "parent_award_single_or_multiple_code",
        "",
        "",
        "",
        "",  # "parent_award_single_or_multiple", "major_program", "national_interest_action_code", "national_interest_action",
        "",
        "",
        "",
        "",  # "cost_or_pricing_data_code", "cost_or_pricing_data", "cost_accounting_standards_clause_code", "cost_accounting_standards_clause",
        "",
        "",
        "",
        "",  # "government_furnished_property_code", "government_furnished_property", "sea_transportation_code", "sea_transportation",
        "",
        "",
        "",
        "",  # "undefinitized_action_code", "undefinitized_action", "consolidated_contract_code", "consolidated_contract",
        "",
        "",
        "",  # "performance_based_service_acquisition_code", "performance_based_service_acquisition", "multi_year_contract_code",
        "",
        "",
        "",
        "",  # "multi_year_contract", "contract_financing_code", "contract_financing", "purchase_card_as_payment_method_code",
        "",
        "",  # "purchase_card_as_payment_method", "contingency_humanitarian_or_peacekeeping_operation_code",
        "",
        "",  # "contingency_humanitarian_or_peacekeeping_operation", "alaskan_native_corporation_owned_firm",
        "",
        "",
        "",  # "american_indian_owned_business", "indian_tribe_federally_recognized", "native_hawaiian_organization_owned_firm",
        "",
        "",
        "",
        "",  # "tribally_owned_firm", "veteran_owned_business", "service_disabled_veteran_owned_business", "woman_owned_business",
        "",
        "",
        "",  # "women_owned_small_business", "economically_disadvantaged_women_owned_small_business", "joint_venture_women_owned_small_business",
        "",
        "",
        "",  # "joint_venture_economic_disadvantaged_women_owned_small_bus", "minority_owned_business", "subcontinent_asian_asian_indian_american_owned_business",
        "",
        "",
        "",
        "",  # "asian_pacific_american_owned_business", "black_american_owned_business", "hispanic_american_owned_business", "native_american_owned_business",
        "",
        "",
        "",  # "other_minority_owned_business", "contracting_officers_determination_of_business_size", "contracting_officers_determination_of_business_size_code",
        "",
        "",
        "",
        "",  # "emerging_small_business", "community_developed_corporation_owned_firm", "labor_surplus_area_firm", "us_federal_government",
        "",
        "",
        "",
        "",
        "",  # "federally_funded_research_and_development_corp", "federal_agency", "us_state_government", "us_local_government", "city_local_government",
        "",
        "",
        "",
        "",  # "county_local_government", "inter_municipal_local_government", "local_government_owned", "municipality_local_government",
        "",
        "",
        "",
        "",  # "school_district_local_government ", "township_local_government", "us_tribal_government ", "foreign_government",
        "",
        "",
        "",
        "",
        "",  # "organizational_type", "corporate_entity_not_tax_exempt", "corporate_entity_tax_exempt", "partnership_or_limited_liability_partnership",
        "",
        "",
        "",
        "",  # "sole_proprietorship", "small_agricultural_cooperative", "international_organization", "us_government_entity",
        "",
        "",
        "",
        "",
        "",  # "community_development_corporation", "domestic_shelter", "educational_institution", "foundation", "hospital_flag",
        "",
        "",
        "",
        "",
        "",  # "manufacturer_of_goods", "veterinary_hospital", "hispanic_servicing_institution", "receives_contracts", "receives_financial_assistance",
        "",
        "",
        "",
        "",  # "receives_contracts_and_financial_assistance ", "airport_authority", "council_of_governments", "housing_authorities_public_tribal",
        "",
        "",
        "",
        "",
        "",  # "interstate_entity", "planning_commission", "port_authority", "transit_authority", "subchapter_scorporation",
        "",
        "",
        "",
        "",  # "limited_liability_corporation ", "foreign_owned", "for_profit_organization", "nonprofit_organization",
        "",
        "",
        "",
        "",  # "other_not_for_profit_organization", "the_ability_one_program", "private_university_or_college ", "state_controlled_institution_of_higher_learning",
        "",
        "",
        "",
        "",
        "",  # "1862_land_grant_college", "1890_land_grant_college", "1994_land_grant_college", "minority_institution", "historically_black_college ",
        "",
        "",
        "",
        "",  # "tribal_college", "alaskan_native_servicing_institution", "native_hawaiian_servicing_institution", "school_of_forestry",
        "",
        "",
        "",
        "",  # "veterinary_college", "dot_certified_disadvantage", "self_certified_small_disadvantaged_business", "small_disadvantaged_business",
        "",
        "",
        "",  # "c8a_program_participant", "historically_underutilized_business_zone_hubzone_firm", "sba_certified_8a_joint_venture",
        "",
        "",
        "",  # "highly_compensated_officer_1_name", "highly_compensated_officer_1_amount", "highly_compensated_officer_2_name",
        "",
        "",
        "",  # "highly_compensated_officer_2_amount", "highly_compensated_officer_3_name", "highly_compensated_officer_3_amount",
        "",
        "",
        "",  # "highly_compensated_officer_4_name", "highly_compensated_officer_4_amount", "highly_compensated_officer_5_name",
        "",
        f"www.usaspending.gov/award/CONT_AWD_{i}_0_0",
        f"05/07/{fiscal_year}",  # "highly_compensated_officer_5_amount", "usaspending_permalink", "last_modified_date"
    ]


def generate_assistance_data(fiscal_year, i):
    return [
        i + 100,
        i + 100,
        f"fain{i+100}",
        "",  # "assistance_transaction_unique_key", "assistance_award_unique_key", "award_id_fain", "modification_number",
        "",
        "",
        "",
        "",
        "",  # "award_id_uri", "sai_number", "federal_action_obligation", "total_obligated_amount", "non_federal_funding_amount",
        "",
        "",
        "",
        "",  # "total_non_federal_funding_amount", "face_value_of_loan", "original_loan_subsidy_cost", "total_face_value_of_loan",
        "",
        "",  # "total_loan_subsidy_cost", "disaster_emergency_fund_codes_for_overall_award",
        "",
        "",
        "",  # "outlayed_amount_from_COVID-19_supplementals_for_overall_award", "obligated_amount_from_COVID-19_supplementals_for_overall_award",
        "",
        "",  # "outlayed_amount_from_IIJA_supplemental_for_overall_award", "obligated_amount_from_IIJA_supplemental_for_overall_award",
        "",
        "",  # "action_date",
        "",  # "action_date_fiscal_year", "period_of_performance_start_date", "period_of_performance_current_end_date",
        "001",
        "Test_Agency",
        "001",
        "Test_Agency",  # "awarding_agency_code", "awarding_toptier_agency_abbreviation", "awarding_sub_agency_code", "awarding_sub_agency_name",
        "",
        "",
        "",
        "",  # "awarding_office_code", "awarding_office_name", "funding_agency_code", "funding_agency_name",
        "",
        "",
        "",
        "",  # "funding_sub_agency_code", "funding_sub_agency_name", "funding_office_code", "funding_office_name",
        "",
        "",  # "treasury_accounts_funding_this_award", "federal_accounts_funding_this_award",
        "",
        "",  # "object_classes_funding_this_award", "recipient_duns", "recipient_name",
        "",
        "",
        "",
        "",  # "recipient_parent_duns", "recipient_parent_name", "recipient_country_code", "recipient_country_name",
        "",
        "",
        "",
        "",  # "recipient_address_line_1", "recipient_address_line_2", "recipient_city_code", "recipient_city_name",
        "",
        "",
        "",
        "",  # "recipient_county_code", "recipient_county_name", "recipient_state_code", "recipient_state_name",
        "",
        "",
        "",
        "",  # "recipient_zip_code", "recipient_zip_last_4_code", "recipient_congressional_district", "recipient_foreign_city_name",
        "",
        "",
        "",
        "",
        "",  # "recipient_foreign_province_name", "recipient_foreign_postal_code", "primary_place_of_performance_scope",
        "",
        "",  # "primary_place_of_performance_country_code", "primary_place_of_performance_country_name",
        "",
        "",  # "primary_place_of_performance_code", "primary_place_of_performance_city_name",
        "",
        "",  # "primary_place_of_performance_county_code", "primary_place_of_performance_county_name",
        "",
        "",  # "primary_place_of_performance_state_name", "primary_place_of_performance_zip_4",
        "",
        "",
        "",  # "primary_place_of_performance_congressional_district","primary_place_of_performance_foreign_location",
        "",
        "",
        "02",
        "Block Grant",
        "",  # "cfda_number", "cfda_title", "assistance_type_code", "assistance_type_description", "award_description",
        "",
        "",
        "",  # "business_funds_indicator_code", "business_funds_indicator_description", "business_types_code",
        "",
        "",
        "",  # "business_types_description", "correction_delete_indicator_code", "correction_delete_indicator_description",
        "",
        "",
        "",
        "",  # "action_type_code", "action_type_description", "record_type_code", "record_type_description",
        "",
        "",
        "",  # "highly_compensated_officer_1_name", "highly_compensated_officer_1_amount", "highly_compensated_officer_2_name",
        "",
        "",
        "",  # "highly_compensated_officer_2_amount", "highly_compensated_officer_3_name", "highly_compensated_officer_3_amount",
        "",
        "",
        "",  # "highly_compensated_officer_4_name", "highly_compensated_officer_4_amount", "highly_compensated_officer_5_name",
        "",
        f"http://www.usaspending.gov/award/ASST_NON_{i}_0_0",
        f"05/07/{fiscal_year}",  # "highly_compensated_officer_5_amount", "usaspending_permalink", "last_modified_date"
    ]


@pytest.fixture
def monthly_download_data(db, monkeypatch):
    for js in JOB_STATUS:
        baker.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    baker.make(
        "references.ToptierAgency", toptier_agency_id=1, toptier_code="001", name="Test_Agency", _fill_optional=True
    )
    baker.make("references.Agency", pk=1, toptier_agency_id=1, _fill_optional=True)
    baker.make(
        "references.ToptierAgency", toptier_agency_id=2, toptier_code="002", name="Test_Agency 2", _fill_optional=True
    )
    baker.make("references.Agency", pk=2, toptier_agency_id=2, _fill_optional=True)
    i = 1
    for fiscal_year in range(2001, 2021):
        baker.make(
            "search.AwardSearch",
            award_id=i,
            generated_unique_award_id=f"CONT_AWD_{i}_0_0",
            is_fpds=True,
            type="B",
            type_description="Purchase Order",
            piid=f"piid{i}",
            awarding_agency_id=1,
            funding_agency_id=1,
            latest_transaction_id=i,
            fiscal_year=fiscal_year,
        )
        baker.make(
            "search.TransactionSearch",
            award_id=i,
            transaction_id=i,
            is_fpds=True,
            transaction_unique_id=i,
            usaspending_unique_transaction_id="",
            type="B",
            type_description="Purchase Order",
            period_of_performance_start_date=datetime.datetime(fiscal_year, 5, 7),
            period_of_performance_current_end_date=datetime.datetime(fiscal_year, 5, 7),
            action_date=datetime.datetime(fiscal_year, 5, 7),
            federal_action_obligation=100,
            modification_number="1",
            transaction_description="a description",
            last_modified_date=datetime.datetime(fiscal_year, 5, 7),
            award_certified_date=datetime.datetime(fiscal_year, 5, 7),
            create_date=datetime.datetime(fiscal_year, 5, 7),
            update_date=datetime.datetime(fiscal_year, 5, 7),
            fiscal_year=fiscal_year,
            awarding_agency_id=1,
            funding_agency_id=1,
            original_loan_subsidy_cost=100.0,
            face_value_loan_guarantee=100.0,
            funding_amount=100.0,
            non_federal_funding_amount=100.0,
            generated_unique_award_id=f"CONT_AWD_{i}_0_0",
            business_categories=[],
            detached_award_proc_unique=f"test{i}",
            piid=f"piid{i}",
            agency_id=1,
            awarding_sub_tier_agency_c="001",
            awarding_subtier_agency_abbreviation="Test_Agency",
            awarding_agency_code="001",
            awarding_toptier_agency_abbreviation="Test_Agency",
            parent_award_id=f"000{i}",
        )
        baker.make(
            "search.AwardSearch",
            award_id=i + 100,
            generated_unique_award_id=f"ASST_NON_{i}_0_0",
            is_fpds=False,
            type="02",
            type_description="Block Grant",
            fain=f"fain{i}",
            awarding_agency_id=1,
            funding_agency_id=1,
            latest_transaction_id=i + 100,
            fiscal_year=fiscal_year,
        )
        baker.make(
            "search.TransactionSearch",
            award_id=i + 100,
            generated_unique_award_id=f"ASST_NON_{i}_0_0",
            transaction_id=i + 100,
            is_fpds=False,
            transaction_unique_id=i + 100,
            usaspending_unique_transaction_id="",
            type="02",
            type_description="Block Grant",
            period_of_performance_start_date=datetime.datetime(fiscal_year, 5, 7),
            period_of_performance_current_end_date=datetime.datetime(fiscal_year, 5, 7),
            action_date=datetime.datetime(fiscal_year, 5, 7),
            federal_action_obligation=100,
            modification_number=f"{i+100}",
            transaction_description="a description",
            last_modified_date=datetime.datetime(fiscal_year, 5, 7),
            award_certified_date=datetime.datetime(fiscal_year, 5, 7),
            create_date=datetime.datetime(fiscal_year, 5, 7),
            update_date=datetime.datetime(fiscal_year, 5, 7),
            fiscal_year=fiscal_year,
            awarding_agency_id=1,
            funding_agency_id=1,
            original_loan_subsidy_cost=100.0,
            face_value_loan_guarantee=100.0,
            funding_amount=100.0,
            non_federal_funding_amount=100.0,
            fain=f"fain{i}",
            awarding_agency_code="001",
            awarding_sub_tier_agency_c=1,
            awarding_toptier_agency_abbreviation="Test_Agency",
            awarding_subtier_agency_abbreviation="Test_Agency",
        )
        i += 1
    monkeypatch.setattr("usaspending_api.settings.MONTHLY_DOWNLOAD_S3_BUCKET_NAME", "whatever")


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_all_agencies(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    call_command("populate_monthly_files", "--fiscal_year=2020", "--local", "--clobber")
    file_list = os.listdir(fake_csv_local_path)

    assert f"FY2020_All_Contracts_Full_{TODAY}.zip" in file_list
    assert f"FY2020_All_Assistance_Full_{TODAY}.zip" in file_list


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_specific_agency(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    contract_data = generate_contract_data(2020, 1)
    assistance_data = generate_assistance_data(2020, 1)
    call_command("populate_monthly_files", "--agencies=1", "--fiscal_year=2020", "--local", "--clobber")
    file_list = os.listdir(fake_csv_local_path)

    assistance_csv_1 = f"FY2020_001_Assistance_Full_{TODAY}_1.csv"
    assistance_zip_1 = f"FY2020_001_Assistance_Full_{TODAY}.zip"
    contracts_csv_1 = f"FY2020_001_Contracts_Full_{TODAY}_1.csv"
    contracts_zip_1 = f"FY2020_001_Contracts_Full_{TODAY}.zip"

    assert contracts_zip_1 in file_list
    assert assistance_zip_1 in file_list

    with zipfile.ZipFile(os.path.normpath(f"{fake_csv_local_path}/{contracts_zip_1}"), "r") as zip_ref:
        zip_ref.extractall(fake_csv_local_path)
        assert contracts_csv_1 in os.listdir(fake_csv_local_path)
    with open(os.path.normpath(f"{fake_csv_local_path}/{contracts_csv_1}"), "r") as contract_file:
        csv_reader = reader(contract_file)
        row_count = 0
        for row in csv_reader:
            if row_count == 0:
                # 63 is the character limit for column names
                assert row == [s[:63] for s in query_paths["transaction_search"]["d1"].keys()]
            else:
                assert row == contract_data
            row_count += 1
    assert row_count >= 1

    with zipfile.ZipFile(os.path.normpath(f"{fake_csv_local_path}/{assistance_zip_1}"), "r") as zip_ref:
        zip_ref.extractall(fake_csv_local_path)
        assert assistance_csv_1 in os.listdir(fake_csv_local_path)
    with open(os.path.normpath(f"{fake_csv_local_path}/{assistance_csv_1}"), "r") as assistance_file:
        csv_reader = reader(assistance_file)
        row_count = 0
        for row in csv_reader:
            if row_count == 0:
                # 63 is the character limit for column names
                assert row == [s[:63] for s in query_paths["transaction_search"]["d2"].keys()]
            else:
                assert row == assistance_data
            row_count += 1
        assert row_count >= 1


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_agency_no_data(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    call_command("populate_monthly_files", "--agencies=2", "--fiscal_year=2022", "--local", "--clobber")
    contracts_zip = f"FY2022_002_Contracts_Full_{TODAY}.zip"
    contracts_csv_1 = f"FY2022_002_Contracts_Full_{TODAY}_1.csv"
    assistance_zip = f"FY2022_002_Assistance_Full_{TODAY}.zip"
    assistance_csv_1 = f"FY2022_002_Assistance_Full_{TODAY}_1.csv"

    for zip_file, csv_file in [(contracts_zip, contracts_csv_1), (assistance_zip, assistance_csv_1)]:

        with zipfile.ZipFile(os.path.normpath(f"{fake_csv_local_path}/{zip_file}"), "r") as zip_ref:
            zip_ref.extractall(fake_csv_local_path)
            assert csv_file in os.listdir(fake_csv_local_path), f"{csv_file} was not generated or extracted"

        with open(os.path.normpath(f"{fake_csv_local_path}/{csv_file}"), "r") as csv_file:
            csv_reader = reader(csv_file)
            row_count = 0
            for _ in csv_reader:
                row_count += 1
            assert row_count == 1, f"{csv_file} was not empty"


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_fiscal_years(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    # contract_data = generate_contract_data(2020, 1)
    # assistance_data = generate_assistance_data(2020, 1)
    call_command("populate_monthly_files", "--agencies=1", "--fiscal_year=2020", "--local", "--clobber")
    call_command("populate_monthly_files", "--agencies=1", "--fiscal_year=2004", "--local", "--clobber")
    file_list = os.listdir(fake_csv_local_path)
    expected_files = (
        f"FY2004_001_Contracts_Full_{TODAY}.zip",
        f"FY2004_001_Assistance_Full_{TODAY}.zip",
        f"FY2020_001_Contracts_Full_{TODAY}.zip",
        f"FY2020_001_Assistance_Full_{TODAY}.zip",
    )

    for expected_file in expected_files:
        assert expected_file in file_list


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
def test_award_type(client, fake_csv_local_path, monthly_download_data, monkeypatch):
    download_generation.retrieve_db_string = Mock(return_value=get_database_dsn_string(settings.DOWNLOAD_DB_ALIAS))
    call_command(
        "populate_monthly_files",
        "--agencies=1",
        "--fiscal_year=2020",
        "--award_types=assistance",
        "--local",
        "--clobber",
    )
    file_list = os.listdir(fake_csv_local_path)

    assert f"FY2020_001_Assistance_Full_{TODAY}.zip" in file_list
    assert f"FY2020_001_Contracts_Full_{TODAY}.zip" not in file_list
