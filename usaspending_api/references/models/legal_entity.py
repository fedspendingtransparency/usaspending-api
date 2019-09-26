from django.db import models
from django.contrib.postgres.fields import ArrayField
from usaspending_api.common.models import DataSourceTrackedModel


class LegalEntity(DataSourceTrackedModel):
    legal_entity_id = models.BigAutoField(primary_key=True, db_index=True)
    recipient_name = models.TextField(blank=True, verbose_name="Recipient Name", null=True)
    location = models.ForeignKey("references.Location", models.DO_NOTHING, null=True)
    parent_recipient_unique_id = models.TextField(
        blank=True, null=True, verbose_name="Parent DUNS Number", db_index=True
    )
    parent_recipient_name = models.TextField(blank=True, verbose_name="Parent Recipient Name", null=True)
    vendor_doing_as_business_name = models.TextField(blank=True, null=True)
    vendor_phone_number = models.TextField(blank=True, null=True)
    vendor_fax_number = models.TextField(blank=True, null=True)
    business_types = models.TextField(blank=True, null=True, db_index=True)
    business_types_description = models.TextField(blank=True, null=True)
    """
    Business Type Categories
    Make sure to leave default as 'list', as [] would share across instances

    Possible entries:

    category_business
    - small_business
    - other_than_small_business
    - corporate_entity_tax_exempt
    - corporate_entity_not_tax_exempt
    - partnership_or_limited_liability_partnership
    - sole_proprietorship
    - manufacturer_of_goods
    - subchapter_s_corporation
    - limited_liability_corporation

    minority_owned_business
    - alaskan_native_owned_business
    - american_indian_owned_business
    - asian_pacific_american_owned_business
    - black_american_owned_business
    - hispanic_american_owned_business
    - native_american_owned_business
    - native_hawaiian_owned_business
    - subcontinent_asian_indian_american_owned_business
    - tribally_owned_business
    - other_minority_owned_business

    women_owned_business
    - women_owned_small_business
    - economically_disadvantaged_women_owned_small_business
    - joint_venture_women_owned_small_business
    - joint_venture_economically_disadvantaged_women_owned_small_business

    veteran_owned_business
    - service_disabled_veteran_owned_business

    special_designations
    - 8a_program_participant
    - ability_one_program
    - dot_certified_disadvantaged_business_enterprise
    - emerging_small_business
    - federally_funded_research_and_development_corp
    - historically_underutilized_business_firm
    - labor_surplus_area_firm
    - sba_certified_8a_joint_venture
    - self_certified_small_disadvanted_business
    - small_agricultural_cooperative
    - small_disadvantaged_business
    - community_developed_corporation_owned_firm
    - us_owned_business
    - foreign_owned_and_us_located_business
    - foreign_owned_and_located_business
    - foreign_government
    - international_organization
    - domestic_shelter
    - hospital
    - veterinary_hospital

    nonprofit
    - foundation
    - community_development_corporations

    higher_education
    - educational_institution
    - public_institution_of_higher_education
    - private_institution_of_higher_education
    - minority_serving_institution_of_higher_education
    - school_of_forestry
    - veterinary_college

    government
    - national_government
    - interstate_entity
    - regional_and_state_government
    - regional_organization
    - us_territory_or_possession
    - council_of_governments
    - local_government
    - indian_native_american_tribal_government
    - authorities_and_commissions

    individuals
    """
    business_categories = ArrayField(models.TextField(), default=list)

    recipient_unique_id = models.TextField(blank=True, default="", null=True, verbose_name="DUNS Number", db_index=True)
    limited_liability_corporation = models.NullBooleanField(blank=False, default=False)
    sole_proprietorship = models.NullBooleanField(blank=False, default=False)
    partnership_or_limited_liability_partnership = models.NullBooleanField(blank=False, default=False)
    subchapter_scorporation = models.NullBooleanField(blank=False, default=False)
    foundation = models.NullBooleanField(blank=False, default=False)
    for_profit_organization = models.NullBooleanField(blank=False, default=False)
    nonprofit_organization = models.NullBooleanField(blank=False, default=False)
    corporate_entity_tax_exempt = models.NullBooleanField(blank=False, default=False)
    corporate_entity_not_tax_exempt = models.NullBooleanField(blank=False, default=False)
    other_not_for_profit_organization = models.NullBooleanField(blank=False, default=False)
    sam_exception = models.TextField(blank=True, null=True)
    city_local_government = models.NullBooleanField(blank=False, default=False)
    county_local_government = models.NullBooleanField(blank=False, default=False)
    inter_municipal_local_government = models.NullBooleanField(blank=False, default=False)
    local_government_owned = models.NullBooleanField(blank=False, default=False)
    municipality_local_government = models.NullBooleanField(blank=False, default=False)
    school_district_local_government = models.NullBooleanField(blank=False, default=False)
    township_local_government = models.NullBooleanField(blank=False, default=False)
    us_state_government = models.NullBooleanField(blank=False, default=False)
    us_federal_government = models.NullBooleanField(blank=False, default=False)
    federal_agency = models.NullBooleanField(blank=False, default=False)
    federally_funded_research_and_development_corp = models.NullBooleanField(blank=False, default=False)
    us_tribal_government = models.NullBooleanField(blank=False, default=False)
    foreign_government = models.NullBooleanField(blank=False, default=False)
    community_developed_corporation_owned_firm = models.NullBooleanField(blank=False, default=False)
    labor_surplus_area_firm = models.NullBooleanField(blank=False, default=False)
    small_agricultural_cooperative = models.NullBooleanField(blank=False, default=False)
    international_organization = models.NullBooleanField(blank=False, default=False)
    us_government_entity = models.NullBooleanField(blank=False, default=False)
    emerging_small_business = models.NullBooleanField(blank=False, default=False)
    c8a_program_participant = models.NullBooleanField(
        db_column="8a_program_participant",
        max_length=1,
        blank=False,
        default=False,
        verbose_name="8a Program Participant",
    )  # Field renamed because it wasn't a valid Python identifier.
    sba_certified_8a_joint_venture = models.NullBooleanField(blank=False, default=False)
    dot_certified_disadvantage = models.NullBooleanField(blank=False, default=False)
    self_certified_small_disadvantaged_business = models.NullBooleanField(blank=False, default=False)
    historically_underutilized_business_zone = models.NullBooleanField(blank=False, default=False)
    small_disadvantaged_business = models.NullBooleanField(blank=False, default=False)
    the_ability_one_program = models.NullBooleanField(blank=False, default=False)
    historically_black_college = models.NullBooleanField(blank=False, default=False)
    c1862_land_grant_college = models.NullBooleanField(
        db_column="1862_land_grant_college",
        max_length=1,
        blank=False,
        default=False,
        verbose_name="1862 Land Grant College",
    )  # Field renamed because it wasn't a valid Python identifier.
    c1890_land_grant_college = models.NullBooleanField(
        db_column="1890_land_grant_college",
        max_length=1,
        blank=False,
        default=False,
        verbose_name="1890 Land Grant College",
    )  # Field renamed because it wasn't a valid Python identifier.
    c1994_land_grant_college = models.NullBooleanField(
        db_column="1994_land_grant_college",
        max_length=1,
        blank=False,
        default=False,
        verbose_name="1894 Land Grant College",
    )  # Field renamed because it wasn't a valid Python identifier.
    minority_institution = models.NullBooleanField(blank=False, default=False)
    private_university_or_college = models.NullBooleanField(blank=False, default=False)
    school_of_forestry = models.NullBooleanField(blank=False, default=False)
    state_controlled_institution_of_higher_learning = models.NullBooleanField(blank=False, default=False)
    tribal_college = models.NullBooleanField(blank=False, default=False)
    veterinary_college = models.NullBooleanField(blank=False, default=False)
    educational_institution = models.NullBooleanField(blank=False, default=False)
    alaskan_native_servicing_institution = models.NullBooleanField(
        blank=False, default=False, verbose_name="Alaskan Native Owned Servicing Institution"
    )
    community_development_corporation = models.NullBooleanField(blank=False, default=False)
    native_hawaiian_servicing_institution = models.NullBooleanField(blank=False, default=False)
    domestic_shelter = models.NullBooleanField(blank=False, default=False)
    manufacturer_of_goods = models.NullBooleanField(blank=False, default=False)
    hospital_flag = models.NullBooleanField(blank=False, default=False)
    veterinary_hospital = models.NullBooleanField(blank=False, default=False)
    hispanic_servicing_institution = models.NullBooleanField(blank=False, default=False)
    woman_owned_business = models.NullBooleanField(blank=False, default=False)
    minority_owned_business = models.NullBooleanField(blank=False, default=False)
    women_owned_small_business = models.NullBooleanField(blank=False, default=False)
    economically_disadvantaged_women_owned_small_business = models.NullBooleanField(blank=False, default=False)
    joint_venture_women_owned_small_business = models.NullBooleanField(blank=False, default=False)
    joint_venture_economic_disadvantaged_women_owned_small_bus = models.NullBooleanField(blank=False, default=False)
    veteran_owned_business = models.NullBooleanField(blank=False, default=False)
    service_disabled_veteran_owned_business = models.NullBooleanField(blank=False, default=False)
    contracts = models.NullBooleanField(blank=False, default=False)
    grants = models.NullBooleanField(blank=False, default=False)
    receives_contracts_and_grants = models.NullBooleanField(blank=False, default=False)
    airport_authority = models.NullBooleanField(blank=False, default=False, verbose_name="Airport Authority")
    council_of_governments = models.NullBooleanField(blank=False, default=False)
    housing_authorities_public_tribal = models.NullBooleanField(blank=False, default=False)
    interstate_entity = models.NullBooleanField(blank=False, default=False)
    planning_commission = models.NullBooleanField(blank=False, default=False)
    port_authority = models.NullBooleanField(blank=False, default=False)
    transit_authority = models.NullBooleanField(blank=False, default=False)
    foreign_owned_and_located = models.NullBooleanField(blank=False, default=False)
    american_indian_owned_business = models.NullBooleanField(
        blank=False, default=False, verbose_name="American Indian Owned Business"
    )
    alaskan_native_owned_corporation_or_firm = models.NullBooleanField(
        blank=False, default=False, verbose_name="Alaskan Native Owned Corporation or Firm"
    )
    indian_tribe_federally_recognized = models.NullBooleanField(blank=False, default=False)
    native_hawaiian_owned_business = models.NullBooleanField(blank=False, default=False)
    tribally_owned_business = models.NullBooleanField(blank=False, default=False)
    asian_pacific_american_owned_business = models.NullBooleanField(
        blank=False, default=False, verbose_name="Asian Pacific American Owned business"
    )
    black_american_owned_business = models.NullBooleanField(blank=False, default=False)
    hispanic_american_owned_business = models.NullBooleanField(blank=False, default=False)
    native_american_owned_business = models.NullBooleanField(blank=False, default=False)
    subcontinent_asian_asian_indian_american_owned_business = models.NullBooleanField(blank=False, default=False)
    other_minority_owned_business = models.NullBooleanField(blank=False, default=False)
    us_local_government = models.NullBooleanField(blank=False, default=False)
    undefinitized_action = models.TextField(blank=True, null=True)
    domestic_or_foreign_entity = models.TextField(blank=True, null=True, db_index=False)
    domestic_or_foreign_entity_description = models.TextField(null=True, blank=True)
    division_name = models.TextField(blank=True, null=True)
    division_number = models.TextField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    # Fields added to accomodate recipient_type of financial assistance records
    city_township_government = models.TextField(blank=True, null=True)
    special_district_government = models.TextField(blank=True, null=True)
    small_business = models.TextField(blank=True, null=True)
    small_business_description = models.TextField(blank=True, null=True)
    individual = models.TextField(blank=True, null=True)
    is_fpds = models.BooleanField(blank=False, default=False, verbose_name="Is FPDS")
    transaction_unique_id = models.TextField(blank=False, default="NONE", verbose_name="Transaction Unique ID")

    class Meta:
        managed = True
        db_table = "legal_entity"
        index_together = ("recipient_unique_id", "recipient_name", "update_date")
