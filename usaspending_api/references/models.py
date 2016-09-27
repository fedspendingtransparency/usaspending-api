from django.db import models


class RefCityCountyCode(models.Model):
    city_county_code_id = models.AutoField(primary_key=True)
    state_code = models.CharField(max_length=2, blank=True, null=True)
    city_name = models.CharField(max_length=50, blank=True, null=True)
    city_code = models.CharField(max_length=5, blank=True, null=True)
    county_code = models.CharField(max_length=3, blank=True, null=True)
    county_name = models.CharField(max_length=100, blank=True, null=True)
    type_of_area = models.CharField(max_length=20, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_city_county_code'


class RefCountryCode(models.Model):
    country_code = models.CharField(primary_key=True, max_length=3)
    country_name = models.CharField(max_length=100, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_country_code'

"""{
    'agency_name': 'name',
    'agency_code_fpds': 'fpds_code',
    'agency_code_cgac': 'cgac_code',
    'department_parent_id': 'department',
    'sub_tier_parent_id': 'parent_agency',
    'agency_code_aac': 'acc_code',
    'agency_code_4cc': 'fourcc_code',

    }"""

class Agency(models.Model):
    #id = models.AutoField(primary_key=True)
    id = models.AutoField(primary_key=True) # CGAC code
    # agency_code_aac = models.CharField(max_length=6, blank=True, null=True)
    fpds_code = models.CharField(max_length=4, blank=True, null=True)
    name = models.CharField(max_length=150, blank=True, null=True)
    department = models.ForeignKey('self', on_delete=models.CASCADE, null=True, related_name='sub_departments')
    parent_agency = models.ForeignKey('self', on_delete=models.CASCADE, null=True, related_name='sub_agencies')
    aac_code = models.CharField(max_length=6, blank=True, null=True)
    fourcc_code = models.CharField(max_length=4, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    location = models.ForeignKey('Location', models.DO_NOTHING, null=True)

    class Meta:
        managed = True
        db_table = 'agency'
 
# comment to remove later
class Location(models.Model):
    location_id = models.AutoField(primary_key=True)
    location_country_code = models.ForeignKey('RefCountryCode', models.DO_NOTHING, db_column='location_country_code', blank=True, null=True)
    location_country_name = models.CharField(max_length=100, blank=True, null=True)
    location_state_code = models.CharField(max_length=2, blank=True, null=True)
    location_state_name = models.CharField(max_length=50, blank=True, null=True)
    location_state_text = models.CharField(max_length=100, blank=True, null=True)
    location_city_name = models.CharField(max_length=40, blank=True, null=True)
    location_city_code = models.CharField(max_length=5, blank=True, null=True)
    location_county_name = models.CharField(max_length=40, blank=True, null=True)
    location_county_code = models.CharField(max_length=3, blank=True, null=True)
    location_address_line1 = models.CharField(max_length=150, blank=True, null=True)
    location_address_line2 = models.CharField(max_length=150, blank=True, null=True)
    location_address_line3 = models.CharField(max_length=55, blank=True, null=True)
    location_foreign_location_description = models.CharField(max_length=100, blank=True, null=True)
    location_zip4 = models.CharField(max_length=10, blank=True, null=True)
    location_zip_4a = models.CharField(max_length=10, blank=True, null=True)
    location_congressional_code = models.CharField(max_length=2, blank=True, null=True)
    location_performance_code = models.CharField(max_length=9, blank=True, null=True)
    location_zip_last4 = models.CharField(max_length=4, blank=True, null=True)
    location_zip5 = models.CharField(max_length=5, blank=True, null=True)
    location_foreign_postal_code = models.CharField(max_length=50, blank=True, null=True)
    location_foreign_province = models.CharField(max_length=25, blank=True, null=True)
    location_foreign_city_name = models.CharField(max_length=40, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)


class LegalEntity(models.Model):
    legal_entity_id = models.AutoField(primary_key=True)
    location = models.ForeignKey('Location', models.DO_NOTHING, null=True)
    ultimate_parent_legal_entity_id = models.IntegerField(null=True)
    # duns number ?
    awardee_or_recipient_legal = models.CharField(max_length=120, blank=True)
    vendor_doing_as_business_n = models.CharField(max_length=400, blank=True, null=True)
    vendor_phone_number = models.CharField(max_length=30, blank=True, null=True)
    vendor_fax_number = models.CharField(max_length=30, blank=True, null=True)
    business_types = models.CharField(max_length=3, blank=True, null=True)
    awardee_or_recipient_uniqu = models.CharField(max_length=9, blank=True, null=True)
    limited_liability_corporat = models.CharField(max_length=1, blank=True, null=True)
    sole_proprietorship = models.CharField(max_length=1, blank=True, null=True)
    partnership_or_limited_lia = models.CharField(max_length=1, blank=True, null=True)
    subchapter_s_corporation = models.CharField(max_length=1, blank=True, null=True)
    foundation = models.CharField(max_length=1, blank=True, null=True)
    for_profit_organization = models.CharField(max_length=1, blank=True, null=True)
    nonprofit_organization = models.CharField(max_length=1, blank=True, null=True)
    corporate_entity_tax_exemp = models.CharField(max_length=1, blank=True, null=True)
    corporate_entity_not_tax_e = models.CharField(max_length=1, blank=True, null=True)
    other_not_for_profit_organ = models.CharField(max_length=1, blank=True, null=True)
    sam_exception = models.CharField(max_length=1, blank=True, null=True)
    city_local_government = models.CharField(max_length=1, blank=True, null=True)
    county_local_government = models.CharField(max_length=1, blank=True, null=True)
    inter_municipal_local_gove = models.CharField(max_length=1, blank=True, null=True)
    local_government_owned = models.CharField(max_length=1, blank=True, null=True)
    municipality_local_governm = models.CharField(max_length=1, blank=True, null=True)
    school_district_local_gove = models.CharField(max_length=1, blank=True, null=True)
    township_local_government = models.CharField(max_length=1, blank=True, null=True)
    us_state_government = models.CharField(max_length=1, blank=True, null=True)
    us_federal_government = models.CharField(max_length=1, blank=True, null=True)
    federal_agency = models.CharField(max_length=1, blank=True, null=True)
    federally_funded_research = models.CharField(max_length=1, blank=True, null=True)
    us_tribal_government = models.CharField(max_length=1, blank=True, null=True)
    foreign_government = models.CharField(max_length=1, blank=True, null=True)
    community_developed_corpor = models.CharField(max_length=1, blank=True, null=True)
    labor_surplus_area_firm = models.CharField(max_length=1, blank=True, null=True)
    small_agricultural_coopera = models.CharField(max_length=1, blank=True, null=True)
    international_organization = models.CharField(max_length=1, blank=True, null=True)
    us_government_entity = models.CharField(max_length=1, blank=True, null=True)
    emerging_small_business = models.CharField(max_length=1, blank=True, null=True)
    number_8a_program_participant = models.CharField(db_column='8a_program_participant', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    sba_certified_8_a_joint_ve = models.CharField(max_length=1, blank=True, null=True)
    dot_certified_disadvantage = models.CharField(max_length=1, blank=True, null=True)
    self_certified_small_disad = models.CharField(max_length=1, blank=True, null=True)
    historically_underutilized = models.CharField(max_length=1, blank=True, null=True)
    small_disadvantaged_busine = models.CharField(max_length=1, blank=True, null=True)
    the_ability_one_program = models.CharField(max_length=1, blank=True, null=True)
    historically_black_college = models.CharField(max_length=1, blank=True, null=True)
    number_1862_land_grant_college = models.CharField(db_column='1862_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    number_1890_land_grant_college = models.CharField(db_column='1890_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    number_1994_land_grant_college = models.CharField(db_column='1994_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    minority_institution = models.CharField(max_length=1, blank=True, null=True)
    private_university_or_coll = models.CharField(max_length=1, blank=True, null=True)
    school_of_forestry = models.CharField(max_length=1, blank=True, null=True)
    state_controlled_instituti = models.CharField(max_length=1, blank=True, null=True)
    tribal_college = models.CharField(max_length=1, blank=True, null=True)
    veterinary_college = models.CharField(max_length=1, blank=True, null=True)
    educational_institution = models.CharField(max_length=1, blank=True, null=True)
    alaskan_native_servicing_i = models.CharField(max_length=1, blank=True, null=True)
    community_development_corp = models.CharField(max_length=1, blank=True, null=True)
    native_hawaiian_servicing = models.CharField(max_length=1, blank=True, null=True)
    domestic_shelter = models.CharField(max_length=1, blank=True, null=True)
    manufacturer_of_goods = models.CharField(max_length=1, blank=True, null=True)
    hospital_flag = models.CharField(max_length=1, blank=True, null=True)
    veterinary_hospital = models.CharField(max_length=1, blank=True, null=True)
    hispanic_servicing_institu = models.CharField(max_length=1, blank=True, null=True)
    woman_owned_business = models.CharField(max_length=1, blank=True, null=True)
    minority_owned_business = models.CharField(max_length=1, blank=True, null=True)
    women_owned_small_business = models.CharField(max_length=1, blank=True, null=True)
    economically_disadvantaged = models.CharField(max_length=1, blank=True, null=True)
    joint_venture_women_owned = models.CharField(max_length=1, blank=True, null=True)
    joint_venture_economically = models.CharField(max_length=1, blank=True, null=True)
    veteran_owned_business = models.CharField(max_length=1, blank=True, null=True)
    service_disabled_veteran_o = models.CharField(max_length=1, blank=True, null=True)
    contracts = models.CharField(max_length=1, blank=True, null=True)
    grants = models.CharField(max_length=1, blank=True, null=True)
    receives_contracts_and_gra = models.CharField(max_length=1, blank=True, null=True)
    airport_authority = models.CharField(max_length=1, blank=True, null=True)
    council_of_governments = models.CharField(max_length=1, blank=True, null=True)
    housing_authorities_public = models.CharField(max_length=1, blank=True, null=True)
    interstate_entity = models.CharField(max_length=1, blank=True, null=True)
    planning_commission = models.CharField(max_length=1, blank=True, null=True)
    port_authority = models.CharField(max_length=1, blank=True, null=True)
    transit_authority = models.CharField(max_length=1, blank=True, null=True)
    foreign_owned_and_located = models.CharField(max_length=1, blank=True, null=True)
    american_indian_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    alaskan_native_owned_corpo = models.CharField(max_length=1, blank=True, null=True)
    indian_tribe_federally_rec = models.CharField(max_length=1, blank=True, null=True)
    native_hawaiian_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    tribally_owned_business = models.CharField(max_length=1, blank=True, null=True)
    asian_pacific_american_own = models.CharField(max_length=1, blank=True, null=True)
    black_american_owned_busin = models.CharField(max_length=1, blank=True, null=True)
    hispanic_american_owned_bu = models.CharField(max_length=1, blank=True, null=True)
    native_american_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    subcontinent_asian_asian_i = models.CharField(max_length=1, blank=True, null=True)
    other_minority_owned_busin = models.CharField(max_length=1, blank=True, null=True)
    us_local_government = models.CharField(max_length=1, blank=True, null=True)
    undefinitized_action = models.CharField(max_length=1, blank=True, null=True)
    domestic_or_foreign_entity = models.CharField(max_length=1, blank=True, null=True)
    division_name = models.CharField(max_length=100, blank=True, null=True)
    division_number = models.CharField(max_length=100, blank=True, null=True)  
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

# Reference tables
class RefObjectClassCode(models.Model):
    object_class = models.CharField(primary_key=True, max_length=4)
    max_object_class_name = models.CharField(max_length=60, blank=True, null=True)
    direct_or_reimbursable = models.CharField(max_length=25, blank=True, null=True)
    label = models.CharField(max_length=100, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_object_class_code'


"""BD 09/21 - Added the ref_program_activity_id, responsible_agency_id, allocation_transfer_agency_id,main_account_code to the RefProgramActivity model as well as unique concatenated key"""

class RefProgramActivity(models.Model):
    ref_program_activity_id = models.IntegerField(primary_key=True)
    program_activity_code = models.CharField(max_length=4)
    program_activity_name = models.CharField(max_length=164)
    budget_year = models.CharField(max_length=4, blank=True, null=True)
    responsible_agency_id = models.CharField(max_length=3, blank=True, null=True)
    allocation_transfer_agency_id = models.CharField(max_length=3, blank=True, null=True)
    main_account_code = models.CharField(max_length=4, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_program_activity'
        unique_together = (('program_activity_code', 'program_activity_name','budget_year','responsible_agency_id','allocation_transfer_agency_id','main_account_code'),)
