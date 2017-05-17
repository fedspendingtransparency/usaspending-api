import logging

from django.db import models
from django.db.models import F, Q
from django.utils.text import slugify
from usaspending_api.common.models import DataSourceTrackedModel
from usaspending_api.references.helpers import canonicalize_string


class RefCityCountyCode(models.Model):
    city_county_code_id = models.AutoField(primary_key=True)
    state_code = models.TextField(blank=True, null=True)
    city_name = models.TextField(blank=True, null=True)
    city_code = models.TextField(blank=True, null=True)
    county_code = models.TextField(blank=True, null=True)
    county_name = models.TextField(blank=True, null=True)
    type_of_area = models.TextField(blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_city_county_code'

    @classmethod
    def canonicalize(cls):
        """
        Transforms the values in `city_name` and `county_name`
        to their canonicalized (uppercase, regulare spaced) form.
        """
        for obj in cls.objects.all():
            obj.city_name = canonicalize_string(obj.city_name)
            obj.county_name = canonicalize_string(obj.county_name)
            obj.save()


class RefCountryCode(models.Model):
    country_code = models.TextField(primary_key=True)
    country_name = models.TextField(blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_country_code'

    def __str__(self):
        return '%s: %s' % (self.country_code, self.country_name)


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

    id = models.AutoField(primary_key=True)  # meaningless id
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    toptier_agency = models.ForeignKey('ToptierAgency', models.DO_NOTHING, null=True)
    subtier_agency = models.ForeignKey('SubtierAgency', models.DO_NOTHING, null=True)
    office_agency = models.ForeignKey('OfficeAgency', models.DO_NOTHING, null=True)

    # 1182 This flag is true if toptier agency name and subtier agency name are equal.
    # This means the award is at the department level.
    toptier_flag = models.BooleanField(default=False)

    @staticmethod
    def get_by_toptier(toptier_cgac_code):
        """
        Get an agency record by toptier information only

        Args:
            toptier_cgac_code: a CGAC (aka department) code

        Returns:
            an Agency instance

        """
        return Agency.objects.filter(
            toptier_agency__cgac_code=toptier_cgac_code,
            subtier_agency__name=F('toptier_agency__name')).order_by('-update_date').first()

    def get_by_subtier(subtier_code):
        """
        Get an agency record by subtier information only

        Args:
            subtier_code: subtier code

        Returns:
            an Agency instance

        If called with None / empty subtier code, returns None
        """
        if subtier_code:
            return Agency.objects.filter(
                subtier_agency__subtier_code=subtier_code).order_by('-update_date').first()

    @staticmethod
    def get_by_toptier_subtier(toptier_cgac_code, subtier_code):
        """
        Lookup an Agency record by toptier cgac code and subtier code

        Args:
            toptier_cgac_code: a CGAC (aka department) code
            subtier_code: an agency subtier code

        Returns:
            an Agency instance

        """
        return Agency.objects.filter(
            toptier_agency__cgac_code=toptier_cgac_code,
            subtier_agency__subtier_code=subtier_code
        ).order_by('-update_date').first()

    class Meta:
        managed = True
        db_table = 'agency'
        unique_together = ("toptier_agency",
                           "subtier_agency",
                           "office_agency")

    def __str__(self):
        stringrep = ""
        for agency in [self.toptier_agency, self.subtier_agency, self.office_agency]:
            if agency:
                stringrep = stringrep + agency.name + " :: "
        return stringrep


class ToptierAgency(models.Model):
    toptier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    cgac_code = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Code")
    fpds_code = models.TextField(blank=True, null=True)
    abbreviation = models.TextField(blank=True, null=True, verbose_name="Agency Abbreviation")
    name = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Name")

    class Meta:
        managed = True
        db_table = 'toptier_agency'


class SubtierAgency(models.Model):
    subtier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    subtier_code = models.TextField(blank=True, null=True, verbose_name="Sub-Tier Agency Code")
    abbreviation = models.TextField(blank=True, null=True, verbose_name="Agency Abbreviation")
    name = models.TextField(blank=True, null=True, verbose_name="Sub-Tier Agency Name")

    class Meta:
        managed = True
        db_table = 'subtier_agency'


class OfficeAgency(models.Model):
    office_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    aac_code = models.TextField(blank=True, null=True, verbose_name="Office Code")
    name = models.TextField(blank=True, null=True, verbose_name="Office Name")

    class Meta:
        managed = True
        db_table = 'office_agency'


class Location(DataSourceTrackedModel):
    location_id = models.AutoField(primary_key=True)
    location_country_code = models.ForeignKey('RefCountryCode', models.DO_NOTHING, db_column='location_country_code', blank=True, null=True, verbose_name="Country Code")
    country_name = models.TextField(blank=True, null=True, verbose_name="Country Name")
    state_code = models.TextField(blank=True, null=True, verbose_name="State Code")
    state_name = models.TextField(blank=True, null=True, verbose_name="State Name")
    state_description = models.TextField(blank=True, null=True, verbose_name="State Description")
    city_name = models.TextField(blank=True, null=True, verbose_name="City Name")
    city_code = models.TextField(blank=True, null=True)
    county_name = models.TextField(blank=True, null=True)
    county_code = models.TextField(blank=True, null=True)
    address_line1 = models.TextField(blank=True, null=True, verbose_name="Address Line 1")
    address_line2 = models.TextField(blank=True, null=True, verbose_name="Address Line 2")
    address_line3 = models.TextField(blank=True, null=True, verbose_name="Address Line 3")
    foreign_location_description = models.TextField(blank=True, null=True)
    zip4 = models.TextField(blank=True, null=True, verbose_name="ZIP+4")
    zip_4a = models.TextField(blank=True, null=True)
    congressional_code = models.TextField(blank=True, null=True, verbose_name="Congressional District Code")
    performance_code = models.TextField(blank=True, null=True, verbose_name="Primary Place Of Performance Location Code")
    zip_last4 = models.TextField(blank=True, null=True)
    zip5 = models.TextField(blank=True, null=True)
    foreign_postal_code = models.TextField(blank=True, null=True)
    foreign_province = models.TextField(blank=True, null=True)
    foreign_city_name = models.TextField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    # Tags whether this location is used as a place of performance or a recipient
    # location, or both
    place_of_performance_flag = models.BooleanField(default=False, verbose_name="Location used as place of performance")
    recipient_flag = models.BooleanField(default=False, verbose_name="Location used as recipient location")

    def save(self, *args, **kwargs):
        self.load_country_data()
        self.load_city_county_data()
        super(Location, self).save(*args, **kwargs)

    def load_country_data(self):
        if self.location_country_code:
            self.country_name = self.location_country_code.country_name

    def load_city_county_data(self):
        # Here we fill in missing information from the ref city county code data
        if self.location_country_code_id == "USA":
            q_kwargs = {
                "city_code": self.city_code,
                "county_code": self.county_code,
                "state_code__iexact": self.state_code,
                "city_name__iexact": self.city_name,
                "county_name__iexact": self.county_name
            }
            # Clear out any blank or None values in our filter, so we can find the best match
            q_kwargs = dict((k, v) for k, v in q_kwargs.items() if v)
            matched_reference = RefCityCountyCode.objects.filter(Q(**q_kwargs))
            # We only load the data if our matched reference count is one; otherwise,
            # we don't have data (count=0) or the match is ambiguous (count>1)
            if matched_reference.count() == 1:
                # Load this data
                matched_reference = matched_reference.first()
                self.city_code = matched_reference.city_code
                self.county_code = matched_reference.county_code
                self.state_code = matched_reference.state_code
                self.city_name = matched_reference.city_name
                self.county_name = matched_reference.county_name
            else:
                logging.getLogger('debug').info("Could not find single matching city/county for following arguments:" + str(q_kwargs) + "; got " + str(matched_reference.count()))

    class Meta:
        # Let's make almost every column unique together so we don't have to
        # perform heavy lifting on checking if a location already exists or not
        unique_together = ("location_country_code",
                           "country_name",
                           "state_code",
                           "state_name",
                           "state_description",
                           "city_name",
                           "city_code",
                           "county_name",
                           "county_code",
                           "address_line1",
                           "address_line2",
                           "address_line3",
                           "foreign_location_description",
                           "zip4",
                           "congressional_code",
                           "performance_code",
                           "zip_last4",
                           "zip5",
                           "foreign_postal_code",
                           "foreign_province",
                           "foreign_city_name",
                           "reporting_period_start",
                           "reporting_period_end")


class LegalEntity(DataSourceTrackedModel):
    legal_entity_id = models.AutoField(primary_key=True)
    location = models.ForeignKey('Location', models.DO_NOTHING, null=True)
    parent_recipient_unique_id = models.TextField(blank=True, null=True, verbose_name="Parent DUNS Number")
    recipient_name = models.TextField(blank=True, verbose_name="Recipient Name")
    vendor_doing_as_business_name = models.TextField(blank=True, null=True)
    vendor_phone_number = models.TextField(blank=True, null=True)
    vendor_fax_number = models.TextField(blank=True, null=True)
    business_types = models.TextField(blank=True, null=True)
    business_types_description = models.TextField(blank=True, null=True)
    recipient_unique_id = models.TextField(blank=True, null=True, verbose_name="DUNS Number")

    # Business Type Categories
    # These "aggregate" the many business type flags into buckets
    category_business = models.BooleanField(default=False)
    category_small_business = models.BooleanField(default=False)
    category_other_than_small_business = models.BooleanField(default=False)

    category_minority_owned_business = models.BooleanField(default=False)
    category_alaskan_native_owned_business = models.BooleanField(default=False)
    category_american_indian_owned_business = models.BooleanField(default=False)
    category_asian_pacific_american_owned_business = models.BooleanField(default=False)
    category_black_american_owned_business = models.BooleanField(default=False)
    category_hispanic_american_owned_business = models.BooleanField(default=False)
    category_native_american_owned_business = models.BooleanField(default=False)
    category_native_hawaiian_owned_business = models.BooleanField(default=False)
    category_subcontinent_asian_indian_american_owned_business = models.BooleanField(default=False)
    category_tribally_owned_business = models.BooleanField(default=False)
    category_other_minority_owned_business = models.BooleanField(default=False)

    category_woman_owned_business = models.BooleanField(default=False)
    category_women_owned_business = models.BooleanField(default=False)
    category_women_owned_small_business = models.BooleanField(default=False)
    category_economically_disadvantaged_women_owned_small_business = models.BooleanField(default=False)
    category_joint_venture_women_owned_small_business = models.BooleanField(default=False)
    category_joint_venture_economically_disadvantaged_women_owned_small_business = models.BooleanField(default=False, db_column="category_jvwosb")

    category_veteran_owned_business = models.BooleanField(default=False)
    category_service_disabled_veteran_owned_business = models.BooleanField(default=False)

    category_special_designations = models.BooleanField(default=False)
    category_8a_program_participant = models.BooleanField(default=False)
    category_ability_one_program = models.BooleanField(default=False)
    category_dot_certified_disadvantaged_business_enterprise = models.BooleanField(default=False)
    category_emerging_small_business = models.BooleanField(default=False)
    category_federally_funded_research_and_development_corp = models.BooleanField(default=False)
    category_historically_underutilized_business_firm = models.BooleanField(default=False)
    category_labor_surplus_area_firm = models.BooleanField(default=False)
    category_sba_certified_8a_joint_venture = models.BooleanField(default=False)
    category_self_certified_small_disadvanted_business = models.BooleanField(default=False)
    category_small_agricultural_cooperative = models.BooleanField(default=False)
    category_small_disadvantaged_business = models.BooleanField(default=False)
    category_community_developed_corporation_owned_firm = models.BooleanField(default=False)
    category_us_owned_business = models.BooleanField(default=False)
    category_foreign_owned_and_us_located_business = models.BooleanField(default=False)
    category_foreign_owned_and_located_business = models.BooleanField(default=False)
    category_foreign_government = models.BooleanField(default=False)
    category_international_organization = models.BooleanField(default=False)

    category_nonprofit = models.BooleanField(default=False)
    category_foundation = models.BooleanField(default=False)
    category_community_development_corporations = models.BooleanField(default=False)

    category_higher_education = models.BooleanField(default=False)
    category_public_institution_of_higher_education = models.BooleanField(default=False)
    category_private_institution_of_higher_education = models.BooleanField(default=False)
    category_minority_serving_institution_of_higher_education = models.BooleanField(default=False)

    category_government = models.BooleanField(default=False)
    category_national_government = models.BooleanField(default=False)
    category_regional_and_state_government = models.BooleanField(default=False)
    category_us_territory_or_possession = models.BooleanField(default=False)
    category_local_government = models.BooleanField(default=False)
    category_indian_native_american_tribal_government = models.BooleanField(default=False)
    category_authorities_and_commissions = models.BooleanField(default=False)

    category_individuals = models.BooleanField(default=False)

    # Business Type Flags
    contracting_officers_determination_of_business_size = models.TextField(blank=True, null=True)
    limited_liability_corporation = models.TextField(blank=True, null=True)
    sole_proprietorship = models.TextField(blank=True, null=True)
    partnership_or_limited_liability_partnership = models.TextField(blank=True, null=True)
    subchapter_scorporation = models.TextField(blank=True, null=True)
    foundation = models.TextField(blank=True, null=True)
    for_profit_organization = models.TextField(blank=True, null=True)
    nonprofit_organization = models.TextField(blank=True, null=True)
    corporate_entity_tax_exempt = models.TextField(blank=True, null=True)
    corporate_entity_not_tax_exempt = models.TextField(blank=True, null=True)
    other_not_for_profit_organization = models.TextField(blank=True, null=True)
    sam_exception = models.TextField(blank=True, null=True)
    city_local_government = models.TextField(blank=True, null=True)
    county_local_government = models.TextField(blank=True, null=True)
    inter_municipal_local_government = models.TextField(blank=True, null=True)
    local_government_owned = models.TextField(blank=True, null=True)
    municipality_local_government = models.TextField(blank=True, null=True)
    school_district_local_government = models.TextField(blank=True, null=True)
    township_local_government = models.TextField(blank=True, null=True)
    us_state_government = models.TextField(blank=True, null=True)
    us_federal_government = models.TextField(blank=True, null=True)
    federal_agency = models.TextField(blank=True, null=True)
    federally_funded_research_and_development_corp = models.TextField(blank=True, null=True)
    us_tribal_government = models.TextField(blank=True, null=True)
    foreign_government = models.TextField(blank=True, null=True)
    community_developed_corporation_owned_firm = models.TextField(blank=True, null=True)
    labor_surplus_area_firm = models.TextField(blank=True, null=True)
    small_agricultural_cooperative = models.TextField(blank=True, null=True)
    international_organization = models.TextField(blank=True, null=True)
    us_government_entity = models.TextField(blank=True, null=True)
    emerging_small_business = models.TextField(blank=True, null=True)
    c8a_program_participant = models.TextField(db_column='8a_program_participant', max_length=1, blank=True, null=True, verbose_name="8a Program Participant")  # Field renamed because it wasn't a valid Python identifier.
    sba_certified_8a_joint_venture = models.TextField(blank=True, null=True)
    dot_certified_disadvantage = models.TextField(blank=True, null=True)
    self_certified_small_disadvantaged_business = models.TextField(blank=True, null=True)
    historically_underutilized_business_zone = models.TextField(blank=True, null=True)
    small_disadvantaged_business = models.TextField(blank=True, null=True)
    the_ability_one_program = models.TextField(blank=True, null=True)
    historically_black_college = models.TextField(blank=True, null=True)
    c1862_land_grant_college = models.TextField(db_column='1862_land_grant_college', max_length=1, blank=True, null=True, verbose_name="1862 Land Grant College")  # Field renamed because it wasn't a valid Python identifier.
    c1890_land_grant_college = models.TextField(db_column='1890_land_grant_college', max_length=1, blank=True, null=True, verbose_name="1890 Land Grant College")  # Field renamed because it wasn't a valid Python identifier.
    c1994_land_grant_college = models.TextField(db_column='1994_land_grant_college', max_length=1, blank=True, null=True, verbose_name="1894 Land Grant College")  # Field renamed because it wasn't a valid Python identifier.
    minority_institution = models.TextField(blank=True, null=True)
    private_university_or_college = models.TextField(blank=True, null=True)
    school_of_forestry = models.TextField(blank=True, null=True)
    state_controlled_institution_of_higher_learning = models.TextField(blank=True, null=True)
    tribal_college = models.TextField(blank=True, null=True)
    veterinary_college = models.TextField(blank=True, null=True)
    educational_institution = models.TextField(blank=True, null=True)
    alaskan_native_servicing_institution = models.TextField(blank=True, null=True, verbose_name="Alaskan Native Owned Servicing Institution")
    community_development_corporation = models.TextField(blank=True, null=True)
    native_hawaiian_servicing_institution = models.TextField(blank=True, null=True)
    domestic_shelter = models.TextField(blank=True, null=True)
    manufacturer_of_goods = models.TextField(blank=True, null=True)
    hospital_flag = models.TextField(blank=True, null=True)
    veterinary_hospital = models.TextField(blank=True, null=True)
    hispanic_servicing_institution = models.TextField(blank=True, null=True)
    woman_owned_business = models.TextField(blank=True, null=True)
    minority_owned_business = models.TextField(blank=True, null=True)
    women_owned_small_business = models.TextField(blank=True, null=True)
    economically_disadvantaged_women_owned_small_business = models.TextField(blank=True, null=True)
    joint_venture_women_owned_small_business = models.TextField(blank=True, null=True)
    joint_venture_economic_disadvantaged_women_owned_small_bus = models.TextField(blank=True, null=True)
    veteran_owned_business = models.TextField(blank=True, null=True)
    service_disabled_veteran_owned_business = models.TextField(blank=True, null=True)
    contracts = models.TextField(blank=True, null=True)
    grants = models.TextField(blank=True, null=True)
    receives_contracts_and_grants = models.TextField(blank=True, null=True)
    airport_authority = models.TextField(blank=True, null=True, verbose_name="Airport Authority")
    council_of_governments = models.TextField(blank=True, null=True)
    housing_authorities_public_tribal = models.TextField(blank=True, null=True)
    interstate_entity = models.TextField(blank=True, null=True)
    planning_commission = models.TextField(blank=True, null=True)
    port_authority = models.TextField(blank=True, null=True)
    transit_authority = models.TextField(blank=True, null=True)
    foreign_owned_and_located = models.TextField(blank=True, null=True)
    american_indian_owned_business = models.TextField(blank=True, null=True, verbose_name="American Indian Owned Business")
    alaskan_native_owned_corporation_or_firm = models.TextField(blank=True, null=True, verbose_name="Alaskan Native Owned Corporation or Firm")
    indian_tribe_federally_recognized = models.TextField(blank=True, null=True)
    native_hawaiian_owned_business = models.TextField(blank=True, null=True)
    tribally_owned_business = models.TextField(blank=True, null=True)
    asian_pacific_american_owned_business = models.TextField(blank=True, null=True, verbose_name="Asian Pacific American Owned business")
    black_american_owned_business = models.TextField(blank=True, null=True)
    hispanic_american_owned_business = models.TextField(blank=True, null=True)
    native_american_owned_business = models.TextField(blank=True, null=True)
    subcontinent_asian_asian_indian_american_owned_business = models.TextField(blank=True, null=True)
    other_minority_owned_business = models.TextField(blank=True, null=True)
    us_local_government = models.TextField(blank=True, null=True)
    undefinitized_action = models.TextField(blank=True, null=True)
    domestic_or_foreign_entity = models.TextField(blank=True, null=True)
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

    def save(self, *args, **kwargs):
        LegalEntity.update_business_type_categories(self)
        super(LegalEntity, self).save(*args, **kwargs)

        LegalEntityOfficers.objects.get_or_create(legal_entity=self)

    @staticmethod
    def update_business_type_categories(le):
        # Business Category
        le.category_small_business = (
            le.small_business == "1" or

            le.business_types == "R"  # For-Profit Organization (Other than Small Business)
        )
        le.category_other_than_small_business = (
            le.business_types == "Q"  # For-Profit Organization (Other than Small Business)
        )
        le.category_business = (
            le.for_profit_organization == "1" or

            le.category_small_business or
            le.category_other_than_small_business
        )
        # End Business Category

        # Minority Owned Business Category
        le.category_alaskan_native_owned_business = (
            le.alaskan_native_owned_corporation_or_firm == "1"
        )
        le.category_american_indian_owned_business = (
            le.american_indian_owned_business == "1"
        )
        le.category_asian_pacific_american_owned_business = (
            le.asian_pacific_american_owned_business == "1"
        )
        le.category_black_american_owned_business = (
            le.black_american_owned_business == "1"
        )
        le.category_hispanic_american_owned_business = (
            le.hispanic_american_owned_business == "1"
        )
        le.category_native_american_owned_business = (
            le.native_american_owned_business == "1"
        )
        le.category_native_hawaiian_owned_business = (
            le.native_hawaiian_owned_business == "1"
        )
        le.category_subcontinent_asian_indian_american_owned_business = (
            le.subcontinent_asian_asian_indian_american_owned_business == "1"
        )
        le.category_tribally_owned_business = (
            le.tribally_owned_business == "1"
        )
        le.category_other_minority_owned_business = (
            le.other_minority_owned_business == "1"
        )
        le.category_minority_owned_business = (
            le.minority_owned_business == "1" or

            le.category_alaskan_native_owned_business or
            le.category_american_indian_owned_business or
            le.category_asian_pacific_american_owned_business or
            le.category_black_american_owned_business or
            le.category_hispanic_american_owned_business or
            le.category_native_american_owned_business or
            le.category_native_hawaiian_owned_business or
            le.category_subcontinent_asian_indian_american_owned_business or
            le.category_tribally_owned_business or
            le.category_other_minority_owned_business
        )
        # End Minority Owned Business Category

        # Woman Owned Business Category
        le.category_women_owned_small_business = (
            le.women_owned_small_business == "1"
        )
        le.category_economically_disadvantaged_women_owned_small_business = (
            le.economically_disadvantaged_women_owned_small_business == "1"
        )
        le.category_joint_venture_women_owned_small_business = (
            le.joint_venture_women_owned_small_business == "1"
        )
        le.category_joint_venture_economically_disadvantaged_women_owned_small_business = (
            le.joint_venture_economic_disadvantaged_women_owned_small_bus == "1"
        )
        le.category_woman_owned_business = (
            le.woman_owned_business == "1" or

            le.category_women_owned_small_business or
            le.category_economically_disadvantaged_women_owned_small_business or
            le.category_joint_venture_women_owned_small_business or
            le.category_joint_venture_economically_disadvantaged_women_owned_small_business
        )

        # Veteran Owned Business Category
        le.category_service_disabled_veteran_owned_business = (
            le.service_disabled_veteran_owned_business == "1"
        )
        le.category_veteran_owned_business = (
            le.veteran_owned_business == "1" or

            le.category_service_disabled_veteran_owned_business
        )
        # End Veteran Owned Business

        # Special Designations Category
        le.category_8a_program_participant = (
            le.c8a_program_participant == "1"
        )
        le.category_ability_one_program = (
            le.the_ability_one_program == "1"
        )
        le.category_dot_certified_disadvantaged_business_enterprise = (
            le.dot_certified_disadvantage == "1"
        )
        le.category_emerging_small_business = (
            le.emerging_small_business == "1"
        )
        le.category_federally_funded_research_and_development_corp = (
            le.federally_funded_research_and_development_corp == "1"
        )
        le.category_historically_underutilized_business_firm = (
            le.historically_underutilized_business_zone == "1"
        )
        le.category_labor_surplus_area_firm = (
            le.labor_surplus_area_firm == "1"
        )
        le.category_sba_certified_8a_joint_venture = (
            le.sba_certified_8a_joint_venture == "1"
        )
        le.category_self_certified_small_disadvanted_business = (
            le.self_certified_small_disadvantaged_business == "1"
        )
        le.category_small_agricultural_cooperative = (
            le.small_agricultural_cooperative == "1"
        )
        le.category_small_disadvantaged_business = (
            le.small_disadvantaged_business == "1"
        )
        le.category_community_developed_corporation_owned_firm = (
            le.community_developed_corporation_owned_firm == "1"
        )
        le.category_us_owned_business = (
            le.domestic_or_foreign_entity == "A"  # U.S. Owned Business
        )
        le.category_foreign_owned_and_us_located_business = (
            le.domestic_or_foreign_entity == "C"  # Foreign-Owned Business Incorporated in the U.S.
        )
        le.category_foreign_owned_and_located_business = (
            le.domestic_or_foreign_entity == "D" or  # Foreign-Owned Business Not Incorporated in the U.S.

            le.foreign_owned_and_located == "1"
        )
        le.category_foreign_government = (
            le.foreign_government == "1"
        )
        le.category_international_organization = (
            le.international_organization == "1"
        )
        le.category_special_designations = (
            le.category_8a_program_participant or
            le.category_ability_one_program or
            le.category_dot_certified_disadvantaged_business_enterprise or
            le.category_emerging_small_business or
            le.category_federally_funded_research_and_development_corp or
            le.category_historically_underutilized_business_firm or
            le.category_labor_surplus_area_firm or
            le.category_sba_certified_8a_joint_venture or
            le.category_self_certified_small_disadvanted_business or
            le.category_small_agricultural_cooperative or
            le.category_small_disadvantaged_business or
            le.category_community_developed_corporation_owned_firm or
            le.category_us_owned_business or
            le.category_foreign_owned_and_us_located_business or
            le.category_foreign_owned_and_located_business or
            le.category_foreign_government or
            le.category_international_organization
        )
        # End Special Designations

        # Non-profit category
        le.category_foundation = (
            le.foundation == "1"
        )
        le.category_community_development_corporations = (
            le.community_developed_corporation_owned_firm == "1"
        )
        le.category_nonprofit = (
            le.business_types == "M" or  # Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)
            le.business_types == "N" or  # Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)

            le.nonprofit_organization == "1" or
            le.other_not_for_profit_organization == "1" or

            le.category_foundation or
            le.category_community_development_corporations
        )
        # End Non-profit category

        # Higher Education Category
        le.category_public_institution_of_higher_education = (
            le.business_types == "H" or  # Public/State Controlled Institution of Higher Education

            le.state_controlled_institution_of_higher_learning == "1" or
            le.c1862_land_grant_college == "1" or
            le.c1890_land_grant_college == "1" or
            le.c1994_land_grant_college == "1"
        )
        le.category_private_institution_of_higher_education = (
            le.business_types == "O" or  # Private Institution of Higher Education

            le.private_university_or_college == "1"
        )
        le.category_minority_serving_institution_of_higher_education = (
            le.business_types == "T" or  # Historically Black Colleges and Universities (HBCUs)
            le.business_types == "U" or  # Tribally Controlled Colleges and Universities (TCCUs)
            le.business_types == "V" or  # Alaska Native and Native Hawaiian Serving Institutions
            le.business_types == "S" or  # Hispanic-serving Institution

            le.minority_institution == "1" or
            le.historically_black_college == "1" or
            le.tribal_college == "1" or
            le.alaskan_native_servicing_institution == "1" or
            le.native_hawaiian_servicing_institution == "1" or
            le.hispanic_servicing_institution == "1"
        )
        le.category_higher_education = (
            le.category_public_institution_of_higher_education or
            le.category_private_institution_of_higher_education or
            le.category_minority_serving_institution_of_higher_education
        )
        # End Higher Education Category

        # Government Category
        le.category_national_government = (
            le.us_federal_government == "1" or
            le.federal_agency == "1" or
            le.us_government_entity == "1" or
            le.interstate_entity == "1"
        )
        le.category_regional_and_state_government = (
            le.business_types == "A" or  # State government
            le.business_types == "E" or  # Regional Organization

            le.us_state_government == "1" or
            le.council_of_governments == "1"
        )
        le.category_us_territory_or_possession = (
            le.business_types == "F"  # U.S. Territory or Possession
        )
        le.category_local_government = (
            le.business_types == "C" or  # City or Township Government
            le.business_types == "B" or  # County Government
            le.business_types == "D" or  # Special District Government
            le.business_types == "G" or  # Independent School District

            le.city_local_government == "1" or
            le.county_local_government == "1" or
            le.inter_municipal_local_government == "1" or
            le.municipality_local_government == "1" or
            le.township_local_government == "1" or
            le.us_local_government == "1" or
            le.local_government_owned == "1" or
            le.school_district_local_government == "1"
        )
        le.category_indian_native_american_tribal_government = (
            le.business_types == "I" or  # Indian/Native American Tribal Government (Federally Recognized)
            le.business_types == "J" or  # Indian/Native American Tribal Government (Other than Federally Recognized)

            le.us_tribal_government == "1" or
            le.indian_tribe_federally_recognized == "1"
        )
        le.category_authorities_and_commissions = (
            le.business_types == "L" or  # Public/Indian Housing Authority

            le.housing_authorities_public_tribal == "1" or
            le.airport_authority == "1" or
            le.port_authority == "1" or
            le.transit_authority == "1" or
            le.planning_commission == "1"
        )
        le.category_government = (
            le.category_national_government or
            le.category_regional_and_state_government or
            le.category_us_territory_or_possession or
            le.category_local_government or
            le.category_indian_native_american_tribal_government or
            le.category_authorities_and_commissions
        )
        # End Government Category

        # Individuals Category
        le.category_individuals = (
            le.individual == "1" or
            le.business_types == "P"  # Individual
        )
        # End Individuals category

    @classmethod
    def get_or_create_by_duns(cls, duns):
        """
        Finds a legal entity with the matching duns, or creates it if it does
        not exist. If the duns is null, will always create a new instance.

        Returns a single legal entity instance, and a boolean indicating if the
        record was created or retrieved (i.e. mimicing the return of get_or_create)
        """
        if duns is None or len(duns) == 0:
            return cls.objects.create(), True
        else:
            return cls.objects.get_or_create(recipient_unique_id=duns)

    class Meta:
        managed = True
        db_table = 'legal_entity'


class LegalEntityOfficers(models.Model):
    legal_entity = models.OneToOneField(
        LegalEntity, on_delete=models.CASCADE,
        primary_key=True, related_name='officers')

    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)

    update_date = models.DateField(auto_now_add=True, blank=True, null=True)

    class Meta:
        managed = True


class ObjectClass(models.Model):
    major_object_class = models.TextField(db_index=True)
    major_object_class_name = models.TextField()
    object_class = models.TextField(db_index=True)
    object_class_name = models.TextField()
    direct_reimbursable = models.TextField(db_index=True, blank=True, null=True)
    direct_reimbursable_name = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'object_class'
        unique_together = ['object_class', 'direct_reimbursable']


class RefProgramActivity(models.Model):
    id = models.AutoField(primary_key=True)
    program_activity_code = models.TextField()
    program_activity_name = models.TextField()
    budget_year = models.TextField(blank=True, null=True)
    responsible_agency_id = models.TextField(blank=True, null=True)
    allocation_transfer_agency_id = models.TextField(blank=True, null=True)
    main_account_code = models.TextField(blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'ref_program_activity'
        unique_together = (('program_activity_code', 'budget_year', 'responsible_agency_id', 'allocation_transfer_agency_id', 'main_account_code'),)


class CFDAProgram(DataSourceTrackedModel):
    program_number = models.TextField(primary_key=True, max_length=7)
    program_title = models.TextField(blank=True, null=True)
    popular_name = models.TextField(blank=True, null=True)
    federal_agency = models.TextField(blank=True, null=True)
    authorization = models.TextField(blank=True, null=True)
    objectives = models.TextField(blank=True, null=True)
    types_of_assistance = models.TextField(blank=True, null=True)
    uses_and_use_restrictions = models.TextField(blank=True, null=True)
    applicant_eligibility = models.TextField(blank=True, null=True)
    beneficiary_eligibility = models.TextField(blank=True, null=True)
    credentials_documentation = models.TextField(blank=True, null=True)
    pre_application_coordination = models.TextField(blank=True, null=True)
    application_procedures = models.TextField(blank=True, null=True)
    award_procedure = models.TextField(blank=True, null=True)
    deadlines = models.TextField(blank=True, null=True)
    range_of_approval_disapproval_time = models.TextField(blank=True, null=True)
    website_address = models.TextField(blank=True, null=True)
    formula_and_matching_requirements = models.TextField(blank=True, null=True)
    length_and_time_phasing_of_assistance = models.TextField(blank=True, null=True)
    reports = models.TextField(blank=True, null=True)
    audits = models.TextField(blank=True, null=True)
    records = models.TextField(blank=True, null=True)
    account_identification = models.TextField(blank=True, null=True)
    obligations = models.TextField(blank=True, null=True)
    range_and_average_of_financial_assistance = models.TextField(blank=True, null=True)
    appeals = models.TextField(blank=True, null=True)
    renewals = models.TextField(blank=True, null=True)
    program_accomplishments = models.TextField(blank=True, null=True)
    regulations_guidelines_and_literature = models.TextField(blank=True, null=True)
    regional_or_local_office = models.TextField(blank=True, null=True)
    headquarters_office = models.TextField(blank=True, null=True)
    related_programs = models.TextField(blank=True, null=True)
    examples_of_funded_projects = models.TextField(blank=True, null=True)
    criteria_for_selecting_proposals = models.TextField(blank=True, null=True)
    url = models.TextField(blank=True, null=True)
    recovery = models.TextField(blank=True, null=True)
    omb_agency_code = models.TextField(blank=True, null=True)
    omb_bureau_code = models.TextField(blank=True, null=True)
    # published_date = models.DateTimeField(blank=True, null=True)
    # archived_date = models.DateTimeField(blank=True, null=True)
    published_date = models.TextField(blank=True, null=True)
    archived_date = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'cfda_program'

    def __str__(self):
        return "%s" % (self.program_title)


class Definition(models.Model):
    id = models.AutoField(primary_key=True)
    term = models.TextField(unique=True, db_index=True, blank=False, null=False)
    data_act_term = models.TextField(blank=True, null=True)
    plain = models.TextField()
    official = models.TextField(blank=True, null=True)
    slug = models.SlugField(max_length=500, null=True)
    resources = models.TextField(blank=True, null=True)

    def save(self, *arg, **kwarg):
        self.slug = slugify(self.term)
        return super(Definition, self).save(*arg, **kwarg)
