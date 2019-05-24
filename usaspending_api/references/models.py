import logging
import re

from django.db import models
from django.db.models import F, Q
from django.utils.text import slugify
from django.contrib.postgres.fields import ArrayField, JSONField

from usaspending_api.common.models import DataSourceTrackedModel, DeleteIfChildlessMixin
from usaspending_api.references.abbreviations import code_to_state, state_to_code
from usaspending_api.references.helpers import canonicalize_string


class GTASTotalObligation(models.Model):
    fiscal_year = models.IntegerField()
    fiscal_quarter = models.IntegerField()
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = 'gtas_total_obligation'
        unique_together = (('fiscal_year', 'fiscal_quarter'),)


class RefCityCountyCode(models.Model):
    city_county_code_id = models.AutoField(primary_key=True)
    state_code = models.TextField(blank=True, null=True)
    city_name = models.TextField(blank=True, null=True, db_index=True)
    city_code = models.TextField(blank=True, null=True)
    county_code = models.TextField(blank=True, null=True, db_index=True)
    county_name = models.TextField(blank=True, null=True, db_index=True)
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

    toptier_agency = models.ForeignKey('ToptierAgency', models.DO_NOTHING, null=True, db_index=True)
    subtier_agency = models.ForeignKey('SubtierAgency', models.DO_NOTHING, null=True, db_index=True)
    office_agency = models.ForeignKey('OfficeAgency', models.DO_NOTHING, null=True, db_index=True)

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

    @staticmethod
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

    @staticmethod
    def get_by_subtier_only(subtier_code):
        """
        Lookup an Agency record by subtier code only

        Useful when data source has an inaccurate top tier code,
        but an accurate subtier code.  Will return an Agency
        if and only if a single match for the subtier code exists.

        Args:
            subtier_code: an agency subtier code

        Returns:
            an Agency instance

        """
        agencies = Agency.objects.filter(
            subtier_agency__subtier_code=subtier_code)
        if agencies.count() == 1:
            return agencies.first()
        else:
            return None

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
    cgac_code = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Code", db_index=True)
    fpds_code = models.TextField(blank=True, null=True)
    abbreviation = models.TextField(blank=True, null=True, verbose_name="Agency Abbreviation")
    name = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Name", db_index=True)
    mission = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Mission Statement")
    website = models.URLField(blank=True, null=True, verbose_name="Top-Tier Agency Website")
    justification = models.URLField(blank=True, null=True, verbose_name="Top-Tier Agency Congressional Justification")
    icon_filename = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Icon Filename")

    class Meta:
        managed = True
        db_table = 'toptier_agency'


class SubtierAgency(models.Model):
    subtier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    subtier_code = models.TextField(blank=True, null=True, verbose_name="Sub-Tier Agency Code")
    abbreviation = models.TextField(blank=True, null=True, verbose_name="Agency Abbreviation")
    name = models.TextField(blank=True, null=True, verbose_name="Sub-Tier Agency Name", db_index=True)

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


class FilterHash(models.Model):
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    filter = JSONField(blank=True, null=True, verbose_name="JSON of Filter")
    hash = models.TextField(blank=False, unique=True, verbose_name="Hash of JSON Filter")

    class Meta:
        managed = True
        db_table = 'filter_hash'


class Location(DataSourceTrackedModel, DeleteIfChildlessMixin):
    location_id = models.BigAutoField(primary_key=True)
    location_country_code = models.TextField(blank=True, null=True, verbose_name="Location Country Code")
    country_name = models.TextField(blank=True, null=True, verbose_name="Country Name")
    state_code = models.TextField(blank=True, null=True, verbose_name="State Code")
    state_name = models.TextField(blank=True, null=True, verbose_name="State Name")
    state_description = models.TextField(blank=True, null=True, verbose_name="State Description")
    city_name = models.TextField(blank=True, null=True, verbose_name="City Name")
    city_code = models.TextField(blank=True, null=True)
    county_name = models.TextField(blank=True, null=True, db_index=False)
    county_code = models.TextField(blank=True, null=True, db_index=True)
    address_line1 = models.TextField(blank=True, null=True, verbose_name="Address Line 1")
    address_line2 = models.TextField(blank=True, null=True, verbose_name="Address Line 2")
    address_line3 = models.TextField(blank=True, null=True, verbose_name="Address Line 3")
    foreign_location_description = models.TextField(blank=True, null=True)
    zip4 = models.TextField(blank=True, null=True, verbose_name="ZIP+4")
    zip_4a = models.TextField(blank=True, null=True)
    congressional_code = models.TextField(blank=True, null=True, verbose_name="Congressional District Code")
    performance_code = models.TextField(
        blank=True, null=True, verbose_name="Primary Place Of Performance Location Code")
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

    # location_unique = models.TextField(blank=True, null=True, db_index=True)

    # Tags whether this location is used as a place of performance or a recipient
    # location, or both
    place_of_performance_flag = models.BooleanField(default=False, verbose_name="Location used as place of performance")
    recipient_flag = models.BooleanField(default=False, verbose_name="Location used as recipient location")
    is_fpds = models.BooleanField(blank=False, null=False, default=False, verbose_name="Is FPDS")
    transaction_unique_id = models.TextField(blank=False, null=False, default="NONE",
                                             verbose_name="Transaction Unique ID")

    def pre_save(self):
        self.load_city_county_data()
        self.fill_missing_state_data()
        self.fill_missing_zip5()

    def save(self, *args, **kwargs):
        self.pre_save()
        super(Location, self).save(*args, **kwargs)

    def fill_missing_state_data(self):
        """Fills in blank US state names or codes from its counterpart"""

        if self.state_code and self.state_name:
            return
        if self.country_name == 'UNITED STATES':
            if not self.state_code:
                self.state_code = state_to_code.get(self.state_name)
            elif not self.state_name:
                state_obj = code_to_state.get(self.state_code)

                if state_obj:
                    self.state_name = state_obj['name']

    zip_code_pattern = re.compile(r'^(\d{5})\-?(\d{4})?$')

    def fill_missing_zip5(self):
        """Where zip5 is blank, fill from a valid zip4, if avaliable"""

        if self.zip4 and not self.zip5:
            match = self.zip_code_pattern.match(self.zip4)
            if match:
                self.zip5 = match.group(1)

    def load_city_county_data(self):
        # Here we fill in missing information from the ref city county code data
        if self.location_country_code == "USA":

            # TODO: this should be checked to see if this is even necessary... are these fields always uppercased?
            if self.state_code:
                temp_state_code = self.state_code.upper()
            else:
                temp_state_code = None

            if self.city_name:
                temp_city_name = self.city_name.upper()
            else:
                temp_city_name = None

            if self.county_name:
                temp_county_name = self.county_name.upper()
            else:
                temp_county_name = None

            q_kwargs = {
                "city_code": self.city_code,
                "county_code": self.county_code,
                "state_code": temp_state_code,
                "city_name": temp_city_name,
                "county_name": temp_county_name
            }
            # Clear out any blank or None values in our filter, so we can find the best match
            q_kwargs = dict((k, v) for k, v in q_kwargs.items() if v)

            # if q_kwargs = {} the filter below will return everything. There's no point in continuing if nothing is
            # being filtered
            if not q_kwargs:
                return

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
                logging.getLogger('debug').info("Could not find single matching city/county for following arguments:" +
                                                str(q_kwargs) + "; got " + str(matched_reference.count()))


class LegalEntity(DataSourceTrackedModel):
    legal_entity_id = models.BigAutoField(primary_key=True, db_index=True)
    recipient_name = models.TextField(blank=True, verbose_name="Recipient Name", null=True)
    location = models.ForeignKey('Location', models.DO_NOTHING, null=True)
    parent_recipient_unique_id = models.TextField(blank=True, null=True, verbose_name="Parent DUNS Number",
                                                  db_index=True)
    parent_recipient_name = models.TextField(blank=True, verbose_name="Parent Recipient Name", null=True)
    vendor_doing_as_business_name = models.TextField(blank=True, null=True)
    vendor_phone_number = models.TextField(blank=True, null=True)
    vendor_fax_number = models.TextField(blank=True, null=True)
    business_types = models.TextField(blank=True, null=True, db_index=True)
    business_types_description = models.TextField(blank=True, null=True)
    '''
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
    '''
    business_categories = ArrayField(models.TextField(), default=list)

    recipient_unique_id = models.TextField(blank=True, default='', null=True, verbose_name="DUNS Number", db_index=True)
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
        db_column='8a_program_participant', max_length=1, blank=False, default=False,
        verbose_name="8a Program Participant")  # Field renamed because it wasn't a valid Python identifier.
    sba_certified_8a_joint_venture = models.NullBooleanField(blank=False, default=False)
    dot_certified_disadvantage = models.NullBooleanField(blank=False, default=False)
    self_certified_small_disadvantaged_business = models.NullBooleanField(blank=False, default=False)
    historically_underutilized_business_zone = models.NullBooleanField(blank=False, default=False)
    small_disadvantaged_business = models.NullBooleanField(blank=False, default=False)
    the_ability_one_program = models.NullBooleanField(blank=False, default=False)
    historically_black_college = models.NullBooleanField(blank=False, default=False)
    c1862_land_grant_college = models.NullBooleanField(
        db_column='1862_land_grant_college',
        max_length=1,
        blank=False,
        default=False,
        verbose_name="1862 Land Grant College")  # Field renamed because it wasn't a valid Python identifier.
    c1890_land_grant_college = models.NullBooleanField(
        db_column='1890_land_grant_college',
        max_length=1,
        blank=False,
        default=False,
        verbose_name="1890 Land Grant College")  # Field renamed because it wasn't a valid Python identifier.
    c1994_land_grant_college = models.NullBooleanField(
        db_column='1994_land_grant_college',
        max_length=1,
        blank=False,
        default=False,
        verbose_name="1894 Land Grant College")  # Field renamed because it wasn't a valid Python identifier.
    minority_institution = models.NullBooleanField(blank=False, default=False)
    private_university_or_college = models.NullBooleanField(blank=False, default=False)
    school_of_forestry = models.NullBooleanField(blank=False, default=False)
    state_controlled_institution_of_higher_learning = models.NullBooleanField(blank=False, default=False)
    tribal_college = models.NullBooleanField(blank=False, default=False)
    veterinary_college = models.NullBooleanField(blank=False, default=False)
    educational_institution = models.NullBooleanField(blank=False, default=False)
    alaskan_native_servicing_institution = models.NullBooleanField(
        blank=False, default=False, verbose_name="Alaskan Native Owned Servicing Institution")
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
        blank=False, default=False, verbose_name="American Indian Owned Business")
    alaskan_native_owned_corporation_or_firm = models.NullBooleanField(
        blank=False, default=False, verbose_name="Alaskan Native Owned Corporation or Firm")
    indian_tribe_federally_recognized = models.NullBooleanField(blank=False, default=False)
    native_hawaiian_owned_business = models.NullBooleanField(blank=False, default=False)
    tribally_owned_business = models.NullBooleanField(blank=False, default=False)
    asian_pacific_american_owned_business = models.NullBooleanField(
        blank=False, default=False, verbose_name="Asian Pacific American Owned business")
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
    transaction_unique_id = models.TextField(
        blank=False, default="NONE", verbose_name="Transaction Unique ID")

    class Meta:
        managed = True
        db_table = 'legal_entity'
        index_together = ['recipient_unique_id', 'recipient_name', 'update_date']


class LegalEntityOfficers(models.Model):
    legal_entity = models.OneToOneField(LegalEntity, on_delete=models.CASCADE, primary_key=True,
                                        related_name='officers')
    duns = models.TextField(blank=True, default='', null=True, verbose_name="DUNS Number", db_index=True)
    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

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

    @property
    def corrected_major_object_class_name(self):
        if self.major_object_class == '00':
            return 'Unknown Object Type'
        else:
            return self.major_object_class_name


class RefProgramActivity(models.Model):
    id = models.AutoField(primary_key=True)
    program_activity_code = models.TextField()
    program_activity_name = models.TextField(blank=True, null=True)
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
        unique_together = (('program_activity_code',
                            'program_activity_name',
                            'responsible_agency_id',
                            'allocation_transfer_agency_id',
                            'main_account_code',
                            'budget_year'),)


class Cfda(DataSourceTrackedModel):
    program_number = models.TextField(null=False, unique=True, db_index=True)
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
    published_date = models.TextField(blank=True, null=True)
    archived_date = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True

    def __str__(self):
        return "%s" % self.program_title


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


class OverallTotals(models.Model):
    id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    fiscal_year = models.IntegerField(blank=True, null=True)
    total_budget_authority = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'overall_totals'


class FrecMap(models.Model):
    """Used to find FR_entity_code for load_budget_authority"""

    id = models.AutoField(primary_key=True)
    agency_identifier = models.TextField(null=False, db_index=True)  # source: AID
    main_account_code = models.TextField(null=False, db_index=True)  # source: MAIN
    treasury_appropriation_account_title = models.TextField(null=False, db_index=True)  # source: GWA_TAS_NAME
    sub_function_code = models.TextField(null=False, db_index=True)  # source: Sub Function Code; dest: Subfunction Code
    fr_entity_code = models.TextField(null=False)  # source: FR Entity Type

    class Meta:
        db_table = 'frec_map'


class NAICS(models.Model):
    """Based on United States Census Bureau"""
    code = models.TextField(primary_key=True)
    description = models.TextField(null=False)
    year = models.IntegerField(default=0)

    class Meta:
        managed = True
        db_table = 'naics'


class PSC(models.Model):
    """Based on https://www.acquisition.gov/PSC_Manual"""
    code = models.CharField(primary_key=True, max_length=4)
    description = models.TextField(null=False)

    class Meta:
        managed = True
        db_table = 'psc'


class Rosetta(models.Model):
    """
        Based on the "public" tab in Schema's Rosetta Crosswalk Data Dictionary:
            Data Transparency Rosetta Stone_All_Versions.xlsx
    """
    document_name = models.TextField(primary_key=True)
    document = JSONField()

    class Meta:
        managed = True
        db_table = "rosetta"
