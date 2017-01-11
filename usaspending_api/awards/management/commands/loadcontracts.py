from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q
from usaspending_api.awards.models import Award, Procurement
from usaspending_api.references.models import LegalEntity, Agency, RefCountryCode, Location
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from datetime import datetime
import csv
import logging
import django


class Command(BaseCommand):
    help = "Loads contracts from a usaspending contracts download. \
            Usage: `python manage.py loadcontracts source_file_path`"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):
        # Create a new submission attributes object for this timestamp
        subattr = SubmissionAttributes()
        subattr.usaspending_update = datetime.now()
        subattr.save()

        field_map = {
            "federal_action_obligation": "dollarsobligated",
            "description": "descriptionofcontractrequirement",
            "other_statutory_authority": "otherstatutoryauthority",
            "modification_number": "modnumber"
        }

        value_map = {
            "data_source": "USA",
            "award": lambda row: self.create_or_get_award(row),
            "awarding_agency": lambda row: self.get_agency(row),
            "action_date": lambda row: self.convert_date(row['signeddate']),
            "last_modified_date": lambda row: self.convert_date(row['last_modified_date']),
            "gfe_gfp": lambda row: row['gfe_gfp'].split(":")[0],
            "multiple_or_single_award_idv": lambda row: row['multipleorsingleawardidc'].split(' ')[0].split(':')[0],
            "cost_or_pricing_data": lambda row: row['costorpricingdata'].split(' ')[0].split(':')[0],
            "type_of_contract_pricing": lambda row: row['typeofcontractpricing'].split(' ')[0].split(':')[0],
            "contract_award_type": evaluate_contract_award_type,
            "naics": lambda row: row['nationalinterestactioncode'].split(' ')[0].split(':')[0],
            "multiple_or_single_award_idv": lambda row: row['multipleorsingleawardidc'].split(' ')[0].split(':')[0],
            "dod_claimant_program_code": lambda row: row['claimantprogramcode'].split(' ')[0].split(':')[0],
            "commercial_item_acquisition_procedures": lambda row: row['commercialitemacquisitionprocedures'].split(' ')[0].split(':')[0],
            "commercial_item_test_program": lambda row: row['commercialitemtestprogram'].split(' ')[0].split(':')[0],
            "consolidated_contract": lambda row: row['consolidatedcontract'].split(' ')[0].split(':')[0],
            "contingency_humanitarian_or_peacekeeping_operation": lambda row: row['contingencyhumanitarianpeacekeepingoperation'].split(' ')[0].split(':')[0],
            "contract_bundling": lambda row: row['contractbundling'].split(' ')[0].split(':')[0],
            "contract_financing": lambda row: row['contractfinancing'].split(' ')[0].split(':')[0],
            "contracting_officers_determination_of_business_size": lambda row: row['contractingofficerbusinesssizedetermination'].split(' ')[0].split(':')[0],
            "country_of_product_or_service_origin": lambda row: row['countryoforigin'].split(' ')[0].split(':')[0],
            "davis_bacon_act": lambda row: row['davisbaconact'].split(' ')[0].split(':')[0],
            "evaluated_preference": lambda row: row['evaluatedpreference'].split(' ')[0].split(':')[0],
            "extent_competed": lambda row: row['extentcompeted'].split(' ')[0].split(':')[0],
            "information_technology_commercial_item_category": lambda row: row['informationtechnologycommercialitemcategory'].split(' ')[0].split(':')[0],
            "interagency_contracting_authority": lambda row: row['interagencycontractingauthority'].split(' ')[0].split(':')[0],
            "local_area_set_aside": lambda row: row['localareasetaside'].split(' ')[0].split(':')[0],
            "purchase_card_as_payment_method": lambda row: row['purchasecardaspaymentmethod'].split(' ')[0].split(':')[0],
            "multi_year_contract": lambda row: row['multiyearcontract'].split(' ')[0].split(':')[0],
            "national_interest_action": lambda row: row['nationalinterestactioncode'].split(' ')[0].split(':')[0],
            "number_of_actions": lambda row: row['numberofactions'].split(' ')[0].split(':')[0],
            "number_of_offers_received": lambda row: row['numberofoffersreceived'].split(' ')[0].split(':')[0],
            "performance_based_service_acquisition": lambda row: row['performancebasedservicecontract'].split(' ')[0].split(':')[0],
            "place_of_manufacture": lambda row: row['placeofmanufacture'].split(' ')[0].split(':')[0],
            "product_or_service_code": lambda row: row['productorservicecode'].split(' ')[0].split(':')[0],
            "recovered_materials_sustainability": lambda row: row['recoveredmaterialclauses'].split(' ')[0].split(':')[0],
            "research": lambda row: row['research'].split(' ')[0].split(':')[0],
            "sea_transportation": lambda row: row['seatransportation'].split(' ')[0].split(':')[0],
            "service_contract_act": lambda row: row['servicecontractact'].split(' ')[0].split(':')[0],
            "small_business_competitiveness_demonstration_program": lambda row: self.parse_first_character(row['smallbusinesscompetitivenessdemonstrationprogram']),
            "solicitation_procedures": lambda row: row['solicitationprocedures'].split(' ')[0].split(':')[0],
            "subcontracting_plan": lambda row: row['subcontractplan'].split(' ')[0].split(':')[0],
            "type_set_aside": lambda row: row['typeofsetaside'].split(' ')[0].split(':')[0],
            "walsh_healey_act": lambda row: row['walshhealyact'].split(' ')[0].split(':')[0],
            "rec_flag": lambda row:  self.parse_first_character(row['rec_flag'].split(' ')[0].split(':')[0]),
            "type_of_idc": lambda row: self.parse_first_character(row['typeofidc']),
            "a76_fair_act_action": lambda row: self.parse_first_character(row['a76action']),
            "clinger_cohen_act_planning": lambda row: self.parse_first_character(row['clingercohenact']),
            "cost_accounting_standards": lambda row: self.parse_first_character(row['costaccountingstandardsclause']),
            "fed_biz_opps": lambda row: self.parse_first_character(row['fedbizopps']),
            "foreign_funding": lambda row: self.parse_first_character(row['fundedbyforeignentity']),
            "major_program": lambda row: self.parse_first_character(row['majorprogramcode']),
            "program_acronym": lambda row: self.parse_first_character(row['programacronym']),
            "referenced_idv_modification_number": lambda row: self.parse_first_character(row['idvmodificationnumber']),
            "transaction_number": lambda row: self.parse_first_character(row['transactionnumber']),
            "solicitation_identifier": lambda row: self.parse_first_character(row['solicitationid']),
            "submission": subattr
        }

        loader = ThreadedDataLoader(Procurement, field_map=field_map, value_map=value_map)
        loader.load_from_file(options['file'][0])

    def get_agency(self, row):
        agency = Agency.objects.filter(subtier_code=self.get_agency_code(row['maj_agency_cat'])).first()
        if not agency:
            print("Missing agency: " + row['maj_agency_cat'])
        return agency

    def create_or_get_recipient(self, row):
        # First, create the locations

        recipient_dict = {
            "location_id": get_or_create_location(row).location_id,
            "recipient_name": row['vendorname'],
            "vendor_phone_number": row['phoneno'],
            "vendor_fax_number": row['faxno'],
            "recipient_unique_id": row['dunsnumber'],
            "city_local_government": self.parse_first_character(row['iscitylocalgovernment']),
            "county_local_government": self.parse_first_character(row['iscountylocalgovernment']),
            "inter_municipal_local_government": self.parse_first_character(row['isintermunicipallocalgovernment']),
            "local_government_owned": self.parse_first_character(row['islocalgovernmentowned']),
            "municipality_local_government": self.parse_first_character(row['ismunicipalitylocalgovernment']),
            "school_district_local_government": self.parse_first_character(row['isschooldistrictlocalgovernment']),
            "township_local_government": self.parse_first_character(row['istownshiplocalgovernment']),
            "federal_agency": self.parse_first_character(row['isfederalgovernmentagency']),
            "federally_funded_research_and_development_corp": self.parse_first_character(row['isfederallyfundedresearchanddevelopmentcorp']),
            "us_tribal_government": self.parse_first_character(row['istriballyownedfirm']),
            "foreign_government": self.parse_first_character(row['isforeigngovernment']),
            "community_developed_corporation_owned_firm": self.parse_first_character(row['iscommunitydevelopedcorporationownedfirm']),
            "labor_surplus_area_firm": self.parse_first_character(row['islaborsurplusareafirm']),
            "small_agricultural_cooperative": self.parse_first_character(row['issmallagriculturalcooperative']),
            "international_organization": self.parse_first_character(row['isinternationalorganization']),
            "c1862_land_grant_college": self.parse_first_character(row['is1862landgrantcollege']),
            "c1890_land_grant_college": self.parse_first_character(row['is1890landgrantcollege']),
            "c1994_land_grant_college": self.parse_first_character(row['is1994landgrantcollege']),
            "minority_institution": self.parse_first_character(row['minorityinstitutionflag']),
            "private_university_or_college": self.parse_first_character(row['isprivateuniversityorcollege']),
            "school_of_forestry": self.parse_first_character(row['isschoolofforestry']),
            "state_controlled_institution_of_higher_learning": self.parse_first_character(row['isstatecontrolledinstitutionofhigherlearning']),
            "tribal_college": self.parse_first_character(row['istribalcollege']),
            "veterinary_college": self.parse_first_character(row['isveterinarycollege']),
            "educational_institution": self.parse_first_character(row['educationalinstitutionflag']),
            "alaskan_native_servicing_institution": self.parse_first_character(row['isalaskannativeownedcorporationorfirm']),
            "community_development_corporation": self.parse_first_character(row['iscommunitydevelopmentcorporation']),
            "native_hawaiian_servicing_institution": self.parse_first_character(row['isnativehawaiianownedorganizationorfirm']),
            "domestic_shelter": self.parse_first_character(row['isdomesticshelter']),
            "manufacturer_of_goods": self.parse_first_character(row['ismanufacturerofgoods']),
            "hospital_flag": self.parse_first_character(row['hospitalflag']),
            "veterinary_hospital": self.parse_first_character(row['isveterinaryhospital']),
            "hispanic_servicing_institution": self.parse_first_character(row['ishispanicservicinginstitution']),
            "woman_owned_business": self.parse_first_character(row['womenownedflag']),
            "minority_owned_business": self.parse_first_character(row['minorityownedbusinessflag']),
            "women_owned_small_business": self.parse_first_character(row['iswomenownedsmallbusiness']),
            "economically_disadvantaged_women_owned_small_business": self.parse_first_character(row['isecondisadvwomenownedsmallbusiness']),
            "joint_venture_women_owned_small_business": self.parse_first_character(row['isjointventureecondisadvwomenownedsmallbusiness']),
            "joint_venture_economic_disadvantaged_women_owned_small_bus": self.parse_first_character(row['isjointventureecondisadvwomenownedsmallbusiness']),
            "veteran_owned_business": self.parse_first_character(row['veteranownedflag']),
            "airport_authority": self.parse_first_character(row['isairportauthority']),
            "housing_authorities_public_tribal": self.parse_first_character(row['ishousingauthoritiespublicortribal']),
            "interstate_entity": self.parse_first_character(row['isinterstateentity']),
            "planning_commission": self.parse_first_character(row['isplanningcommission']),
            "port_authority": self.parse_first_character(row['isportauthority']),
            "transit_authority": self.parse_first_character(row['istransitauthority']),
            "foreign_owned_and_located": self.parse_first_character(row['isforeignownedandlocated']),
            "american_indian_owned_business": self.parse_first_character(row['aiobflag']),
            "alaskan_native_owned_corporation_or_firm": self.parse_first_character(row['isalaskannativeownedcorporationorfirm']),
            "indian_tribe_federally_recognized": self.parse_first_character(row['isindiantribe']),
            "native_hawaiian_owned_business": self.parse_first_character(row['isnativehawaiianownedorganizationorfirm']),
            "tribally_owned_business": self.parse_first_character(row['istriballyownedfirm']),
            "asian_pacific_american_owned_business": self.parse_first_character(row['apaobflag']),
            "black_american_owned_business": self.parse_first_character(row['baobflag']),
            "hispanic_american_owned_business": self.parse_first_character(row['baobflag']),
            "native_american_owned_business": self.parse_first_character(row['naobflag']),
            "subcontinent_asian_asian_indian_american_owned_business": self.parse_first_character(row['saaobflag']),
            "us_local_government": self.parse_first_character(row['islocalgovernmentowned']),
            "division_name": self.parse_first_character(row['divisionname']),
            "division_number": self.parse_first_character(row['divisionnumberorofficecode'])
        }

        le = LegalEntity.objects.filter(recipient_unique_id=row['dunsnumber']).first()
        if not le:
            le = LegalEntity.objects.create(**recipient_dict)

        return le

    def parse_first_character(self, flag):
        if len(flag) > 0:
            return flag[0]

    def convert_date(self, date):
        return datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

    def get_agency_code(self, maj_agency_cat):
        return maj_agency_cat.split(' ')[0].split(':')[0]

    def create_or_get_award(self, row):
        piid = row.get("piid", None)
        award = Award.objects.filter(piid=piid, parent_award=None).first()
        if not award:
            award = Award.objects.create(piid=piid, parent_award=None)
        award.recipient = self.create_or_get_recipient(row)
        award.save()
        return award


def evaluate_contract_award_type(row):
    first_element = row['contractactiontype'].split(' ')[0].split(':')[0]
    if len(first_element) == 1:
        return first_element
    else:
        cat = row['contractactiontype'].lower()
        # Not using DAIMS enumeration . . .
        if 'bpa' in cat:
            return 'A'
        elif 'purchase' in cat:
            return 'B'
        elif 'delivery' in cat:
            return 'C'
        elif 'definitive' in cat:
            return 'D'
        else:
            return None


def fetch_country_code(vendor_country_code):
    code_str = vendor_country_code.split(":")[0]

    country_code = RefCountryCode.objects.filter(
        Q(country_code=code_str) | Q(country_name__iexact=code_str)).first()
    if not country_code:
        # We don't have an exact match on the name or the code, so we need to
        # chain filter on the name
        query_set = RefCountryCode.objects
        for word in code_str.split():
            query_set = query_set.filter(country_name__icontains=word)
        country_code = query_set.first()
    
    return country_code


def get_or_create_location(row):
    country_code = fetch_country_code(row['vendorcountrycode'])
    location_dict = {
        "location_country_code": country_code,
        "location_address_line1": row["streetaddress"],
        "location_address_line2": row["streetaddress2"],
        "location_address_line3": row["streetaddress3"],
    }

    if country_code.country_code == "USA":
        location_dict.update(
            location_zip5=row["zipcode"][:5],
            location_zip_last4=row["zipcode"].replace("-", "")[5:],
            location_state_code=row["state"],
            location_city_name=row["city"],
        )
    else:
        location_dict.update(
            location_foreign_postal_code=row["zipcode"],
            location_foreign_province=row["state"],
            location_foreign_city_name=row["city"],
        )

    recipient_location = Location.objects.filter(Q(**location_dict)).first()
    if not recipient_location:
        recipient_location = Location.objects.create(**location_dict)
    return recipient_location
