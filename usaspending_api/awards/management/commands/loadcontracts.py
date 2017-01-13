import logging
from datetime import datetime

from django.core.management.base import BaseCommand

import usaspending_api.awards.management.commands.helpers as h
from usaspending_api.awards.models import Procurement
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from usaspending_api.references.models import LegalEntity
from usaspending_api.submissions.models import SubmissionAttributes


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
            "modification_number": "modnumber",
            "parent_award_id": "idvpiid",
            "usaspending_unique_transaction_id": "unique_transaction_id"
        }

        value_map = {
            "data_source": "USA",
            "recipient": lambda row: self.get_or_create_recipient(row),
            "award": lambda row: h.get_or_create_award(row),
            "place_of_performance": lambda row: h.get_or_create_location(row, "place_of_performance"),
            "awarding_agency": lambda row: h.get_agency(row["contractingofficeagencyid"]),
            "funding_agency": lambda row: h.get_agency(row["fundingrequestingagencyid"]),
            "action_date": lambda row: h.convert_date(row['signeddate']),
            "last_modified_date": lambda row: h.convert_date(row['last_modified_date']),
            "gfe_gfp": lambda row: h.up2colon(row['gfe_gfp']),
            "multiple_or_single_award_idv": lambda row: h.up2colon(row['multipleorsingleawardidc']),
            "cost_or_pricing_data": lambda row: h.up2colon(row['costorpricingdata']),
            "type_of_contract_pricing": lambda row: h.up2colon(row['typeofcontractpricing']),
            "contract_award_type": evaluate_contract_award_type,
            "naics": lambda row: h.up2colon(row['nationalinterestactioncode']),
            "multiple_or_single_award_idv": lambda row: h.up2colon(row['multipleorsingleawardidc']),
            "dod_claimant_program_code": lambda row: h.up2colon(row['claimantprogramcode']),
            "commercial_item_acquisition_procedures": lambda row: h.up2colon(row['commercialitemacquisitionprocedures']),
            "commercial_item_test_program": lambda row: h.up2colon(row['commercialitemtestprogram']),
            "consolidated_contract": lambda row: h.up2colon(row['consolidatedcontract']),
            "contingency_humanitarian_or_peacekeeping_operation": lambda row: h.up2colon(row['contingencyhumanitarianpeacekeepingoperation']),
            "contract_bundling": lambda row: h.up2colon(row['contractbundling']),
            "contract_financing": lambda row: h.up2colon(row['contractfinancing']),
            "contracting_officers_determination_of_business_size": lambda row: h.up2colon(row['contractingofficerbusinesssizedetermination']),
            "country_of_product_or_service_origin": lambda row: h.up2colon(row['countryoforigin']),
            "davis_bacon_act": lambda row: h.up2colon(row['davisbaconact']),
            "evaluated_preference": lambda row: h.up2colon(row['evaluatedpreference']),
            "extent_competed": lambda row: h.up2colon(row['extentcompeted']),
            "information_technology_commercial_item_category": lambda row: h.up2colon(row['informationtechnologycommercialitemcategory']),
            "interagency_contracting_authority": lambda row: h.up2colon(row['interagencycontractingauthority']),
            "local_area_set_aside": lambda row: h.up2colon(row['localareasetaside']),
            "purchase_card_as_payment_method": lambda row: h.up2colon(row['purchasecardaspaymentmethod']),
            "multi_year_contract": lambda row: h.up2colon(row['multiyearcontract']),
            "national_interest_action": lambda row: h.up2colon(row['nationalinterestactioncode']),
            "number_of_actions": lambda row: h.up2colon(row['numberofactions']),
            "number_of_offers_received": lambda row: h.up2colon(row['numberofoffersreceived']),
            "performance_based_service_acquisition": lambda row: h.up2colon(row['performancebasedservicecontract']),
            "place_of_manufacture": lambda row: h.up2colon(row['placeofmanufacture']),
            "product_or_service_code": lambda row: h.up2colon(row['productorservicecode']),
            "recovered_materials_sustainability": lambda row: h.up2colon(row['recoveredmaterialclauses']),
            "research": lambda row: h.up2colon(row['research']),
            "sea_transportation": lambda row: h.up2colon(row['seatransportation']),
            "service_contract_act": lambda row: h.up2colon(row['servicecontractact']),
            "small_business_competitiveness_demonstration_program": lambda row: self.parse_first_character(row['smallbusinesscompetitivenessdemonstrationprogram']),
            "solicitation_procedures": lambda row: h.up2colon(row['solicitationprocedures']),
            "subcontracting_plan": lambda row: h.up2colon(row['subcontractplan']),
            "type_set_aside": lambda row: h.up2colon(row['typeofsetaside']),
            "walsh_healey_act": lambda row: h.up2colon(row['walshhealyact']),
            "rec_flag": lambda row:  h.up2colon(self.parse_first_character(row['rec_flag'])),
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

    def get_or_create_recipient(self, row):
        recipient_dict = {
            "location_id": h.get_or_create_location(row).location_id,
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
