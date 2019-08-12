# LEGACY CODE - NOT USED ANYMORE

import usaspending_api.etl.helpers as h

#
#
# class Command(BaseCommand):
#     help = "Loads contracts from a usaspending contracts download. \
#             Usage: `python manage.py load_usaspending_contracts source_file_path`"
#     logger = logging.getLogger('console')
#
#     def add_arguments(self, parser):
#         parser.add_argument('file', nargs=1, help='the file to load')
#
#     def handle(self, *args, **options):
#
#         h.clear_caches()
#
#         csv_file = options['file'][0]
#         self.logger.info("Starting load for file {}".format(csv_file))
#
#         # Create the csv reader
#         reader = CsvDataReader(csv_file)
#
#         # Create a new submission attributes object for this timestamp
#         subattr = SubmissionAttributes()
#         subattr.usaspending_update = datetime.now()
#         subattr.save()
#
#         # Create lists to hold model instances for bulk insert
#         txn_list = []
#         txn_contract_list = []
#
#         subtier_agency_dict = h.get_subtier_agency_dict()
#
#         for idx, row in enumerate(reader):
#             if len(reader) % 1000 == 0:
#                 self.logger.info("Read row {}".format(len(reader)))
#             row = h.cleanse_values(row)
#
#             awarding_agency_id = self.get_agency_id(row["contractingofficeagencyid"], subtier_agency_dict)
#
#             # Create the transaction object for this row
#             txn_dict = {
#                 "action_date": h.convert_date(row['signeddate']),
#                 "award": self.get_or_create_award(row, awarding_agency_id),
#                 "awarding_agency_id": awarding_agency_id,
#                 "description": row["descriptionofcontractrequirement"],
#                 "federal_action_obligation": row["dollarsobligated"],
#                 "funding_agency_id": self.get_agency_id(row["fundingrequestingagencyid"], subtier_agency_dict),
#                 "last_modified_date": h.convert_date(row['last_modified_date']),
#                 "modification_number": row["modnumber"],
#                 "place_of_performance": h.get_or_create_location(
#                     row, mapper=location_mapper_place_of_performance),
#                 "period_of_performance_current_end_date": h.convert_date(row['currentcompletiondate']),
#                 "period_of_performance_start_date": h.convert_date(row['effectivedate']),
#                 "recipient": self.get_or_create_recipient(row),
#                 "type": evaluate_contract_award_type(row),
#                 "action_type": h.up2colon(row['reasonformodification']),
#                 "usaspending_unique_transaction_id": row["unique_transaction_id"]
#             }
#             txn = TransactionNormalized(**txn_dict)
#             txn.fiscal_year = fy(txn.action_date)
#             txn_list.append(txn)
#
#             # Create the transaction contract object for this row
#             txn_contract_dict = {
#                 "piid": row['piid'],
#                 "parent_award_id": row['idvpiid'],
#                 "current_total_value_award": h.parse_numeric_value(row["baseandexercisedoptionsvalue"]),
#                 "period_of_perf_potential_e": h.convert_date(row['ultimatecompletiondate']),
#                 "potential_total_value_awar": h.parse_numeric_value(row["baseandalloptionsvalue"]),
#                 "epa_designated_product": self.parse_first_character(row['useofepadesignatedproducts']),
#                 "gfe_gfp": h.up2colon(row['gfe_gfp']),
#                 "cost_or_pricing_data": h.up2colon(row['costorpricingdata']),
#                 "type_of_contract_pricing": h.up2colon(row['typeofcontractpricing']),
#                 "multiple_or_single_award_idv": h.up2colon(row['multipleorsingleawardidc']),
#                 "naics": h.up2colon(row['nationalinterestactioncode']),
#                 "dod_claimant_program_code": h.up2colon(row['claimantprogramcode']),
#                 "commercial_item_acquisition_procedures": h.up2colon(
#                     row['commercialitemacquisitionprocedures']),
#                 "commercial_item_test_program": h.up2colon(row['commercialitemtestprogram']),
#                 "consolidated_contract": h.up2colon(row['consolidatedcontract']),
#                 "contingency_humanitarian_or_peacekeeping_operation": h.up2colon(
#                     row['contingencyhumanitarianpeacekeepingoperation']),
#                 "contract_bundling": h.up2colon(row['contractbundling']),
#                 "contract_financing": h.up2colon(row['contractfinancing']),
#                 "contracting_officers_determination_of_business_size": h.up2colon(
#                     row['contractingofficerbusinesssizedetermination']),
#                 "country_of_product_or_service_origin": h.up2colon(row['countryoforigin']),
#                 "davis_bacon_act": h.up2colon(row['davisbaconact']),
#                 "evaluated_preference": h.up2colon(row['evaluatedpreference']),
#                 "extent_competed": h.up2colon(row['extentcompeted']),
#                 "information_technology_commercial_item_category": h.up2colon(
#                     row['informationtechnologycommercialitemcategory']),
#                 "interagency_contracting_authority": h.up2colon(row['interagencycontractingauthority']),
#                 "local_area_set_aside": h.up2colon(row['localareasetaside']),
#                 "purchase_card_as_payment_method": h.up2colon(row['purchasecardaspaymentmethod']),
#                 "multi_year_contract": h.up2colon(row['multiyearcontract']),
#                 "national_interest_action": h.up2colon(row['nationalinterestactioncode']),
#                 "number_of_actions": h.up2colon(row['numberofactions']),
#                 "number_of_offers_received": h.up2colon(row['numberofoffersreceived']),
#                 "performance_based_service_acquisition": h.up2colon(row['performancebasedservicecontract']),
#                 "place_of_manufacture": h.up2colon(row['placeofmanufacture']),
#                 "product_or_service_code": h.up2colon(row['productorservicecode']),
#                 "recovered_materials_sustainability": h.up2colon(row['recoveredmaterialclauses']),
#                 "research": h.up2colon(row['research']),
#                 "sea_transportation": h.up2colon(row['seatransportation']),
#                 "service_contract_act": h.up2colon(row['servicecontractact']),
#                 "small_business_competitiveness_demonstration_program": self.parse_first_character(
#                     row['smallbusinesscompetitivenessdemonstrationprogram']),
#                 "solicitation_procedures": h.up2colon(row['solicitationprocedures']),
#                 "subcontracting_plan": h.up2colon(row['subcontractplan']),
#                 "type_set_aside": h.up2colon(row['typeofsetaside']),
#                 "walsh_healey_act": h.up2colon(row['walshhealyact']),
#                 "rec_flag": self.parse_first_character(h.up2colon(row['rec_flag'])),
#                 "type_of_idc": self.parse_first_character(row['typeofidc']),
#                 "a76_fair_act_action": self.parse_first_character(row['a76action']),
#                 "clinger_cohen_act_planning": self.parse_first_character(row['clingercohenact']),
#                 "cost_accounting_standards": self.parse_first_character(
#                     row['costaccountingstandardsclause']),
#                 "fed_biz_opps": self.parse_first_character(row['fedbizopps']),
#                 "foreign_funding": self.parse_first_character(row['fundedbyforeignentity']),
#                 "major_program": self.parse_first_character(row['majorprogramcode']),
#                 "program_acronym": self.parse_first_character(row['programacronym']),
#                 "referenced_idv_modification_number": self.parse_first_character(
#                     row['idvmodificationnumber']),
#                 "transaction_number": self.parse_first_character(row['transactionnumber']),
#                 "solicitation_identifier": self.parse_first_character(row['solicitationid'])
#             }
#             txn_contract = TransactionFPDS(**txn_contract_dict)
#             txn_contract_list.append(txn_contract)
#
#         # Bulk insert transaction rows
#         self.logger.info("Starting Transaction bulk insert ({} records)".format(len(txn_list)))
#         TransactionNormalized.objects.bulk_create(txn_list)
#         self.logger.info("Completed Transaction bulk insert")
#         # Update txn contract list with newly-inserted transactions
#         award_id_list = []  # we'll need this when updating the awards later on
#         for idx, t in enumerate(txn_contract_list):
#             # add transaction info to this TransactionFPDS object
#             t.transaction = txn_list[idx]
#             # add the corresponding award id to a list we'll use when batch-updating award data
#             award_id_list.append(txn_list[idx].award_id)
#         # Bulk insert transaction contract rows
#         self.logger.info("Starting TransactionFPDS bulk insert ({} records)".format(len(txn_contract_list)))
#         TransactionFPDS.objects.bulk_create(txn_contract_list)
#         self.logger.info("Completed TransactionFPDS bulk insert")
#
#         # Update awards to reflect latest transaction information
#         # (note that this can't be done via signals or a save()
#         # override in the model itself, because those aren't
#         # triggered by a bulk update
#         self.logger.info("Starting Awards update")
#         count = update_awards(tuple(award_id_list))
#         update_contract_awards(tuple(award_id_list))
#         update_model_description_fields()
#         self.logger.info("Completed Awards update ({} records)".format(count))
#
#     def get_agency_id(self, agency_string, subtier_agency_dict):
#         agency_code = h.up2colon(agency_string)
#         agency_id = subtier_agency_dict.get(agency_code)
#         if not agency_id:
#             self.logger.error("Missing agency: " + agency_string)
#         return agency_id
#
#     def get_or_create_recipient(self, row):
#         recipient_dict = {
#             "location_id": h.get_or_create_location(row, mapper=location_mapper_vendor).location_id,
#             "recipient_name": row['vendorname'],
#             "vendor_phone_number": row['phoneno'],
#             "vendor_fax_number": row['faxno'],
#             "parent_recipient_unique_id": row['parentdunsnumber'],
#             "city_local_government": self.parse_first_character(row['iscitylocalgovernment']),
#             "county_local_government": self.parse_first_character(row['iscountylocalgovernment']),
#             "inter_municipal_local_government": self.parse_first_character(row['isintermunicipallocalgovernment']),
#             "local_government_owned": self.parse_first_character(row['islocalgovernmentowned']),
#             "municipality_local_government": self.parse_first_character(row['ismunicipalitylocalgovernment']),
#             "school_district_local_government": self.parse_first_character(row['isschooldistrictlocalgovernment']),
#             "township_local_government": self.parse_first_character(row['istownshiplocalgovernment']),
#             "federal_agency": self.parse_first_character(row['isfederalgovernmentagency']),
#             "us_federal_government": self.parse_first_character(row['federalgovernmentflag']),
#             "federally_funded_research_and_development_corp":
#               self.parse_first_character(row['isfederallyfundedresearchanddevelopmentcorp']),
#             "us_tribal_government": self.parse_first_character(row['istriballyownedfirm']),
#             "c8a_program_participant": self.parse_first_character(row['firm8aflag']),
#             "foreign_government": self.parse_first_character(row['isforeigngovernment']),
#             "community_developed_corporation_owned_firm":
#                 self.parse_first_character(row['iscommunitydevelopedcorporationownedfirm']),
#             "labor_surplus_area_firm": self.parse_first_character(row['islaborsurplusareafirm']),
#             "small_agricultural_cooperative": self.parse_first_character(row['issmallagriculturalcooperative']),
#             "international_organization": self.parse_first_character(row['isinternationalorganization']),
#             "c1862_land_grant_college": self.parse_first_character(row['is1862landgrantcollege']),
#             "c1890_land_grant_college": self.parse_first_character(row['is1890landgrantcollege']),
#             "c1994_land_grant_college": self.parse_first_character(row['is1994landgrantcollege']),
#             "minority_institution": self.parse_first_character(row['minorityinstitutionflag']),
#             "private_university_or_college": self.parse_first_character(row['isprivateuniversityorcollege']),
#             "school_of_forestry": self.parse_first_character(row['isschoolofforestry']),
#             "state_controlled_institution_of_higher_learning":
#                   self.parse_first_character(row['isstatecontrolledinstitutionofhigherlearning']),
#             "tribal_college": self.parse_first_character(row['istribalcollege']),
#             "veterinary_college": self.parse_first_character(row['isveterinarycollege']),
#             "educational_institution": self.parse_first_character(row['educationalinstitutionflag']),
#             "alaskan_native_servicing_institution":
#                   self.parse_first_character(row['isalaskannativeownedcorporationorfirm']),
#             "community_development_corporation":
#                   self.parse_first_character(row['iscommunitydevelopmentcorporation']),
#             "native_hawaiian_servicing_institution":
#                   self.parse_first_character(row['isnativehawaiianownedorganizationorfirm']),
#             "domestic_shelter": self.parse_first_character(row['isdomesticshelter']),
#             "manufacturer_of_goods": self.parse_first_character(row['ismanufacturerofgoods']),
#             "hospital_flag": self.parse_first_character(row['hospitalflag']),
#             "veterinary_hospital": self.parse_first_character(row['isveterinaryhospital']),
#             "hispanic_servicing_institution": self.parse_first_character(row['ishispanicservicinginstitution']),
#             "woman_owned_business": self.parse_first_character(row['womenownedflag']),
#             "minority_owned_business": self.parse_first_character(row['minorityownedbusinessflag']),
#             "women_owned_small_business": self.parse_first_character(row['iswomenownedsmallbusiness']),
#             "economically_disadvantaged_women_owned_small_business":
#                   self.parse_first_character(row['isecondisadvwomenownedsmallbusiness']),
#             "joint_venture_economic_disadvantaged_women_owned_small_bus":
#                   self.parse_first_character(row['isjointventureecondisadvwomenownedsmallbusiness']),
#             "veteran_owned_business": self.parse_first_character(row['veteranownedflag']),
#             "airport_authority": self.parse_first_character(row['isairportauthority']),
#             "housing_authorities_public_tribal":
#                   self.parse_first_character(row['ishousingauthoritiespublicortribal']),
#             "interstate_entity": self.parse_first_character(row['isinterstateentity']),
#             "planning_commission": self.parse_first_character(row['isplanningcommission']),
#             "port_authority": self.parse_first_character(row['isportauthority']),
#             "transit_authority": self.parse_first_character(row['istransitauthority']),
#             "foreign_owned_and_located": self.parse_first_character(row['isforeignownedandlocated']),
#             "american_indian_owned_business": self.parse_first_character(row['aiobflag']),
#             "alaskan_native_owned_corporation_or_firm":
#                   self.parse_first_character(row['isalaskannativeownedcorporationorfirm']),
#             "indian_tribe_federally_recognized": self.parse_first_character(row['isindiantribe']),
#             "native_hawaiian_owned_business":
#                   self.parse_first_character(row['isnativehawaiianownedorganizationorfirm']),
#             "tribally_owned_business": self.parse_first_character(row['istriballyownedfirm']),
#             "asian_pacific_american_owned_business": self.parse_first_character(row['apaobflag']),
#             "black_american_owned_business": self.parse_first_character(row['baobflag']),
#             "hispanic_american_owned_business": self.parse_first_character(row['haobflag']),
#             "native_american_owned_business": self.parse_first_character(row['naobflag']),
#             "historically_black_college": self.parse_first_character(row['hbcuflag']),
#             "subcontinent_asian_asian_indian_american_owned_business": self.parse_first_character(row['saaobflag']),
#             "us_local_government": self.parse_first_character(row['islocalgovernmentowned']),
#             "division_name": self.parse_first_character(row['divisionname']),
#             "division_number": self.parse_first_character(row['divisionnumberorofficecode']),
#             "historically_underutilized_business_zone": self.parse_first_character(row['hubzoneflag']),
#             "corporate_entity_tax_exempt": self.parse_first_character(row['iscorporateentitytaxexempt']),
#             "corporate_entity_not_tax_exempt": self.parse_first_character(row['iscorporateentitynottaxexempt']),
#             "council_of_governments": self.parse_first_character(row['iscouncilofgovernments']),
#             "dot_certified_disadvantage":
#                   self.parse_first_character(row['isdotcertifieddisadvantagedbusinessenterprise']),
#             "for_profit_organization": self.parse_first_character(row['isforprofitorganization']),
#             "foundation": self.parse_first_character(row['isfoundation']),
#             "joint_venture_women_owned_small_business":
#                   self.parse_first_character(row['isjointventurewomenownedsmallbusiness']),
#             "limited_liability_corporation": self.parse_first_character(row['islimitedliabilitycorporation']),
#             "other_not_for_profit_organization": self.parse_first_character(row['isothernotforprofitorganization']),
#             "other_minority_owned_business": self.parse_first_character(row['isotherminorityowned']),
#             "partnership_or_limited_liability_partnership":
#                   self.parse_first_character(row['ispartnershiporlimitedliabilitypartnership']),
#             "sole_proprietorship": self.parse_first_character(row['issoleproprietorship']),
#             "subchapter_scorporation": self.parse_first_character(row['issubchapterscorporation']),
#             "nonprofit_organization": self.parse_first_character(row['nonprofitorganizationflag']),
#         }
#
#         le, created = LegalEntity.objects.get_or_create(
#             recipient_unique_id=row['dunsnumber'],
#             recipient_name=row['vendorname']
#         )
#         if created:
#             # Update from our recipient dictionary
#             for attr, value in recipient_dict.items():
#                 setattr(le, attr, value)
#             le.save()
#
#     def get_or_create_award(self, row, awarding_agency_id):
#         piid = row.get("piid", None)
#         parent_award_id = row.get("idvpiid", None)
#
#         awarding_agency = Agency.objects.get(id=awarding_agency_id)
#         created, award = Award.get_or_create_summary_award(
#             piid=piid, fain=None, uri=None, awarding_agency=awarding_agency,
#             parent_award_id=parent_award_id)
#         return award
#
#     def parse_first_character(self, flag):
#         if len(flag) > 0:
#             return flag[0]


def evaluate_contract_award_type(row):
    first_element = h.up2colon(row["contractactiontype"].split()[0])

    if len(first_element) == 1:
        return first_element
    else:
        cat = row["contractactiontype"].lower()
        # Not using DAIMS enumeration . . .
        if "bpa" in cat:
            return "A"
        elif "purchase" in cat:
            return "B"
        elif "delivery" in cat:
            return "C"
        elif "definitive" in cat:
            return "D"
        else:
            return None


def location_mapper_place_of_performance(row):

    loc = {
        "city_name": row.get("placeofperformancecity", ""),
        "congressional_code": row.get("placeofperformancecongressionaldistrict", "")[
            2:
        ],  # Need to strip the state off the front
        "location_country_code": row.get("placeofperformancecountrycode", ""),
        "location_zip": row.get("placeofperformancezipcode", "").replace(
            "-", ""
        ),  # Either ZIP5, or ZIP5+4, sometimes with hypens
        "state_code": h.up2colon(
            row.get("pop_state_code", "")
        ),  # Format is VA: VIRGINIA, so we need to grab the first bit
    }
    return loc


def location_mapper_vendor(row):
    loc = {
        "city_name": row.get("city", ""),
        "congressional_code": row.get("vendor_cd", "").zfill(2),  # Need to add leading zeroes here
        "location_country_code": row.get(
            "vendorcountrycode", ""
        ),  # Never actually a country code, just the string name
        "location_zip": row.get("zipcode", "").replace("-", ""),
        "state_code": h.up2colon(row.get("vendor_state_code", "")),
        "address_line1": row.get("streetaddress", ""),
        "address_line2": row.get("streetaddress2", ""),
        "address_line3": row.get("streetaddress3", ""),
    }
    return loc
