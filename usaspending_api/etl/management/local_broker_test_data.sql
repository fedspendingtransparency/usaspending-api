--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.1
-- Dumped by pg_dump version 9.6.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: local_broker; Type: SCHEMA; Schema: -; Owner: catherine
--

CREATE SCHEMA local_broker;


ALTER SCHEMA local_broker OWNER TO catherine;

SET search_path = local_broker, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: award_financial_assistance; Type: TABLE; Schema: local_broker; Owner: catherine
--

CREATE TABLE award_financial_assistance (
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    award_financial_assistance_id integer,
    submission_id integer,
    job_id integer,
    row_number integer,
    action_date text,
    action_type text,
    assistance_type text,
    award_description text,
    awardee_or_recipient_legal text,
    awardee_or_recipient_uniqu text,
    awarding_agency_code text,
    awarding_agency_name text,
    awarding_office_code text,
    awarding_office_name text,
    awarding_sub_tier_agency_c text,
    awarding_sub_tier_agency_n text,
    award_modification_amendme text,
    business_funds_indicator text,
    business_types text,
    cfda_number text,
    cfda_title text,
    correction_late_delete_ind text,
    face_value_loan_guarantee text,
    fain text,
    federal_action_obligation numeric,
    fiscal_year_and_quarter_co text,
    funding_agency_code text,
    funding_agency_name text,
    funding_office_name text,
    funding_office_code text,
    funding_sub_tier_agency_co text,
    funding_sub_tier_agency_na text,
    legal_entity_address_line1 text,
    legal_entity_address_line2 text,
    legal_entity_address_line3 text,
    legal_entity_city_code text,
    legal_entity_city_name text,
    legal_entity_congressional text,
    legal_entity_country_code text,
    legal_entity_county_code text,
    legal_entity_county_name text,
    legal_entity_foreign_city text,
    legal_entity_foreign_posta text,
    legal_entity_foreign_provi text,
    legal_entity_state_code text,
    legal_entity_state_name text,
    legal_entity_zip5 text,
    legal_entity_zip_last4 text,
    non_federal_funding_amount text,
    original_loan_subsidy_cost text,
    period_of_performance_curr text,
    period_of_performance_star text,
    place_of_performance_city text,
    place_of_performance_code text,
    place_of_performance_congr text,
    place_of_perform_country_c text,
    place_of_perform_county_na text,
    place_of_performance_forei text,
    place_of_perform_state_nam text,
    place_of_performance_zip4a text,
    record_type text,
    sai_number text,
    total_funding_amount text,
    uri text
);


ALTER TABLE award_financial_assistance OWNER TO catherine;

--
-- Name: award_procurement; Type: TABLE; Schema: local_broker; Owner: catherine
--

CREATE TABLE award_procurement (
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    award_procurement_id integer,
    submission_id integer,
    job_id integer,
    row_number integer,
    piid text,
    awarding_sub_tier_agency_c text,
    awarding_sub_tier_agency_n text,
    awarding_agency_code text,
    awarding_agency_name text,
    parent_award_id text,
    award_modification_amendme text,
    type_of_contract_pricing text,
    contract_award_type text,
    naics text,
    naics_description text,
    awardee_or_recipient_uniqu text,
    ultimate_parent_legal_enti text,
    ultimate_parent_unique_ide text,
    award_description text,
    place_of_performance_zip4a text,
    place_of_performance_congr text,
    awardee_or_recipient_legal text,
    legal_entity_city_name text,
    legal_entity_state_code text,
    legal_entity_zip4 text,
    legal_entity_congressional text,
    legal_entity_address_line1 text,
    legal_entity_address_line2 text,
    legal_entity_address_line3 text,
    legal_entity_country_code text,
    legal_entity_country_name text,
    period_of_performance_star text,
    period_of_performance_curr text,
    period_of_perf_potential_e text,
    ordering_period_end_date text,
    action_date text,
    action_type text,
    federal_action_obligation numeric,
    current_total_value_award text,
    potential_total_value_awar text,
    funding_sub_tier_agency_co text,
    funding_sub_tier_agency_na text,
    funding_office_code text,
    funding_office_name text,
    awarding_office_code text,
    awarding_office_name text,
    referenced_idv_agency_iden text,
    funding_agency_code text,
    funding_agency_name text,
    place_of_performance_locat text,
    place_of_performance_state text,
    place_of_perform_country_c text,
    idv_type text,
    vendor_doing_as_business_n text,
    vendor_phone_number text,
    vendor_fax_number text,
    multiple_or_single_award_i text,
    type_of_idc text,
    a_76_fair_act_action text,
    dod_claimant_program_code text,
    clinger_cohen_act_planning text,
    commercial_item_acquisitio text,
    commercial_item_test_progr text,
    consolidated_contract text,
    contingency_humanitarian_o text,
    contract_bundling text,
    contract_financing text,
    contracting_officers_deter text,
    cost_accounting_standards text,
    cost_or_pricing_data text,
    country_of_product_or_serv text,
    davis_bacon_act text,
    evaluated_preference text,
    extent_competed text,
    fed_biz_opps text,
    foreign_funding text,
    government_furnished_equip text,
    information_technology_com text,
    interagency_contracting_au text,
    local_area_set_aside text,
    major_program text,
    purchase_card_as_payment_m text,
    multi_year_contract text,
    national_interest_action text,
    number_of_actions text,
    number_of_offers_received text,
    other_statutory_authority text,
    performance_based_service text,
    place_of_manufacture text,
    price_evaluation_adjustmen text,
    product_or_service_code text,
    program_acronym text,
    other_than_full_and_open_c text,
    recovered_materials_sustai text,
    research text,
    sea_transportation text,
    service_contract_act text,
    small_business_competitive text,
    solicitation_identifier text,
    solicitation_procedures text,
    fair_opportunity_limited_s text,
    subcontracting_plan text,
    program_system_or_equipmen text,
    type_set_aside text,
    epa_designated_product text,
    walsh_healey_act text,
    transaction_number text,
    sam_exception text,
    city_local_government text,
    county_local_government text,
    inter_municipal_local_gove text,
    local_government_owned text,
    municipality_local_governm text,
    school_district_local_gove text,
    township_local_government text,
    us_state_government text,
    us_federal_government text,
    federal_agency text,
    federally_funded_research text,
    us_tribal_government text,
    foreign_government text,
    community_developed_corpor text,
    labor_surplus_area_firm text,
    corporate_entity_not_tax_e text,
    corporate_entity_tax_exemp text,
    partnership_or_limited_lia text,
    sole_proprietorship text,
    small_agricultural_coopera text,
    international_organization text,
    us_government_entity text,
    emerging_small_business text,
    c8a_program_participant text,
    sba_certified_8_a_joint_ve text,
    dot_certified_disadvantage text,
    self_certified_small_disad text,
    historically_underutilized text,
    small_disadvantaged_busine text,
    the_ability_one_program text,
    historically_black_college text,
    c1862_land_grant_college text,
    c1890_land_grant_college text,
    c1994_land_grant_college text,
    minority_institution text,
    private_university_or_coll text,
    school_of_forestry text,
    state_controlled_instituti text,
    tribal_college text,
    veterinary_college text,
    educational_institution text,
    alaskan_native_servicing_i text,
    community_development_corp text,
    native_hawaiian_servicing text,
    domestic_shelter text,
    manufacturer_of_goods text,
    hospital_flag text,
    veterinary_hospital text,
    hispanic_servicing_institu text,
    foundation text,
    woman_owned_business text,
    minority_owned_business text,
    women_owned_small_business text,
    economically_disadvantaged text,
    joint_venture_women_owned text,
    joint_venture_economically text,
    veteran_owned_business text,
    service_disabled_veteran_o text,
    contracts text,
    grants text,
    receives_contracts_and_gra text,
    airport_authority text,
    council_of_governments text,
    housing_authorities_public text,
    interstate_entity text,
    planning_commission text,
    port_authority text,
    transit_authority text,
    subchapter_s_corporation text,
    limited_liability_corporat text,
    foreign_owned_and_located text,
    american_indian_owned_busi text,
    alaskan_native_owned_corpo text,
    indian_tribe_federally_rec text,
    native_hawaiian_owned_busi text,
    tribally_owned_business text,
    asian_pacific_american_own text,
    black_american_owned_busin text,
    hispanic_american_owned_bu text,
    native_american_owned_busi text,
    subcontinent_asian_asian_i text,
    other_minority_owned_busin text,
    for_profit_organization text,
    nonprofit_organization text,
    other_not_for_profit_organ text,
    us_local_government text,
    referenced_idv_modificatio text,
    undefinitized_action text,
    domestic_or_foreign_entity text
);


ALTER TABLE award_procurement OWNER TO catherine;

--
-- Data for Name: award_financial_assistance; Type: TABLE DATA; Schema: local_broker; Owner: catherine
--

COPY award_financial_assistance (created_at, updated_at, award_financial_assistance_id, submission_id, job_id, row_number, action_date, action_type, assistance_type, award_description, awardee_or_recipient_legal, awardee_or_recipient_uniqu, awarding_agency_code, awarding_agency_name, awarding_office_code, awarding_office_name, awarding_sub_tier_agency_c, awarding_sub_tier_agency_n, award_modification_amendme, business_funds_indicator, business_types, cfda_number, cfda_title, correction_late_delete_ind, face_value_loan_guarantee, fain, federal_action_obligation, fiscal_year_and_quarter_co, funding_agency_code, funding_agency_name, funding_office_name, funding_office_code, funding_sub_tier_agency_co, funding_sub_tier_agency_na, legal_entity_address_line1, legal_entity_address_line2, legal_entity_address_line3, legal_entity_city_code, legal_entity_city_name, legal_entity_congressional, legal_entity_country_code, legal_entity_county_code, legal_entity_county_name, legal_entity_foreign_city, legal_entity_foreign_posta, legal_entity_foreign_provi, legal_entity_state_code, legal_entity_state_name, legal_entity_zip5, legal_entity_zip_last4, non_federal_funding_amount, original_loan_subsidy_cost, period_of_performance_curr, period_of_performance_star, place_of_performance_city, place_of_performance_code, place_of_performance_congr, place_of_perform_country_c, place_of_perform_county_na, place_of_performance_forei, place_of_perform_state_nam, place_of_performance_zip4a, record_type, sai_number, total_funding_amount, uri) FROM stdin;
2017-07-14 17:15:14.053958	2017-07-14 17:15:14.053966	19537719	4057	56179	1126	20170110	C	04	FY2013 Juvenile Accountability Block Grant	Idaho Department of Juvenile Corrections	026467840	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	A	16.523	Juvenile Accountability Block Grants	\N	\N	2013JBFX0007	0.0000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	954 W JEFFERSON ST	\N	\N	\N	BOISE	02	USA	001	ADA	\N	\N	\N	ID	Idaho	83702	5436	\N	\N	20170930	20131217	BOISE	ID08830	02	USA	\N	\N	Idaho	83702-5436	2	SAI NOT AVAILABLE	0.0000	FY2013-2013JBFX0007-00-NON-01
2017-07-14 17:15:13.853822	2017-07-14 17:15:13.85383	19537694	4057	56179	1101	20170124	C	05	BJA FY15 Strategic Initiatives Implementation Fellow	George Mason University	077817450	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	X	16.828	Swift and Certain Sanctions/Replicating the Concepts Behind Project HOPE	\N	\N	2015HOBXK014	0.0000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	4400 UNIVERSITY DR	\N	\N	\N	FAIRFAX	11	USA	600	FAIRFAX CITY	\N	\N	\N	VA	Virginia	22030	4422	\N	\N	20180331	20151001	FAIRFAX	VA26496	11	USA	\N	\N	Virginia	22030-4422	2	SAI NOT AVAILABLE	0.0000	FY2017-2015HOBXK014-00-NON-01
2017-07-14 17:15:14.719326	2017-07-14 17:15:14.719334	19537806	4057	56179	1213	20170207	C	04	OVC FY 13 VOCA Victim Assistance Formula	Office of the Attorney General	855031761	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	A	16.575	Crime Victim Assistance	\N	\N	2013VAGX0064	-219.4600	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	590 SOUTH MARINE CORP DRIVE	STE 706	\N	\N	TAMUNING	98	USA	010	GUAM	\N	\N	\N	GU	Guam	96913	3537	\N	\N	20160930	20121001	\N	GU**010	98	USA	Guam	\N	Guam	\N	2	SAI NOT AVAILABLE	-219.4600	FY2017-2013VAGX0064-00-NON-01
2017-07-14 17:15:14.712064	2017-07-14 17:15:14.712073	19537805	4057	56179	1212	20170124	C	04	The Positive S.T.E.P.S. Reentry Program is a multi-facted jail reentry program that was implemented in 2007 by the City of Petersburg  Virginia Sheriff's Office.	City of Petersburg	069519986	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	X	16.738	Edward Byrne Memorial Justice Assistance Grant Program	\N	\N	2013DJBX1007	-6500.0000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	8 COURTHOUSE AVENUE	\N	\N	\N	PETERSBURG	04	USA	730	PETERSBURG CITY	\N	\N	\N	VA	Virginia	23803	4559	\N	\N	20160930	20121001	PETERSBURG	VA61832	04	USA	\N	\N	Virginia	23803-4559	2	SAI NOT AVAILABLE	-6500.0000	FY2017-2013DJBX1007-00-NON-01
2017-07-14 17:15:09.776103	2017-07-14 17:15:09.776111	19537197	4057	56179	604	20170224	C	05	OVW Fiscal Year 2013 Safe Havens:Supervised Visitation and Safe Exchange Grant Program	Shelby County Commission	075461137	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	B	16.527	Supervised Visitation, Safe Havens for Children	\N	\N	2013FLAXK006	-1303.0500	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	200 W COLLEGE ST	P. O. Box 467	\N	\N	COLUMBIANA	06	USA	117	SHELBY	\N	\N	\N	AL	Alabama	35051	9734	\N	\N	20161130	20131001	COLUMBIANA	AL16768	06	USA	\N	\N	Alabama	35051-9734	2	SAI NOT AVAILABLE	-1303.0500	FY2013-2013FLAXK006-00-NON-01
2017-07-14 17:15:09.54084	2017-07-14 17:15:09.540848	19537168	4057	56179	575	20170221	C	04	Juvenile Accountability Block Grant	Michigan Department of Health and Human Serivces	113704139	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	A	16.523	Juvenile Accountability Block Grants	\N	\N	2011JBFX0037	-375.6100	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	320 S WALNUT ST	\N	\N	\N	LANSING	08	USA	065	INGHAM	\N	\N	\N	MI	Michigan	48933	2014	\N	\N	20150102	20120103	LANSING	MI46000	08	USA	\N	\N	Michigan	48933-2014	2	SAI NOT AVAILABLE	-375.6100	FY2011-2011JBFX0037-00-NON-01
2017-07-14 17:15:08.689025	2017-07-14 17:15:08.689034	19537069	4057	56179	476	20170206	C	04	FY 2014 Forensic DNA Backlog Reduction Program - Kentucky State Police	Commonwealth of Kentucky	927678110	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	A	16.741	Forensic DNA Backlog Reduction Program	\N	\N	2014DNBX0039	-6.1000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	919 VERSAILLES RD	Suite 102	\N	\N	FRANKFORT	06	USA	073	FRANKLIN	\N	\N	\N	KY	Kentucky	40601	8272	\N	\N	20160930	20141001	FRANKFORT	KY28900	06	USA	\N	\N	Kentucky	40601-8272	2	SAI NOT AVAILABLE	-6.1000	FY2014-2014DNBX0039-00-NON-01
2017-07-14 17:15:06.290041	2017-07-14 17:15:06.290049	19536782	4057	56179	189	20170118	C	04	FY 15 JAG Program	ESPANOLA POLICE DEPARTMENT	089307797	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	C	16.738	Edward Byrne Memorial Justice Assistance Grant Program	\N	\N	2015DJBX0908	0.0000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	405 NORTH PASEO DE ONATE	\N	\N	\N	ESPANOLA	03	USA	039	RIO ARRIBA	\N	\N	\N	NM	New Mexico	87532	2619	\N	\N	20170331	20141001	ESPANOLA	NM25170	03	USA	\N	\N	New Mexico	87532-2619	2	SAI NOT AVAILABLE	0.0000	FY2017-2015DJBX0908-00-NON-01
2017-07-14 17:15:05.718176	2017-07-14 17:15:05.718184	19536717	4057	56179	124	20170105	C	04	2015 Paul Coverdell Forensic Science Improvement Grants Program-Washington State Application	Washington State Patrol	808883854	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	A	16.742	Paul Coverdell Forensic Sciences Improvement Grant Program	\N	\N	2015CDBX0057	-17058.1900	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	210 11TH AVE SW RM116	\N	\N	\N	OLYMPIA	10	USA	067	THURSTON	\N	\N	\N	WA	Washington	98504	2602	\N	\N	20160930	20151001	OLYMPIA	WA51300	10	USA	\N	\N	Washington	98504-2602	2	SAI NOT AVAILABLE	-17058.1900	FY2017-2015CDBX0057-00-NON-01
2017-07-14 17:15:05.179042	2017-07-14 17:15:05.17905	19536657	4057	56179	64	20170105	C	04	DNA Capacity Enhancement	Houston Forensic Science Center  Inc	079218240	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	X	16.741	Forensic DNA Backlog Reduction Program	\N	\N	2015DNBX0072	0.0000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	1301 FANNIN ST STE 170	Suite 170	\N	\N	HOUSTON	18	USA	201	HARRIS	\N	\N	\N	TX	Texas	77002	7010	\N	\N	20171231	20160101	HOUSTON	TX35000	18	USA	\N	\N	Texas	77002-7010	2	SAI NOT AVAILABLE	0.0000	FY2017-2015DNBX0072-00-NON-01
2017-07-14 17:15:10.443685	2017-07-14 17:15:10.443693	19537276	4057	56179	683	20170308	C	04	Breaking Free will deliver on-site Transitional Housing  targeting victims from Minnesota  nationwide  and internationally who are escaping prostitution  domestic violence  assault  and trafficking.	Breaking Free Incorporated	015890965	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	01	NON	M	16.736	Transitional Housing Assistance for Victims of Domestic Violence, Dating Violence, Stalking, or Sexual Assault	\N	\N	2009WHAX0013	-37685.2300	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	770 UNIVERSITY AVE W	\N	\N	\N	SAINT PAUL	04	USA	123	RAMSEY	\N	\N	\N	MN	Minnesota	55104	4805	\N	\N	20150831	20090901	SAINT PAUL	MN58000	04	USA	\N	\N	Minnesota	55104-4805	2	SAI NOT AVAILABLE	-37685.2300	FY2012-2009WHAX0013-01-NON-02
2017-07-14 17:15:04.931306	2017-07-14 17:15:04.931315	19536628	4057	56179	35	20170112	C	04	Enhancement of Problem-Oriented Policing  Investigations and Active Shooter Response (Equipment Acquisition)	Fond du Lac  City of	140135927	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	X	16.738	Edward Byrne Memorial Justice Assistance Grant Program	\N	\N	2015DJBX0927	-897.6100	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	126 N MAIN ST	\N	\N	\N	FOND DU LAC	06	USA	039	FOND DU LAC	\N	\N	\N	WI	Wisconsin	54935	3424	\N	\N	20160930	20141001	FOND DU LAC	WI26300	06	USA	\N	\N	Wisconsin	54935-3424	2	SAI NOT AVAILABLE	-897.6100	FY2017-2015DJBX0927-00-NON-01
2017-07-14 17:15:07.976387	2017-07-14 17:15:07.976395	19536982	4057	56179	389	20170213	C	04	Youth Accountability Board Project	Covina Police Department	627617152	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	X	16.738	Edward Byrne Memorial Justice Assistance Grant Program	\N	\N	2014DJBX0189	-3704.9600	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	444 N CITRUS AVE	\N	\N	\N	COVINA	32	USA	037	LOS ANGELES	\N	\N	\N	CA	California	91723	2013	\N	\N	20160930	20131001	COVINA	CA16742	32	USA	\N	\N	California	91723-2013	2	SAI NOT AVAILABLE	-3704.9600	FY2014-2014DJBX0189-00-NON-01
2017-07-14 17:15:07.625262	2017-07-14 17:15:07.62527	19536939	4057	56179	346	20170124	C	04	Patrol Car  Criminal Investigations Interview Room  Trauma Kit ProjectPlus Egg Harbor Township Body Armor Initiative and Pleasantville Violent Crime Reduction Initiative	City of Atlantic City	781983069	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	X	16.738	Edward Byrne Memorial Justice Assistance Grant Program	\N	\N	2013DJBX0384	-6651.9000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	2711 ATLANTIC AVE	\N	\N	\N	ATLANTIC CITY	02	USA	001	ATLANTIC	\N	\N	\N	NJ	New Jersey	08401	6401	\N	\N	20160930	20121001	ATLANTIC CITY	NJ02080	02	USA	\N	\N	New Jersey	08401-6401	2	SAI NOT AVAILABLE	-6651.9000	FY2013-2013DJBX0384-00-NON-01
2017-07-14 17:15:07.581025	2017-07-14 17:15:07.581034	19536934	4057	56179	341	20170128	C	05	Enhancing Supervised Visitation for Families Who Have Experienced Domestic Violence	Vera Institute of Justice	073299836	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	M	16.526	OVW Technical Assistance Initiative	\N	\N	2014TAAXK052	0.0000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	233 BROADWAY FL 12	12th Floor	\N	\N	NEW YORK	10	USA	061	NEW YORK	\N	\N	\N	NY	New York	10279	1299	\N	\N	20170930	20150101	NEW YORK	NY51000	10	USA	\N	\N	New York	10279-1299	2	SAI NOT AVAILABLE	0.0000	FY2014-2014TAAXK052-00-NON-01
2017-07-14 17:15:07.350507	2017-07-14 17:15:07.350515	19536907	4057	56179	314	20170131	C	04	FY 2014 DC PREA Reallocation Grant	Office of Victim Services and Justice Grants	135751258	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	A	16.751	Edward Byrne Memorial Competitive Grant Program	\N	\N	2014XTBX0066	-6429.8100	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	1350 PENNSYLVANIA AVE NW	Suite 727N	\N	\N	WASHINGTON	98	USA	001	DISTRICT OF COLUMBIA	\N	\N	\N	DC	District of Columbia	20004	3003	\N	\N	20160930	20141001	WASHINGTON	DC50000	98	USA	\N	\N	District of Columbia	20004-3003	2	SAI NOT AVAILABLE	-6429.8100	FY2014-2014XTBX0066-00-NON-01
2017-07-14 17:15:07.119763	2017-07-14 17:15:07.119771	19536879	4057	56179	286	20170119	C	04	FY2014 DNA Capacity Enhancement and Backlog Reduction Program  (Tulsa Police Department)	City Of Tulsa	078662251	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	C	16.741	Forensic DNA Backlog Reduction Program	\N	\N	2014DNBX0080	-1845.4100	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	175 E 2 ST STE 15129	175 E. 2nd Street	\N	\N	TULSA	01	USA	143	TULSA	\N	\N	\N	OK	Oklahoma	74103	3224	\N	\N	20160930	20141001	TULSA	OK75000	01	USA	\N	\N	Oklahoma	74103-3224	2	SAI NOT AVAILABLE	-1845.4100	FY2014-2014DNBX0080-00-NON-01
2017-07-14 17:15:07.095568	2017-07-14 17:15:07.095576	19536876	4057	56179	283	20170124	C	04	Regional and State Transitional Offender Reentry (RESTORE) Initiative	Palm Beach County Board of County Commissioners	078470481	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	01	NON	X	16.571	Public Safety Officers' Benefits Program	\N	\N	2012CZBX0016	-237.6100	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	301 N OLIVE AVE FRNT	\N	\N	\N	WEST PALM BEACH	21	USA	099	PALM BEACH	\N	\N	\N	FL	Florida	33401	4705	\N	\N	20160930	20121001	WEST PALM BEACH	FL76600	21	USA	\N	\N	Florida	33401-4705	2	SAI NOT AVAILABLE	-237.6100	FY2013-2012CZBX0016-01-NON-02-02
2017-07-14 17:15:06.967673	2017-07-14 17:15:06.967681	19536860	4057	56179	267	20170131	C	04	Expanding the Use of DMC Data:  Analysis of Patterns to Identify Best Practices	Development Services Group Inc.	166113332	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	02	NON	R	16.540	Juvenile Justice and Delinquency Prevention_Allocation to States	\N	\N	2009JFFX0103	-11062.6000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	7315 WISCONSIN AVE STE 800 E	Suite 800 East	\N	\N	BETHESDA	08	USA	031	MONTGOMERY	\N	\N	\N	MD	Maryland	20814	3210	\N	\N	20160930	20091001	BETHESDA	MD07125	08	USA	\N	\N	Maryland	20814-3210	2	SAI NOT AVAILABLE	-11062.6000	FY2014-2009JFFX0103-02-NON-03
2017-07-14 17:15:04.808353	2017-07-14 17:15:04.808361	19536615	4057	56179	22	20170112	C	04	FY 2012 STOP Violence Against Women Formula Grant Program	Louisiana Commission on Law Enforcement	134720015	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	1550	Office of Justice Programs	00	NON	A	16.588	Violence Against Women Formula Grants	\N	\N	2012WFAX0011	-44858.0000	\N	015	JUSTICE, DEPARTMENT OF (1500)	\N	\N	\N	\N	602 N 5TH ST	602 N. 5th St.  1st Floor	\N	\N	BATON ROUGE	06	USA	033	EAST BATON ROUGE	\N	\N	\N	LA	Louisiana	70821	3133	\N	\N	20150630	20120701	BATON ROUGE	LA05000	06	USA	\N	\N	Louisiana	70821-3133	2	SAI NOT AVAILABLE	-44858.0000	FY2012-2012WFAX0011-00-NON-01
\.


--
-- Data for Name: award_procurement; Type: TABLE DATA; Schema: local_broker; Owner: catherine
--

COPY award_procurement (created_at, updated_at, award_procurement_id, submission_id, job_id, row_number, piid, awarding_sub_tier_agency_c, awarding_sub_tier_agency_n, awarding_agency_code, awarding_agency_name, parent_award_id, award_modification_amendme, type_of_contract_pricing, contract_award_type, naics, naics_description, awardee_or_recipient_uniqu, ultimate_parent_legal_enti, ultimate_parent_unique_ide, award_description, place_of_performance_zip4a, place_of_performance_congr, awardee_or_recipient_legal, legal_entity_city_name, legal_entity_state_code, legal_entity_zip4, legal_entity_congressional, legal_entity_address_line1, legal_entity_address_line2, legal_entity_address_line3, legal_entity_country_code, legal_entity_country_name, period_of_performance_star, period_of_performance_curr, period_of_perf_potential_e, ordering_period_end_date, action_date, action_type, federal_action_obligation, current_total_value_award, potential_total_value_awar, funding_sub_tier_agency_co, funding_sub_tier_agency_na, funding_office_code, funding_office_name, awarding_office_code, awarding_office_name, referenced_idv_agency_iden, funding_agency_code, funding_agency_name, place_of_performance_locat, place_of_performance_state, place_of_perform_country_c, idv_type, vendor_doing_as_business_n, vendor_phone_number, vendor_fax_number, multiple_or_single_award_i, type_of_idc, a_76_fair_act_action, dod_claimant_program_code, clinger_cohen_act_planning, commercial_item_acquisitio, commercial_item_test_progr, consolidated_contract, contingency_humanitarian_o, contract_bundling, contract_financing, contracting_officers_deter, cost_accounting_standards, cost_or_pricing_data, country_of_product_or_serv, davis_bacon_act, evaluated_preference, extent_competed, fed_biz_opps, foreign_funding, government_furnished_equip, information_technology_com, interagency_contracting_au, local_area_set_aside, major_program, purchase_card_as_payment_m, multi_year_contract, national_interest_action, number_of_actions, number_of_offers_received, other_statutory_authority, performance_based_service, place_of_manufacture, price_evaluation_adjustmen, product_or_service_code, program_acronym, other_than_full_and_open_c, recovered_materials_sustai, research, sea_transportation, service_contract_act, small_business_competitive, solicitation_identifier, solicitation_procedures, fair_opportunity_limited_s, subcontracting_plan, program_system_or_equipmen, type_set_aside, epa_designated_product, walsh_healey_act, transaction_number, sam_exception, city_local_government, county_local_government, inter_municipal_local_gove, local_government_owned, municipality_local_governm, school_district_local_gove, township_local_government, us_state_government, us_federal_government, federal_agency, federally_funded_research, us_tribal_government, foreign_government, community_developed_corpor, labor_surplus_area_firm, corporate_entity_not_tax_e, corporate_entity_tax_exemp, partnership_or_limited_lia, sole_proprietorship, small_agricultural_coopera, international_organization, us_government_entity, emerging_small_business, c8a_program_participant, sba_certified_8_a_joint_ve, dot_certified_disadvantage, self_certified_small_disad, historically_underutilized, small_disadvantaged_busine, the_ability_one_program, historically_black_college, c1862_land_grant_college, c1890_land_grant_college, c1994_land_grant_college, minority_institution, private_university_or_coll, school_of_forestry, state_controlled_instituti, tribal_college, veterinary_college, educational_institution, alaskan_native_servicing_i, community_development_corp, native_hawaiian_servicing, domestic_shelter, manufacturer_of_goods, hospital_flag, veterinary_hospital, hispanic_servicing_institu, foundation, woman_owned_business, minority_owned_business, women_owned_small_business, economically_disadvantaged, joint_venture_women_owned, joint_venture_economically, veteran_owned_business, service_disabled_veteran_o, contracts, grants, receives_contracts_and_gra, airport_authority, council_of_governments, housing_authorities_public, interstate_entity, planning_commission, port_authority, transit_authority, subchapter_s_corporation, limited_liability_corporat, foreign_owned_and_located, american_indian_owned_busi, alaskan_native_owned_corpo, indian_tribe_federally_rec, native_hawaiian_owned_busi, tribally_owned_business, asian_pacific_american_own, black_american_owned_busin, hispanic_american_owned_bu, native_american_owned_busi, subcontinent_asian_asian_i, other_minority_owned_busin, for_profit_organization, nonprofit_organization, other_not_for_profit_organ, us_local_government, referenced_idv_modificatio, undefinitized_action, domestic_or_foreign_entity) FROM stdin;
2017-07-14 17:21:21.15617	2017-07-14 17:21:21.156178	12416148	4057	56177	26161	DJBP0113SE310001	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	DJBP0700NASBPA073	4	J	A	334516	ANALYTICAL LABORATORY INSTRUMENT MANUFACTURING	947803078	PHAMATECH, INCORPORATED	947803078	URINALYSIS LABORATORY SERVICES FOR FY-2017	921283401	52	PHAMATECH INCORPORATED	SAN DIEGO	CA	921283401	52	15175 INNOVATION DR	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170314	20170331	20170331	\N	20170314	C	-132.0000	-132.0000	-132.0000	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B113	15B113: DEPT OF JUSTICE BUREAU OF PRISONS	15B113	15B113: DEPT OF JUSTICE BUREAU OF PRISONS	1540	015	Department of Justice	\N	CA	USA	A	\N	8886355840	8586355843	\N	\N	0	\N	0	D	0	0	\N	D	\N	S	\N	\N	USA	X	NONE	A	\N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	X	D	\N	6545	\N	\N	C	\N	\N	X	\N	\N	MAFO	FAIR	\N	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	1	\N	\N	\N	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	9	X	A
2017-07-14 17:20:50.457822	2017-07-14 17:20:50.45783	12413277	4057	56177	23290	DJD14HQ06S17002	1524	DRUG ENFORCEMENT ADMINISTRATION	015	Department of Justice	DJD14HQS0006	2	J	A	325120	INDUSTRIAL GAS MANUFACTURING	002812691	ARC3 GASES INC.	079626705	GASES FOR THE LABORATORY	207745313	04	ARCET EQUIPMENT COMPANY	RICHMOND	VA	232224808	04	1700 CHAMBERLAYNE AVE	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170323	20171203	20171203	\N	20170323	C	500.0000	500.0000	500.0000	1524	1524: DRUG ENFORCEMENT ADMINISTRATION	15DDL3	15DDL3: MID-ATLANTIC LABORATORY	15DDL3	15DDL3: MID-ATLANTIC LABORATORY	1524	015	Department of Justice	\N	MD	USA	A	\N	8046444521	8047888904	\N	\N	0	\N	0	A	\N	0	\N	D	Z	S	\N	N	USA	X	NONE	F	\N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	X	D	\N	6830	\N	\N	C	\N	\N	X	\N	\N	SP1	\N	\N	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:20:45.244199	2017-07-14 17:20:45.244207	12412797	4057	56177	22810	DJJ15F2607	1501	OFFICES, BOARDS AND DIVISIONS	015	Department of Justice	GS07F0036Y	7	Y	C	611699	ALL OTHER MISCELLANEOUS SCHOOLS AND INSTRUCTION	603837258	ARMADA, LTD	603837258	IGF::CT::IGF SECURITY PROGRAM SUPPORT FOR EOUSA YRG$S0178329	205300001	00	ARMADA, LTD	POWELL	OH	430658064	12	23 CLAIREDAN DRIVE	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170324	20170731	20200731	\N	20170324	B	48377.2800	48377.2800	473503.6800	1501	1501: OFFICES, BOARDS AND DIVISIONS	15JA96	15JA96: U.S. ATTORNEYS OFFICE - (MISC)	15JPSS	15JPSS: JMD-PROCUREMENT SERVICES STAFF	4732	015	Department of Justice	\N	DC	USA	C	\N	6144319700	6144319706	\N	\N	0	\N	0	B	0	0	\N	D	\N	S	\N	\N	USA	N	NONE	A	Y	X	0	\N	X	0	\N	0	\N	NONE	1	2	\N	N	C	\N	R408	\N	\N	C	\N	\N	N	\N	\N	MAFO	CSA	\N	\N	SBA	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	1	1	1	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:20:37.281697	2017-07-14 17:20:37.281705	12412055	4057	56177	22068	DJBP0114SB130186	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	VA797P12D0001	0	J	C	325412	PHARMACEUTICAL PREPARATION MANUFACTURING	177667227	MCKESSON CORPORATION	177667227	151060-REQUEST FOR VARIOUS MEDICATIONS.	941045252	12	MCKESSON CORPORATION	SAN FRANCISCO	CA	941045252	12	ONE POST ST	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170215	20170215	20170215	\N	20170215	\N	6098.0800	6098.0800	6098.0800	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B114	15B114: DEPT OF JUSTICE BUREAU OF PRISONS	15B114	15B114: DEPT OF JUSTICE BUREAU OF PRISONS	3600	015	Department of Justice	\N	CA	USA	C	\N	9724464947	9724465795	\N	\N	0	\N	0	A	0	\N	\N	H	\N	O	\N	N	USA	N	NONE	A	Y	X	0	\N	X	0	\N	0	0	NONE	1	5	\N	X	D	\N	6505	\N	\N	C	\N	\N	N	\N	\N	NP	\N	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:20:32.141435	2017-07-14 17:20:32.141443	12411576	4057	56177	21589	DJBP0302SA110200	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	\N	0	J	B	311999	ALL OTHER MISCELLANEOUS FOOD MANUFACTURING	118243315	C.J. FOODS, INC.	118243315	NATIONAL MENU 3RD QTR FY 2017	563104534	06	C.J. FOODS, INC.	AVON	MN	563104534	06	18685 368TH ST	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170401	20170630	20170630	\N	20170316	\N	12312.0000	12312.0000	12312.0000	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B302	15B302: DEPT OF JUSTICE BUREAU OF PRISONS	15B302	15B302: DEPT OF JUSTICE BUREAU OF PRISONS	\N	015	Department of Justice	\N	MN	USA	B	\N	3208454080	3208454090	\N	\N	0	\N	0	B	0	\N	\N	H	Z	S	\N	\N	USA	X	NONE	F	Y	X	0	\N	X	0	\N	0	\N	NONE	1	37	\N	X	C	\N	8945	\N	\N	C	\N	\N	X	\N	RFQP03021700009	SP1	\N	B	\N	SBA	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	1	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:20:30.238026	2017-07-14 17:20:30.238034	12411396	4057	56177	21409	DJF161200S0002482	1549	FEDERAL BUREAU OF INVESTIGATION	015	Department of Justice	\N	8	J	B	221210	NATURAL GAS DISTRIBUTION	007940976	NISOURCE INC.	185654076	COLUMBIA TRANSPORTATION	238362400	04	COLUMBIA GAS OF VIRGINIA, INC.	CHESTER	VA	238362400	04	1809 COYOTE DR	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170317	20170428	20170428	\N	20170317	M	0.0000	0.0000	0.0000	1549	1549: FEDERAL BUREAU OF INVESTIGATION	15F059	15F059: DIVISION 0300	15F059	15F059: DIVISION 0300	\N	015	Department of Justice	\N	VA	USA	B	\N	6144604824	6144606851	\N	\N	0	\N	0	D	0	0	\N	D	\N	O	\N	\N	USA	X	NONE	C	N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	X	D	\N	9130	\N	ONE	C	\N	\N	X	\N	\N	SSS	\N	B	\N	NONE	A	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:20:25.43852	2017-07-14 17:20:25.438528	12410950	4057	56177	20963	DJBP0611SA130228	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	\N	0	J	B	311615	POULTRY PROCESSING	078359546	NORTH STAR IMPORTS, LLC	078359546	QUARTERLY SUBSISTENCE AND MEAT	553371265	02	NORTH STAR IMPORTS, LLC	BURNSVILLE	MN	553371265	02	2206 EAST 117TH STREET	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170403	20170428	20170428	\N	20170320	\N	3550.0000	3550.0000	3550.0000	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B611	15B611: DEPT OF JUSTICE BUREAU OF PRISONS	15B611	15B611: DEPT OF JUSTICE BUREAU OF PRISONS	\N	015	Department of Justice	\N	MN	USA	B	\N	9524568607	9524568492	\N	\N	0	\N	0	A	0	\N	\N	H	\N	S	\N	\N	USA	X	NONE	F	Y	X	0	\N	X	0	\N	0	\N	NONE	1	13	\N	X	C	\N	8905	\N	\N	C	\N	\N	X	\N	RFQP06111700014	SP1	\N	B	\N	SBA	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:20:06.831336	2017-07-14 17:20:06.831345	12409220	4057	56177	19233	DJF171200P0002754	1549	FEDERAL BUREAU OF INVESTIGATION	015	Department of Justice	\N	1	J	B	238210	ELECTRICAL CONTRACTORS AND OTHER WIRING INSTALLATION CONTRACTORS	007821085	FREEMAN EXPOSITIONS, INC.	007821085	IGF::OT::IGF BOOTH SETUP, OPERATION, DISPLAYS, DISASSEMBLY AND PACKING FOR STORAGE.	941023423	12	FREEMAN EXPOSITIONS, INC.	DALLAS	TX	752352312	33	1600 VICEROY STE 100	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170315	20170315	20170315	\N	20170315	C	820.1000	820.1000	820.1000	1549	1549: FEDERAL BUREAU OF INVESTIGATION	15F009	15F009: SAN FRANCISCO FIELD OFFICE	15F067	15F067: DIVISION 1200	\N	015	Department of Justice	\N	CA	USA	B	\N	8885085054	8885085054	\N	\N	0	\N	0	B	0	0	\N	D	\N	O	\N	\N	USA	N	NONE	C	N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	N	C	\N	R499	\N	ONE	C	\N	\N	N	\N	\N	SSS	\N	B	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:19:58.60069	2017-07-14 17:19:58.600698	12408475	4057	56177	18488	DJD15HQ14S17C008	1524	DRUG ENFORCEMENT ADMINISTRATION	015	Department of Justice	DJD15HQS0014	3	J	A	333315	PHOTOGRAPHIC AND PHOTOCOPYING EQUIPMENT MANUFACTURING	116194192	CANON INC.	690549662	IGF::CL::IGF PERIOD OF PERFORMANCE 10/1/2016 - 9/30/2017	222031551	08	CANON U.S.A., INC.	ARLINGTON	VA	222031678	08	4100 N. FAIRFAX DRIVE, SUITE 200	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170123	20170930	20170930	\N	20170131	C	-7440.0000	0.0000	0.0000	1524	1524: DRUG ENFORCEMENT ADMINISTRATION	15DD0M	15DD0M: OPERATIONS MANAGEMENT	15DDHQ	15DDHQ: HEADQUATERS	1524	015	Department of Justice	\N	VA	USA	A	\N	8003239170	7038073819	\N	\N	0	\N	0	A	0	0	\N	D	\N	O	\N	\N	USA	X	NONE	A	\N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	Y	C	\N	J074	\N	\N	C	ST3	\N	X	\N	\N	MAFO	ONE	\N	\N	NONE	A	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	4	X	A
2017-07-14 17:19:48.847204	2017-07-14 17:19:48.847212	12407570	4057	56177	17583	DJM16D58P0006	1544	U.S. MARSHALS SERVICE	015	Department of Justice	\N	5	J	B	561421	TELEPHONE ANSWERING SERVICES	796646453	AT&T INC.	108024050	IGF::CT::IGF	282021633	12	BELLSOUTH TELECOMMUNICATIONS, INC.	ATLANTA	GA	303196004	06	2180 LAKE BLVD	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170307	20161015	20161015	\N	20170307	K	-22.2000	-22.2000	-22.2000	1544	1544: U.S. MARSHALS SERVICE	15M058	15M058: U.S. DEPT OF JUSTICE, USM, W/NC	15M058	15M058: U.S. DEPT OF JUSTICE, USM, W/NC	\N	015	Department of Justice	\N	NC	USA	B	\N	5713544106	\N	\N	\N	0	\N	0	B	0	0	\N	D	\N	O	\N	\N	USA	N	NONE	F	N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	N	C	\N	D304	\N	\N	C	\N	\N	Y	\N	\N	SP1	\N	B	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:19:50.494589	2017-07-14 17:19:50.494597	12407721	4057	56177	17734	DJD1301017D106	1524	DRUG ENFORCEMENT ADMINISTRATION	015	Department of Justice	DJD13C0010	2	Z	C	541930	TRANSLATION AND INTERPRETATION SERVICES	038049532	M V M, INC.	038049532	IGF::CL::IGF - TRANSLATION SERVICES	770279506	07	M V M, INC.	ASHBURN	VA	201476063	10	44620 GUILFORD DRIVE, STE 150	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170306	20170130	20170130	\N	20170306	K	-4168.8100	-4168.8100	-4168.8100	1524	1524: DRUG ENFORCEMENT ADMINISTRATION	15DD0S	15DD0S: SPECIAL OPERATIONS DIVISION	15DD0S	15DD0S: SPECIAL OPERATIONS DIVISION	1524	015	Department of Justice	\N	TX	USA	C	\N	5712234630	5712234487	\N	\N	0	\N	0	D	0	0	\N	D	\N	O	\N	Y	USA	N	NONE	A	Y	X	0	\N	X	0	\N	0	1	NONE	1	2	\N	N	C	\N	R608	\N	\N	C	\N	\N	Y	\N	\N	NP	\N	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	1	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:19:42.671824	2017-07-14 17:19:42.671832	12406998	4057	56177	17011	DJM17D24P0009	1544	U.S. MARSHALS SERVICE	015	Department of Justice	\N	1	J	B	531311	RESIDENTIAL PROPERTY MANAGERS	033798061	AHR ENTERPRISES, INC.	033798061	IGF::OT::IGF 24-17-087 PROPERTY MGT 14CR705-6 BOND SEIZURE 2241 N MONITOR	606041834	07	AHR ENTERPRISES, INC.	LANSING	IL	604384219	02	3753 193RD PLACE	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170307	20170930	20170930	\N	20170307	M	0.0000	0.0000	0.0000	1544	1544: U.S. MARSHALS SERVICE	15M024	15M024: U.S. DEPT OF JUSTICE, USM, N/IL	15M024	15M024: U.S. DEPT OF JUSTICE, USM, N/IL	\N	015	Department of Justice	\N	IL	USA	B	\N	7085421331	\N	\N	\N	0	\N	0	B	0	\N	\N	H	\N	S	\N	\N	USA	X	NONE	G	X	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	N	C	\N	R610	\N	SP2	C	\N	\N	X	\N	\N	SP1	\N	B	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	1	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:19:33.928645	2017-07-14 17:19:33.928653	12406189	4057	56177	16202	DJM16A44D0007	1544	U.S. MARSHALS SERVICE	015	Department of Justice	DJJ11C2131	5	Z	C	541199	ALL OTHER LEGAL SERVICES	134510648	ENGILITY CORPORATION	783837672	AFD: FSA NATIONAL CAPITAL REGION FY16 TASK ORDER IGF::OT::IGF	222024511	08	FORFEITURE SUPPORT ASSOCIATES, LLC	ASHBURN	VA	201475060	10	20110 ASHBROOK PL STE 220	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170303	20160930	20160930	\N	20170303	C	-147592.9300	-147592.9300	-147592.9300	1544	1544: U.S. MARSHALS SERVICE	15M500	15M500: U.S. DEPT OF JUSTICE, USMS	15M103	15M103: U.S. DEPT OF JUSTICE, USMS	1501	015	Department of Justice	\N	VA	USA	C	\N	5712918900	5712918957	\N	\N	0	\N	0	D	0	0	\N	D	\N	O	\N	N	USA	N	NONE	A	Y	X	0	\N	X	0	\N	0	0	NONE	1	6	\N	N	C	\N	R418	\N	\N	C	\N	\N	Y	\N	\N	NP	\N	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	1	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:19:33.827912	2017-07-14 17:19:33.82792	12406179	4057	56177	16192	DJBP0411SB230279	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	DJBP0411BPA17011	0	J	A	622110	GENERAL MEDICAL AND SURGICAL HOSPITALS	079148128	PEKIN MEMORIAL HOSPITAL	079148128	IGF::OT::IGF HOSPITAL/MEDICAL SERVICES, INPATIENT/OUTPATIENT PROVIDED TO FEDERAL PRISON INMATES AT THE FEDERAL CORRECTIONAL INSTITUTION/FEDERAL PRISON CAMP FY-17.	615544936	17	PEKIN MEMORIAL HOSPITAL	PEKIN	IL	615544936	17	600 S 13TH ST	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170301	20170302	20170302	\N	20170302	\N	25000.0000	25000.0000	25000.0000	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B411	15B411: DEPT OF JUSTICE BUREAU OF PRISONS	15B411	15B411: DEPT OF JUSTICE BUREAU OF PRISONS	1540	015	Department of Justice	\N	IL	USA	A	\N	3093530514	3093530491	\N	\N	0	\N	0	A	\N	\N	\N	H	\N	O	\N	\N	USA	N	NONE	G	\N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	N	C	\N	Q526	\N	SP2	C	\N	\N	N	\N	\N	SP1	\N	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	1	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	X	A
2017-07-14 17:19:26.523648	2017-07-14 17:19:26.523656	12405506	4057	56177	15519	DJBP0409SP130117	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	\N	0	J	B	562111	SOLID WASTE COLLECTION	156235827	PERRY RIDGE LANDFILL INC	156235827	IGF::OT::IGF TRASH REMOVAL MARCH	628324332	12	PERRY RIDGE LANDFILL INC	DU QUOIN	IL	628324332	12	6305 SACRED HEART RD	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170301	20170331	20170331	\N	20170228	\N	3600.0000	3600.0000	3600.0000	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B409	15B409: DEPT OF JUSTICE BUREAU OF PRISONS	15B409	15B409: DEPT OF JUSTICE BUREAU OF PRISONS	\N	015	Department of Justice	\N	IL	USA	B	\N	6183181490	6302467540	\N	\N	0	\N	0	A	0	\N	\N	H	\N	S	\N	\N	USA	X	NONE	F	N	X	0	\N	X	1	\N	0	\N	NONE	1	1	\N	N	C	\N	S205	\N	\N	C	\N	\N	X	\N	\N	SP1	\N	B	\N	SBA	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:19:25.383232	2017-07-14 17:19:25.38324	12405399	4057	56177	15412	DJM15A44D0323	1544	U.S. MARSHALS SERVICE	015	Department of Justice	DJM13A44D0121	1	J	C	493190	OTHER WAREHOUSING AND STORAGE	806457391	ALLIANCE WORLDWIDE DISTRIBUTING LLC	806457391	MIQ M-15-A44-R-000012 DEOB REMAINING FUNDS FOR OY2. IGF::OT::IGF	282022721	12	ALLIANCE WORLDWIDE DISTRIBUTING LLC	KNOXVILLE	TN	379205845	02	601 WEST FORD VALLEY ROAD	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170301	20160831	20160831	\N	20170301	K	-64518.2400	-64518.2400	-64518.2400	1544	1544: U.S. MARSHALS SERVICE	15M500	15M500: U.S. DEPT OF JUSTICE, USMS	15M103	15M103: U.S. DEPT OF JUSTICE, USMS	1544	015	Department of Justice	\N	NC	USA	C	\N	8652081013	8652555294	\N	\N	0	\N	0	A	1	0	\N	D	\N	S	\N	N	USA	N	NONE	F	Y	X	0	\N	X	0	\N	0	0	NONE	1	5	\N	N	C	\N	S215	\N	\N	C	\N	\N	Y	\N	\N	SP1	\N	\N	\N	SBA	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	1	1	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	1	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:19:14.687992	2017-07-14 17:19:14.688	12404403	4057	56177	14416	DJBP0309RB230048	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	DJBP0309BPA16003	9	J	A	622110	GENERAL MEDICAL AND SURGICAL HOSPITALS	004677399	NAPHCARE, INC.	004677399	IGF::OT::IGF COMPREHENSIVE MEDICAL SERVICES	352162158	06	NAPHCARE, INC.	BIRMINGHAM	AL	352162158	06	2090 COLUMBIANA RD, SUITE 4000	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170227	20170227	20170227	\N	20170227	C	30700.4200	30700.4200	30700.4200	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B309	15B309: DEPT OF JUSTICE BUREAU OF PRISONS	15B309	15B309: DEPT OF JUSTICE BUREAU OF PRISONS	1540	015	Department of Justice	\N	AL	USA	A	\N	2055368400	2052448093	\N	\N	0	\N	0	A	\N	0	\N	D	\N	O	\N	\N	USA	N	NONE	F	\N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	N	C	\N	Q999	\N	\N	C	\N	\N	N	\N	\N	SP1	\N	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:19:10.137211	2017-07-14 17:19:10.137219	12403984	4057	56177	13997	DJBP0201RB250031	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	DJB020100000027	7	J	C	622110	GENERAL MEDICAL AND SURGICAL HOSPITALS	086710329	SEVEN CORNERS, INC	086710329	IGF::OT::IGF ESTIMATED OUTSIDE MEDICAL SERVICES FOR FCC ALLENWOOD UNDER DJBP0201000027.	178109159	10	SEVEN CORNERS, INC	CARMEL	IN	460325631	05	303 CONGRESSIONAL BLVD	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170224	20160831	20160831	\N	20170224	C	-4486.5600	-4486.5600	-4486.5600	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B201	15B201: DEPT OF JUSTICE BUREAU OF PRISONS	15B201	15B201: DEPT OF JUSTICE BUREAU OF PRISONS	1540	015	Department of Justice	\N	PA	USA	C	\N	3178182805	3175752795	\N	\N	0	\N	0	A	0	0	\N	D	\N	S	\N	N	USA	N	NONE	A	Y	X	0	\N	X	0	\N	0	0	NONE	1	8	\N	N	C	\N	Q201	\N	\N	C	\N	\N	N	\N	\N	NP	\N	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:19:05.618889	2017-07-14 17:19:05.618897	12403567	4057	56177	13580	DJBP0407SB130145	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	VA797P12D0001	0	J	C	325412	PHARMACEUTICAL PREPARATION MANUFACTURING	177667227	MCKESSON CORPORATION	177667227	PRESCRIPTION DRUGS	941045252	12	MCKESSON CORPORATION	SAN FRANCISCO	CA	941045252	12	ONE POST ST	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170223	20170228	20170228	\N	20170223	\N	26281.9800	26281.9800	26281.9800	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B407	15B407: DEPT OF JUSTICE BUREAU OF PRISONS	15B407	15B407: DEPT OF JUSTICE BUREAU OF PRISONS	3600	015	Department of Justice	\N	CA	USA	C	\N	9724464947	9724465795	\N	\N	0	\N	0	A	0	\N	\N	H	\N	O	\N	N	USA	N	NONE	A	Y	X	0	\N	X	0	\N	0	0	NONE	1	5	\N	X	D	\N	6505	\N	\N	C	\N	\N	N	\N	\N	NP	\N	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:19:01.908004	2017-07-14 17:19:01.908012	12403222	4057	56177	13235	DJD17TRP0036	1524	DRUG ENFORCEMENT ADMINISTRATION	015	Department of Justice	\N	0	J	B	511130	BOOK PUBLISHERS	832161538	PUBLISHERS GROUP WEST, LLC	832161538	STREET DRUG ID GUIDE NEEDED TO SUPPORT ACADEMY CLASSES.	553569569	03	PUBLISHERS GROUP WEST, LLC	LONG LAKE	MN	553569569	03	2255 N WILLOW DR	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170223	20170331	20170331	\N	20170223	\N	4063.4000	4063.4000	4063.4000	1524	1524: DRUG ENFORCEMENT ADMINISTRATION	15DDTR	15DDTR: OFFICE OF TRAINING	15DDTR	15DDTR: OFFICE OF TRAINING	\N	015	Department of Justice	\N	MN	USA	B	\N	7634730646	7634040725	\N	\N	0	\N	0	A	0	\N	\N	H	\N	S	\N	\N	USA	X	NONE	F	X	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	X	D	\N	7610	\N	\N	C	\N	\N	X	\N	\N	SP1	\N	B	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:18:55.422444	2017-07-14 17:18:55.422453	12402674	4057	56177	12687	DJBP0118SB140099	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	V797P7363A	0	J	C	561320	TEMPORARY HELP SERVICES	926684788	AMN HEALTHCARE SERVICES, INC.	152725966	IGF::OT::IGF - TEMPORARY PHYSICIAN SERVICES -	412242067	05	STAFF CARE, INC	COPPELL	TX	750194630	24	8840 CYPRESS WATERS BLVD STE 300	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170301	20170331	20170331	\N	20170222	\N	25200.0000	25200.0000	25200.0000	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B118	15B118: DEPT OF JUSTICE BUREAU OF PRISONS	15B118	15B118: DEPT OF JUSTICE BUREAU OF PRISONS	3600	015	Department of Justice	\N	KY	USA	C	\N	8006852272	9729830717	\N	\N	0	\N	0	A	0	\N	\N	H	\N	O	\N	\N	USA	N	NONE	A	Y	X	0	\N	X	0	\N	0	\N	NONE	1	3	\N	N	C	\N	Q201	\N	\N	C	\N	\N	Y	\N	\N	MAFO	FAIR	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	P00009	X	A
2017-07-14 17:18:48.548449	2017-07-14 17:18:48.548457	12402079	4057	56177	12092	DJM16D46D0046	1544	U.S. MARSHALS SERVICE	015	Department of Justice	DJM13A32V0040	24	Y	C	561612	SECURITY GUARDS AND PATROL SERVICES	076192475	INTER-CON SECURITY SYSTEMS, INC.	076192475	CSO SERVICES FOR THE DISTRICT OF MONTANA (46). IGF::OT::IGF	594044228	00	INTER-CON SECURITY SYSTEMS, INC.	PASADENA	CA	911052079	27	210 S DE LACEY AVE # 200	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170112	20160930	20160930	\N	20170112	K	-59243.7600	-59243.7600	-59243.7600	1027	1027: ADMINISTRATIVE OFFICE OF THE U.S. COURTS	103299	103299: AOUSC	15M200	15M200: US DOJ, USMS OFC SECURITY CONTRACTS	1544	010	The Judicial Branch	\N	MT	USA	C	\N	6265352210	6266859111	\N	\N	0	\N	0	D	0	0	\N	D	Z	O	\N	N	USA	X	NONE	A	Y	X	1	\N	X	0	\N	0	0	NONE	1	9	\N	N	C	\N	S206	\N	\N	C	\N	\N	Y	\N	\N	NP	\N	\N	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	1	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	1	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:18:34.089154	2017-07-14 17:18:34.089163	12400761	4057	56177	10774	DJM17A32DH0112	1544	U.S. MARSHALS SERVICE	015	Department of Justice	DJM15A32V0033	0	J	C	561621	SECURITY SYSTEMS SERVICES (EXCEPT LOCKSMITHS)	004469300	DIEBOLD, INCORPORATED	004469300	IGF::OT::IGF HIDS INSTALL - PEAD, DUSTIN	447203306	07	DIEBOLD, INCORPORATED	NORTH CANTON	OH	447201597	16	5995 MAYFAIR RD	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170215	20170815	20170815	\N	20170215	\N	314.2500	314.2500	314.2500	1027	1027: ADMINISTRATIVE OFFICE OF THE U.S. COURTS	103299	103299: AOUSC	15M200	15M200: US DOJ, USMS OFC SECURITY CONTRACTS	1544	010	The Judicial Branch	\N	OH	USA	C	\N	8019561277	8019561299	\N	\N	0	\N	0	A	0	\N	\N	H	\N	O	\N	N	USA	X	NONE	A	Y	X	0	\N	X	0	\N	0	0	NONE	1	2	\N	N	C	\N	N063	\N	\N	C	\N	\N	X	\N	\N	NP	\N	\N	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:18:30.060357	2017-07-14 17:18:30.060365	12400396	4057	56177	10409	DJF161200G0008122	1549	FEDERAL BUREAU OF INVESTIGATION	015	Department of Justice	GS00F0049M	1	J	C	518210	DATA PROCESSING, HOSTING, AND RELATED SERVICES	175344753	DELL SERVICES FEDERAL GOVERNMENT  INC.	601839660	IGF::OT::IGF- THE GOVERNMENT REQUIRES RED HAT SUBSCRIPTIONS WITH UP TO 24X7 PREMIUM SUPPORT SERVICES TO FACILITATE CUSTOMER MAINTENANCE CYCLES, BUG FIXES AND UPGRADES FOR RED HAT ERRATA THAT ARE BATCHED TOGETHER INTO PERIODIC UPDATES.	263060001	01	DELL SERVICES FEDERAL GOVERNMENT, INC.	HERNDON	VA	201714686	11	13880 DULLES CORNER LN STE 200	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170214	20170214	20170831	\N	20170214	M	0.0000	0.0000	0.0000	1549	1549: FEDERAL BUREAU OF INVESTIGATION	15F057	15F057: DIVISION 0100	15F067	15F067: DIVISION 1200	4730	015	Department of Justice	\N	WV	USA	C	\N	7032898128	7032898252	\N	\N	0	\N	0	D	0	0	\N	D	Z	O	\N	\N	USA	X	\N	A	X	X	0	\N	X	0	\N	0	0	NONE	1	9	\N	N	C	\N	J070	\N	\N	C	\N	\N	X	\N	\N	MAFO	FAIR	\N	\N	\N	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	PA0025ADDRESSCHG	X	A
2017-07-14 17:18:18.048809	2017-07-14 17:18:18.048817	12399296	4057	56177	9309	DJD1300517D107	1524	DRUG ENFORCEMENT ADMINISTRATION	015	Department of Justice	DJD13C0005	0	Z	C	541930	TRANSLATION AND INTERPRETATION SERVICES	038049532	M V M, INC.	038049532	IGF::CL::IGF LINGUIST SERVICES	606041745	07	M V M, INC.	ASHBURN	VA	201476063	10	44620 GUILFORD DRIVE, STE 150	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170201	20170312	20170312	\N	20170201	\N	27517.8400	27517.8400	27517.8400	1524	1524: DRUG ENFORCEMENT ADMINISTRATION	15DD0S	15DD0S: SPECIAL OPERATIONS DIVISION	15DD0S	15DD0S: SPECIAL OPERATIONS DIVISION	1524	015	Department of Justice	\N	IL	USA	C	\N	5712234630	5712234487	\N	\N	0	\N	0	D	0	0	\N	D	\N	O	\N	Y	USA	N	NONE	A	Y	X	0	\N	X	0	\N	0	1	NONE	1	3	\N	N	C	\N	R608	\N	\N	C	\N	\N	Y	\N	\N	NP	\N	\N	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	1	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:18:01.916208	2017-07-14 17:18:01.916216	12397821	4057	56177	7834	DJBP0313SE120001	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	DJBP0700NAS137	0	J	C	315990	APPAREL ACCESSORIES AND OTHER APPAREL MANUFACTURING	156992745	INTEGRIO TECHNOLOGIES, LLC	156992745	INTEGRIO TECHNOLOGIES-HEATHER HEWITT, 703-429-3209, BOPPPEGLOVES@INTEGRIO.COM RP# 0087-17 PERSONAL PROTECTIVE EQUIPMENT-LAW ENFORCEMENT GLOVES, ALPHA-PLUS GLOVE (TUS-12) VENDOR IS REQUIRED TO SELF CERTIFY BUSINESS SIZE(SMALL/LARGE) ON ALL INVOICES.	201711334	10	INTEGRIO TECHNOLOGIES, LLC	HERNDON	VA	201716154	11	2355 DULLES CORNER BLVD STE 600	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170203	20170317	20170417	\N	20170203	\N	13958.7500	13958.7500	13958.7500	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B313	15B313: DEPT OF JUSTICE BUREAU OF PRISONS	15B313	15B313: DEPT OF JUSTICE BUREAU OF PRISONS	1540	015	Department of Justice	\N	VA	USA	C	\N	7034293250	7039611127	\N	\N	0	\N	0	A	0	\N	\N	H	\N	S	\N	N	USA	X	NONE	D	Y	X	0	\N	X	0	\N	0	1	NONE	1	11	\N	X	D	\N	8470	\N	\N	C	\N	\N	X	\N	\N	NP	\N	\N	\N	SBA	A	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:17:49.117033	2017-07-14 17:17:49.117042	12396652	4057	56177	6665	DJD1301117D091	1524	DRUG ENFORCEMENT ADMINISTRATION	015	Department of Justice	DJD13C0011	1	Z	C	541930	TRANSLATION AND INTERPRETATION SERVICES	186945325	METROPOLITAN INTERPRETERS & TRANSLATORS, INC.	186945325	IGF::CL::IGF - TRANSLATION SERVICES	100114713	10	METROPOLITAN INTERPRETERS & TRANSLATORS, INC.	NEW YORK	NY	100178538	12	110 E 42ND ST, STE 802	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170123	20161228	20161228	\N	20170123	K	-3122.9000	-3122.9000	-3122.9000	1524	1524: DRUG ENFORCEMENT ADMINISTRATION	15DD0S	15DD0S: SPECIAL OPERATIONS DIVISION	15DD0S	15DD0S: SPECIAL OPERATIONS DIVISION	1524	015	Department of Justice	\N	NY	USA	C	\N	2136734710	2136734711	\N	\N	0	\N	0	D	0	0	\N	D	\N	O	\N	Y	USA	X	NONE	A	Y	X	0	\N	X	0	\N	0	0	NONE	1	4	\N	N	C	\N	R608	\N	\N	C	\N	\N	X	\N	\N	NP	\N	\N	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	1	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	X	A
2017-07-14 17:17:44.806358	2017-07-14 17:17:44.806366	12396257	4057	56177	6270	DJBP061000000051	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	\N	0	J	B	238290	OTHER BUILDING EQUIPMENT CONTRACTORS	005262308	KONE OYJ	459906942	IGF::OT::IGF ELEVATOR MAINTENANCE SERVICES-MCC SAN DIEGO, CA BASE AND FOUR OPTION YEARS	\N	\N	KONE INC.	MOLINE	IL	612651374	17	ONE KONE COURT	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170201	\N	\N	20220131	20170130	\N	0.0000	\N	529669.6800	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B610	15B610: DEPT OF JUSTICE BUREAU OF PRISONS	15BFA0	15BFA0: DOJ BOP FIELD ACQUISITION OFFICE	\N	015	Department of Justice	\N	\N	\N	B	\N	3097435126	3097435800	S	A	0	\N	0	A	0	0	\N	D	\N	O	X	N	\N	N	NONE	F	Y	X	0	\N	X	0	\N	\N	0	NONE	\N	2	\N	N	\N	\N	J099	\N	\N	C	\N	\N	Y	\N	RFQP06101600009	SP1	\N	B	\N	NONE	\N	N	\N	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	C
2017-07-14 17:17:36.953756	2017-07-14 17:17:36.953765	12395616	4057	56177	5629	DJD17NYP0033	1524	DRUG ENFORCEMENT ADMINISTRATION	015	Department of Justice	\N	0	J	B	334118	COMPUTER TERMINAL AND OTHER COMPUTER PERIPHERAL EQUIPMENT MANUFACTURING	120142380	J & R TELECOMMUNICATIONS, INC.	120142380	IGF::OT::IGF	100114713	10	J & R TELECOMMUNICATIONS, INC.	PERRINEVILLE	NJ	085351135	04	10 MERKIN DRIVE	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170124	20170301	20170301	\N	20170124	\N	8200.0000	8200.0000	8200.0000	1524	1524: DRUG ENFORCEMENT ADMINISTRATION	15DDNY	15DDNY: NEW YORK NY DIVISION	15DDNY	15DDNY: NEW YORK NY DIVISION	\N	015	Department of Justice	\N	NY	USA	B	\N	7326619636	7326611940	\N	\N	0	\N	0	D	0	0	\N	D	\N	S	\N	\N	USA	N	NONE	C	N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	N	C	\N	J058	\N	ONE	C	\N	\N	N	\N	\N	SSS	\N	B	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	1	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:17:30.936344	2017-07-14 17:17:30.936352	12395091	4057	56177	5104	DJBP0402RB210001L	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	DJBP0402BPA15003	6	J	A	622110	GENERAL MEDICAL AND SURGICAL HOSPITALS	015682615	ESSENTIA HEALTH	619042760	IGF::OT::IGF MONTHLY MEDICAL SERVICES (ESTIMATE) BPA # DJBP0402BPA15003	558051950	08	ST. MARY'S DULUTH CLINIC HEALTH SYSTEM	DULUTH	MN	558051950	08	400 E THIRD ST	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170125	20170125	20170125	\N	20170125	C	-5797.2200	-5797.2200	-5797.2200	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B402	15B402: DEPT OF JUSTICE BUREAU OF PRISONS	15B402	15B402: DEPT OF JUSTICE BUREAU OF PRISONS	1540	015	Department of Justice	\N	MN	USA	A	\N	2187866443	2187868621	\N	\N	0	\N	0	A	\N	0	\N	D	\N	O	\N	\N	USA	X	NONE	F	\N	X	0	\N	X	0	\N	0	\N	NONE	1	2	\N	N	C	\N	Q201	\N	\N	C	\N	\N	X	\N	\N	SP1	\N	\N	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	X	A
2017-07-14 17:17:29.956188	2017-07-14 17:17:29.956196	12395003	4057	56177	5016	DJBP0409SB230221	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	\N	2	J	B	622110	GENERAL MEDICAL AND SURGICAL HOSPITALS	040139735	COMMUNITY HEALTH SYSTEMS, INC.	137572269	IGF::OT::IGF DECEMBER SERVICES	629595884	12	MARION HOSPITAL CORPORATION	MARION	IL	629595884	12	3333 W DEYOUNG ST	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170123	20170123	20170123	\N	20170123	C	-3430.5000	-3430.5000	-3430.5000	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B409	15B409: DEPT OF JUSTICE BUREAU OF PRISONS	15B409	15B409: DEPT OF JUSTICE BUREAU OF PRISONS	\N	015	Department of Justice	\N	IL	USA	B	\N	6189987020	6189987449	\N	\N	0	\N	0	A	0	0	\N	D	\N	O	\N	\N	USA	X	NONE	A	N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	N	C	\N	Q201	\N	\N	C	\N	\N	X	\N	\N	NP	\N	B	\N	NONE	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	1	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:17:10.304607	2017-07-14 17:17:10.304615	12393215	4057	56177	3228	DJF174210P0002464	1549	FEDERAL BUREAU OF INVESTIGATION	015	Department of Justice	\N	0	J	B	424120	STATIONERY AND OFFICE SUPPLIES MERCHANT WHOLESALERS	616385667	ASE DIRECT, INC.	616385667	VARIOUS COLORS OF TONER AND PHOTO CONDUCTORS FOR LEXMARK MFP	205350002	00	ASE DIRECT, INC.	BRENTWOOD	TN	370273240	07	7113 PEACH CT STE 200	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170110	20170710	20170710	\N	20170110	\N	7005.4000	7005.4000	7005.4000	1549	1549: FEDERAL BUREAU OF INVESTIGATION	15F067	15F067: DIVISION 1200	15F067	15F067: DIVISION 1200	\N	015	Department of Justice	\N	DC	USA	B	\N	8882041938	8888025651	\N	\N	0	\N	0	A	0	0	\N	D	\N	S	\N	\N	USA	N	NONE	F	N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	X	D	\N	7510	\N	\N	C	\N	\N	N	\N	\N	SP1	\N	B	\N	SBA	A	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	1	1	0	0	1	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:17:09.06757	2017-07-14 17:17:09.067578	12393098	4057	56177	3111	DJBP0514SC110045	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	\N	0	J	B	325611	SOAP AND OTHER DETERGENT MANUFACTURING	627857444	AMERICAN SANITARY PRODUCTS, INC.	627857444	COMPASS SOLID LAUNDRY DETERGENT SOLID GREEN (STAIN REMOVER)	211082552	04	AMERICAN SANITARY PRODUCTS, INC.	MILLERSVILLE	MD	211082552	04	303 NAJOLES RD STE 109	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170118	20170127	20170127	\N	20170118	\N	20152.8000	20152.8000	20152.8000	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B514	15B514: FCI SEAGOVILLE	15B514	15B514: FCI SEAGOVILLE	\N	015	Department of Justice	\N	MD	USA	B	\N	4107292033	4107293454	\N	\N	0	\N	0	A	0	0	\N	D	\N	S	\N	\N	USA	N	NONE	C	N	X	0	\N	X	0	\N	0	\N	NONE	1	1	\N	X	D	\N	3510	\N	ONE	C	\N	\N	N	\N	\N	SSS	\N	B	\N	NONE	E	N	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	1	1	0	0	1	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
2017-07-14 17:16:42.229805	2017-07-14 17:16:42.229813	12390651	4057	56177	664	DJBP0502SA110157	1540	FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	015	Department of Justice	\N	0	J	B	311991	PERISHABLE PREPARED FOOD MANUFACTURING	026543926	MCNEW'S PRODUCE CO	026543926	WEEKLY PRODUCE ORDER	756543610	01	MCNEW'S PRODUCE CO	HENDERSON	TX	756543610	01	400 US HWY 79 S	\N	\N	USA	USA: UNITED STATES OF AMERICA	20170105	20170112	20170112	\N	20170105	\N	11807.2500	11807.2500	11807.2500	1540	1540: FEDERAL PRISON SYSTEM / BUREAU OF PRISONS	15B502	15B502: FCC BEAUMONT	15B502	15B502: FCC BEAUMONT	\N	015	Department of Justice	\N	TX	USA	B	\N	9036574832	9036579427	\N	\N	0	\N	0	A	0	0	\N	D	\N	S	\N	\N	USA	X	NONE	F	X	X	0	\N	X	0	\N	0	\N	NONE	1	3	\N	X	C	\N	8915	\N	\N	C	\N	\N	X	\N	\N	SP1	\N	B	\N	SBA	E	X	0	\N	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	\N	0	0	0	0	0	0	\N	\N	\N	\N	0	0	0	0	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	0	0	0	\N	X	A
\.


--
-- PostgreSQL database dump complete
--

