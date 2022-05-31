"""
Manually generated to handle swapping multiple SourceProcurementTransaction from Text to Boolean.
The process below is:
* Create new "_temp" BooleanField(null=True, blank=True)
* Backfill the TextField values to BooleanField(null=True, blank=True)
* Rename the current TextField columns to "_old"
* Rename the new BooleanField(null=True, blank=True) columns to not have "_temp"
* Remove the "_old" columns
"""

from django.db import migrations, models
from django.db.models import F
from django.db.models.functions import Cast


def copy_fields(apps, _):
    SourceProcurementTransaction = apps.get_model("transactions", "SourceProcurementTransaction")
    SourceProcurementTransaction.objects.all().update(
        small_business_competitive_temp=Cast(F("small_business_competitive"), output_field=models.BooleanField()),
        city_local_government_temp=Cast(F("city_local_government"), output_field=models.BooleanField()),
        county_local_government_temp=Cast(F("county_local_government"), output_field=models.BooleanField()),
        inter_municipal_local_gove_temp=Cast(F("inter_municipal_local_gove"), output_field=models.BooleanField()),
        local_government_owned_temp=Cast(F("local_government_owned"), output_field=models.BooleanField()),
        municipality_local_governm_temp=Cast(F("municipality_local_governm"), output_field=models.BooleanField()),
        school_district_local_gove_temp=Cast(F("school_district_local_gove"), output_field=models.BooleanField()),
        township_local_government_temp=Cast(F("township_local_government"), output_field=models.BooleanField()),
        us_state_government_temp=Cast(F("us_state_government"), output_field=models.BooleanField()),
        us_federal_government_temp=Cast(F("us_federal_government"), output_field=models.BooleanField()),
        federal_agency_temp=Cast(F("federal_agency"), output_field=models.BooleanField()),
        federally_funded_research_temp=Cast(F("federally_funded_research"), output_field=models.BooleanField()),
        us_tribal_government_temp=Cast(F("us_tribal_government"), output_field=models.BooleanField()),
        foreign_government_temp=Cast(F("foreign_government"), output_field=models.BooleanField()),
        community_developed_corpor_temp=Cast(F("community_developed_corpor"), output_field=models.BooleanField()),
        labor_surplus_area_firm_temp=Cast(F("labor_surplus_area_firm"), output_field=models.BooleanField()),
        corporate_entity_not_tax_e_temp=Cast(F("corporate_entity_not_tax_e"), output_field=models.BooleanField()),
        corporate_entity_tax_exemp_temp=Cast(F("corporate_entity_tax_exemp"), output_field=models.BooleanField()),
        partnership_or_limited_lia_temp=Cast(F("partnership_or_limited_lia"), output_field=models.BooleanField()),
        sole_proprietorship_temp=Cast(F("sole_proprietorship"), output_field=models.BooleanField()),
        small_agricultural_coopera_temp=Cast(F("small_agricultural_coopera"), output_field=models.BooleanField()),
        international_organization_temp=Cast(F("international_organization"), output_field=models.BooleanField()),
        us_government_entity_temp=Cast(F("us_government_entity"), output_field=models.BooleanField()),
        emerging_small_business_temp=Cast(F("emerging_small_business"), output_field=models.BooleanField()),
        c8a_program_participant_temp=Cast(F("c8a_program_participant"), output_field=models.BooleanField()),
        sba_certified_8_a_joint_ve_temp=Cast(F("sba_certified_8_a_joint_ve"), output_field=models.BooleanField()),
        dot_certified_disadvantage_temp=Cast(F("dot_certified_disadvantage"), output_field=models.BooleanField()),
        self_certified_small_disad_temp=Cast(F("self_certified_small_disad"), output_field=models.BooleanField()),
        historically_underutilized_temp=Cast(F("historically_underutilized"), output_field=models.BooleanField()),
        small_disadvantaged_busine_temp=Cast(F("small_disadvantaged_busine"), output_field=models.BooleanField()),
        the_ability_one_program_temp=Cast(F("the_ability_one_program"), output_field=models.BooleanField()),
        historically_black_college_temp=Cast(F("historically_black_college"), output_field=models.BooleanField()),
        c1862_land_grant_college_temp=Cast(F("c1862_land_grant_college"), output_field=models.BooleanField()),
        c1890_land_grant_college_temp=Cast(F("c1890_land_grant_college"), output_field=models.BooleanField()),
        c1994_land_grant_college_temp=Cast(F("c1994_land_grant_college"), output_field=models.BooleanField()),
        minority_institution_temp=Cast(F("minority_institution"), output_field=models.BooleanField()),
        private_university_or_coll_temp=Cast(F("private_university_or_coll"), output_field=models.BooleanField()),
        school_of_forestry_temp=Cast(F("school_of_forestry"), output_field=models.BooleanField()),
        state_controlled_instituti_temp=Cast(F("state_controlled_instituti"), output_field=models.BooleanField()),
        tribal_college_temp=Cast(F("tribal_college"), output_field=models.BooleanField()),
        veterinary_college_temp=Cast(F("veterinary_college"), output_field=models.BooleanField()),
        educational_institution_temp=Cast(F("educational_institution"), output_field=models.BooleanField()),
        alaskan_native_servicing_i_temp=Cast(F("alaskan_native_servicing_i"), output_field=models.BooleanField()),
        community_development_corp_temp=Cast(F("community_development_corp"), output_field=models.BooleanField()),
        native_hawaiian_servicing_temp=Cast(F("native_hawaiian_servicing"), output_field=models.BooleanField()),
        domestic_shelter_temp=Cast(F("domestic_shelter"), output_field=models.BooleanField()),
        manufacturer_of_goods_temp=Cast(F("manufacturer_of_goods"), output_field=models.BooleanField()),
        hospital_flag_temp=Cast(F("hospital_flag"), output_field=models.BooleanField()),
        veterinary_hospital_temp=Cast(F("veterinary_hospital"), output_field=models.BooleanField()),
        hispanic_servicing_institu_temp=Cast(F("hispanic_servicing_institu"), output_field=models.BooleanField()),
        foundation_temp=Cast(F("foundation"), output_field=models.BooleanField()),
        woman_owned_business_temp=Cast(F("woman_owned_business"), output_field=models.BooleanField()),
        minority_owned_business_temp=Cast(F("minority_owned_business"), output_field=models.BooleanField()),
        women_owned_small_business_temp=Cast(F("women_owned_small_business"), output_field=models.BooleanField()),
        economically_disadvantaged_temp=Cast(F("economically_disadvantaged"), output_field=models.BooleanField()),
        joint_venture_women_owned_temp=Cast(F("joint_venture_women_owned"), output_field=models.BooleanField()),
        joint_venture_economically_temp=Cast(F("joint_venture_economically"), output_field=models.BooleanField()),
        veteran_owned_business_temp=Cast(F("veteran_owned_business"), output_field=models.BooleanField()),
        service_disabled_veteran_o_temp=Cast(F("service_disabled_veteran_o"), output_field=models.BooleanField()),
        contracts_temp=Cast(F("contracts"), output_field=models.BooleanField()),
        grants_temp=Cast(F("grants"), output_field=models.BooleanField()),
        receives_contracts_and_gra_temp=Cast(F("receives_contracts_and_gra"), output_field=models.BooleanField()),
        airport_authority_temp=Cast(F("airport_authority"), output_field=models.BooleanField()),
        council_of_governments_temp=Cast(F("council_of_governments"), output_field=models.BooleanField()),
        housing_authorities_public_temp=Cast(F("housing_authorities_public"), output_field=models.BooleanField()),
        interstate_entity_temp=Cast(F("interstate_entity"), output_field=models.BooleanField()),
        planning_commission_temp=Cast(F("planning_commission"), output_field=models.BooleanField()),
        port_authority_temp=Cast(F("port_authority"), output_field=models.BooleanField()),
        transit_authority_temp=Cast(F("transit_authority"), output_field=models.BooleanField()),
        subchapter_s_corporation_temp=Cast(F("subchapter_s_corporation"), output_field=models.BooleanField()),
        limited_liability_corporat_temp=Cast(F("limited_liability_corporat"), output_field=models.BooleanField()),
        foreign_owned_and_located_temp=Cast(F("foreign_owned_and_located"), output_field=models.BooleanField()),
        american_indian_owned_busi_temp=Cast(F("american_indian_owned_busi"), output_field=models.BooleanField()),
        alaskan_native_owned_corpo_temp=Cast(F("alaskan_native_owned_corpo"), output_field=models.BooleanField()),
        indian_tribe_federally_rec_temp=Cast(F("indian_tribe_federally_rec"), output_field=models.BooleanField()),
        native_hawaiian_owned_busi_temp=Cast(F("native_hawaiian_owned_busi"), output_field=models.BooleanField()),
        tribally_owned_business_temp=Cast(F("tribally_owned_business"), output_field=models.BooleanField()),
        asian_pacific_american_own_temp=Cast(F("asian_pacific_american_own"), output_field=models.BooleanField()),
        black_american_owned_busin_temp=Cast(F("black_american_owned_busin"), output_field=models.BooleanField()),
        hispanic_american_owned_bu_temp=Cast(F("hispanic_american_owned_bu"), output_field=models.BooleanField()),
        native_american_owned_busi_temp=Cast(F("native_american_owned_busi"), output_field=models.BooleanField()),
        subcontinent_asian_asian_i_temp=Cast(F("subcontinent_asian_asian_i"), output_field=models.BooleanField()),
        other_minority_owned_busin_temp=Cast(F("other_minority_owned_busin"), output_field=models.BooleanField()),
        for_profit_organization_temp=Cast(F("for_profit_organization"), output_field=models.BooleanField()),
        nonprofit_organization_temp=Cast(F("nonprofit_organization"), output_field=models.BooleanField()),
        other_not_for_profit_organ_temp=Cast(F("other_not_for_profit_organ"), output_field=models.BooleanField()),
        us_local_government_temp=Cast(F("us_local_government"), output_field=models.BooleanField()),
    )


def reverse_copy_fields(apps, _):
    SourceProcurementTransaction = apps.get_model("transactions", "SourceProcurementTransaction")
    SourceProcurementTransaction.objects.all().update(
        small_business_competitive=Cast(F("small_business_competitive_temp"), output_field=models.TextField()),
        city_local_government=Cast(F("city_local_government_temp"), output_field=models.TextField()),
        county_local_government=Cast(F("county_local_government_temp"), output_field=models.TextField()),
        inter_municipal_local_gove=Cast(F("inter_municipal_local_gove_temp"), output_field=models.TextField()),
        local_government_owned=Cast(F("local_government_owned_temp"), output_field=models.TextField()),
        municipality_local_governm=Cast(F("municipality_local_governm_temp"), output_field=models.TextField()),
        school_district_local_gove=Cast(F("school_district_local_gove_temp"), output_field=models.TextField()),
        township_local_government=Cast(F("township_local_government_temp"), output_field=models.TextField()),
        us_state_government=Cast(F("us_state_government_temp"), output_field=models.TextField()),
        us_federal_government=Cast(F("us_federal_government_temp"), output_field=models.TextField()),
        federal_agency=Cast(F("federal_agency_temp"), output_field=models.TextField()),
        federally_funded_research=Cast(F("federally_funded_research_temp"), output_field=models.TextField()),
        us_tribal_government=Cast(F("us_tribal_government_temp"), output_field=models.TextField()),
        foreign_government=Cast(F("foreign_government_temp"), output_field=models.TextField()),
        community_developed_corpor=Cast(F("community_developed_corpor_temp"), output_field=models.TextField()),
        labor_surplus_area_firm=Cast(F("labor_surplus_area_firm_temp"), output_field=models.TextField()),
        corporate_entity_not_tax_e=Cast(F("corporate_entity_not_tax_e_temp"), output_field=models.TextField()),
        corporate_entity_tax_exemp=Cast(F("corporate_entity_tax_exemp_temp"), output_field=models.TextField()),
        partnership_or_limited_lia=Cast(F("partnership_or_limited_lia_temp"), output_field=models.TextField()),
        sole_proprietorship=Cast(F("sole_proprietorship_temp"), output_field=models.TextField()),
        small_agricultural_coopera=Cast(F("small_agricultural_coopera_temp"), output_field=models.TextField()),
        international_organization=Cast(F("international_organization_temp"), output_field=models.TextField()),
        us_government_entity=Cast(F("us_government_entity_temp"), output_field=models.TextField()),
        emerging_small_business=Cast(F("emerging_small_business_temp"), output_field=models.TextField()),
        c8a_program_participant=Cast(F("c8a_program_participant_temp"), output_field=models.TextField()),
        sba_certified_8_a_joint_ve=Cast(F("sba_certified_8_a_joint_ve_temp"), output_field=models.TextField()),
        dot_certified_disadvantage=Cast(F("dot_certified_disadvantage_temp"), output_field=models.TextField()),
        self_certified_small_disad=Cast(F("self_certified_small_disad_temp"), output_field=models.TextField()),
        historically_underutilized=Cast(F("historically_underutilized_temp"), output_field=models.TextField()),
        small_disadvantaged_busine=Cast(F("small_disadvantaged_busine_temp"), output_field=models.TextField()),
        the_ability_one_program=Cast(F("the_ability_one_program_temp"), output_field=models.TextField()),
        historically_black_college=Cast(F("historically_black_college_temp"), output_field=models.TextField()),
        c1862_land_grant_college=Cast(F("c1862_land_grant_college_temp"), output_field=models.TextField()),
        c1890_land_grant_college=Cast(F("c1890_land_grant_college_temp"), output_field=models.TextField()),
        c1994_land_grant_college=Cast(F("c1994_land_grant_college_temp"), output_field=models.TextField()),
        minority_institution=Cast(F("minority_institution_temp"), output_field=models.TextField()),
        private_university_or_coll=Cast(F("private_university_or_coll_temp"), output_field=models.TextField()),
        school_of_forestry=Cast(F("school_of_forestry_temp"), output_field=models.TextField()),
        state_controlled_instituti=Cast(F("state_controlled_instituti_temp"), output_field=models.TextField()),
        tribal_college=Cast(F("tribal_college_temp"), output_field=models.TextField()),
        veterinary_college=Cast(F("veterinary_college_temp"), output_field=models.TextField()),
        educational_institution=Cast(F("educational_institution_temp"), output_field=models.TextField()),
        alaskan_native_servicing_i=Cast(F("alaskan_native_servicing_i_temp"), output_field=models.TextField()),
        community_development_corp=Cast(F("community_development_corp_temp"), output_field=models.TextField()),
        native_hawaiian_servicing=Cast(F("native_hawaiian_servicing_temp"), output_field=models.TextField()),
        domestic_shelter=Cast(F("domestic_shelter_temp"), output_field=models.TextField()),
        manufacturer_of_goods=Cast(F("manufacturer_of_goods_temp"), output_field=models.TextField()),
        hospital_flag=Cast(F("hospital_flag_temp"), output_field=models.TextField()),
        veterinary_hospital=Cast(F("veterinary_hospital_temp"), output_field=models.TextField()),
        hispanic_servicing_institu=Cast(F("hispanic_servicing_institu_temp"), output_field=models.TextField()),
        foundation=Cast(F("foundation_temp"), output_field=models.TextField()),
        woman_owned_business=Cast(F("woman_owned_business_temp"), output_field=models.TextField()),
        minority_owned_business=Cast(F("minority_owned_business_temp"), output_field=models.TextField()),
        women_owned_small_business=Cast(F("women_owned_small_business_temp"), output_field=models.TextField()),
        economically_disadvantaged=Cast(F("economically_disadvantaged_temp"), output_field=models.TextField()),
        joint_venture_women_owned=Cast(F("joint_venture_women_owned_temp"), output_field=models.TextField()),
        joint_venture_economically=Cast(F("joint_venture_economically_temp"), output_field=models.TextField()),
        veteran_owned_business=Cast(F("veteran_owned_business_temp"), output_field=models.TextField()),
        service_disabled_veteran_o=Cast(F("service_disabled_veteran_o_temp"), output_field=models.TextField()),
        contracts=Cast(F("contracts_temp"), output_field=models.TextField()),
        grants=Cast(F("grants_temp"), output_field=models.TextField()),
        receives_contracts_and_gra=Cast(F("receives_contracts_and_gra_temp"), output_field=models.TextField()),
        airport_authority=Cast(F("airport_authority_temp"), output_field=models.TextField()),
        council_of_governments=Cast(F("council_of_governments_temp"), output_field=models.TextField()),
        housing_authorities_public=Cast(F("housing_authorities_public_temp"), output_field=models.TextField()),
        interstate_entity=Cast(F("interstate_entity_temp"), output_field=models.TextField()),
        planning_commission=Cast(F("planning_commission_temp"), output_field=models.TextField()),
        port_authority=Cast(F("port_authority_temp"), output_field=models.TextField()),
        transit_authority=Cast(F("transit_authority_temp"), output_field=models.TextField()),
        subchapter_s_corporation=Cast(F("subchapter_s_corporation_temp"), output_field=models.TextField()),
        limited_liability_corporat=Cast(F("limited_liability_corporat_temp"), output_field=models.TextField()),
        foreign_owned_and_located=Cast(F("foreign_owned_and_located_temp"), output_field=models.TextField()),
        american_indian_owned_busi=Cast(F("american_indian_owned_busi_temp"), output_field=models.TextField()),
        alaskan_native_owned_corpo=Cast(F("alaskan_native_owned_corpo_temp"), output_field=models.TextField()),
        indian_tribe_federally_rec=Cast(F("indian_tribe_federally_rec_temp"), output_field=models.TextField()),
        native_hawaiian_owned_busi=Cast(F("native_hawaiian_owned_busi_temp"), output_field=models.TextField()),
        tribally_owned_business=Cast(F("tribally_owned_business_temp"), output_field=models.TextField()),
        asian_pacific_american_own=Cast(F("asian_pacific_american_own_temp"), output_field=models.TextField()),
        black_american_owned_busin=Cast(F("black_american_owned_busin_temp"), output_field=models.TextField()),
        hispanic_american_owned_bu=Cast(F("hispanic_american_owned_bu_temp"), output_field=models.TextField()),
        native_american_owned_busi=Cast(F("native_american_owned_busi_temp"), output_field=models.TextField()),
        subcontinent_asian_asian_i=Cast(F("subcontinent_asian_asian_i_temp"), output_field=models.TextField()),
        other_minority_owned_busin=Cast(F("other_minority_owned_busin_temp"), output_field=models.TextField()),
        for_profit_organization=Cast(F("for_profit_organization_temp"), output_field=models.TextField()),
        nonprofit_organization=Cast(F("nonprofit_organization_temp"), output_field=models.TextField()),
        other_not_for_profit_organ=Cast(F("other_not_for_profit_organ_temp"), output_field=models.TextField()),
        us_local_government=Cast(F("us_local_government_temp"), output_field=models.TextField()),
    )


class Migration(migrations.Migration):

    dependencies = [
        ('transactions', '0006_auto_20210929_2219'),
    ]

    operations = [
        # Add temporary fields
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='small_business_competitive_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='city_local_government_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='county_local_government_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='inter_municipal_local_gove_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='local_government_owned_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='municipality_local_governm_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='school_district_local_gove_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='township_local_government_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='us_state_government_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='us_federal_government_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='federal_agency_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='federally_funded_research_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='us_tribal_government_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='foreign_government_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='community_developed_corpor_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='labor_surplus_area_firm_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='corporate_entity_not_tax_e_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='corporate_entity_tax_exemp_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='partnership_or_limited_lia_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='sole_proprietorship_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='small_agricultural_coopera_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='international_organization_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='us_government_entity_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='emerging_small_business_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='c8a_program_participant_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='sba_certified_8_a_joint_ve_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='dot_certified_disadvantage_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='self_certified_small_disad_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='historically_underutilized_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='small_disadvantaged_busine_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='the_ability_one_program_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='historically_black_college_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='c1862_land_grant_college_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='c1890_land_grant_college_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='c1994_land_grant_college_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='minority_institution_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='private_university_or_coll_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='school_of_forestry_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='state_controlled_instituti_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='tribal_college_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='veterinary_college_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='educational_institution_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='alaskan_native_servicing_i_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='community_development_corp_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='native_hawaiian_servicing_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='domestic_shelter_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='manufacturer_of_goods_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='hospital_flag_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='veterinary_hospital_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='hispanic_servicing_institu_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='foundation_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='woman_owned_business_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='minority_owned_business_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='women_owned_small_business_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='economically_disadvantaged_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='joint_venture_women_owned_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='joint_venture_economically_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='veteran_owned_business_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='service_disabled_veteran_o_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='contracts_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='grants_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='receives_contracts_and_gra_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='airport_authority_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='council_of_governments_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='housing_authorities_public_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='interstate_entity_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='planning_commission_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='port_authority_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='transit_authority_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='subchapter_s_corporation_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='limited_liability_corporat_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='foreign_owned_and_located_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='american_indian_owned_busi_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='alaskan_native_owned_corpo_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='indian_tribe_federally_rec_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='native_hawaiian_owned_busi_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='tribally_owned_business_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='asian_pacific_american_own_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='black_american_owned_busin_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='hispanic_american_owned_bu_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='native_american_owned_busi_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='subcontinent_asian_asian_i_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='other_minority_owned_busin_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='for_profit_organization_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='nonprofit_organization_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='other_not_for_profit_organ_temp',
            field=models.BooleanField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='sourceprocurementtransaction',
            name='us_local_government_temp',
            field=models.BooleanField(null=True, blank=True),
        ),

        # Call Python function to backfill values
        migrations.RunPython(copy_fields, reverse_code=reverse_copy_fields),

        # Change the name of the current TextField
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='small_business_competitive',
            new_name='small_business_competitive_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='city_local_government',
            new_name='city_local_government_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='county_local_government',
            new_name='county_local_government_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='inter_municipal_local_gove',
            new_name='inter_municipal_local_gove_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='local_government_owned',
            new_name='local_government_owned_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='municipality_local_governm',
            new_name='municipality_local_governm_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='school_district_local_gove',
            new_name='school_district_local_gove_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='township_local_government',
            new_name='township_local_government_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_state_government',
            new_name='us_state_government_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_federal_government',
            new_name='us_federal_government_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='federal_agency',
            new_name='federal_agency_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='federally_funded_research',
            new_name='federally_funded_research_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_tribal_government',
            new_name='us_tribal_government_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='foreign_government',
            new_name='foreign_government_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='community_developed_corpor',
            new_name='community_developed_corpor_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='labor_surplus_area_firm',
            new_name='labor_surplus_area_firm_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='corporate_entity_not_tax_e',
            new_name='corporate_entity_not_tax_e_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='corporate_entity_tax_exemp',
            new_name='corporate_entity_tax_exemp_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='partnership_or_limited_lia',
            new_name='partnership_or_limited_lia_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='sole_proprietorship',
            new_name='sole_proprietorship_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='small_agricultural_coopera',
            new_name='small_agricultural_coopera_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='international_organization',
            new_name='international_organization_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_government_entity',
            new_name='us_government_entity_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='emerging_small_business',
            new_name='emerging_small_business_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='c8a_program_participant',
            new_name='c8a_program_participant_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='sba_certified_8_a_joint_ve',
            new_name='sba_certified_8_a_joint_ve_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='dot_certified_disadvantage',
            new_name='dot_certified_disadvantage_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='self_certified_small_disad',
            new_name='self_certified_small_disad_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='historically_underutilized',
            new_name='historically_underutilized_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='small_disadvantaged_busine',
            new_name='small_disadvantaged_busine_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='the_ability_one_program',
            new_name='the_ability_one_program_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='historically_black_college',
            new_name='historically_black_college_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='c1862_land_grant_college',
            new_name='c1862_land_grant_college_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='c1890_land_grant_college',
            new_name='c1890_land_grant_college_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='c1994_land_grant_college',
            new_name='c1994_land_grant_college_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='minority_institution',
            new_name='minority_institution_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='private_university_or_coll',
            new_name='private_university_or_coll_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='school_of_forestry',
            new_name='school_of_forestry_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='state_controlled_instituti',
            new_name='state_controlled_instituti_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='tribal_college',
            new_name='tribal_college_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='veterinary_college',
            new_name='veterinary_college_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='educational_institution',
            new_name='educational_institution_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='alaskan_native_servicing_i',
            new_name='alaskan_native_servicing_i_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='community_development_corp',
            new_name='community_development_corp_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='native_hawaiian_servicing',
            new_name='native_hawaiian_servicing_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='domestic_shelter',
            new_name='domestic_shelter_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='manufacturer_of_goods',
            new_name='manufacturer_of_goods_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='hospital_flag',
            new_name='hospital_flag_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='veterinary_hospital',
            new_name='veterinary_hospital_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='hispanic_servicing_institu',
            new_name='hispanic_servicing_institu_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='foundation',
            new_name='foundation_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='woman_owned_business',
            new_name='woman_owned_business_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='minority_owned_business',
            new_name='minority_owned_business_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='women_owned_small_business',
            new_name='women_owned_small_business_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='economically_disadvantaged',
            new_name='economically_disadvantaged_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='joint_venture_women_owned',
            new_name='joint_venture_women_owned_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='joint_venture_economically',
            new_name='joint_venture_economically_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='veteran_owned_business',
            new_name='veteran_owned_business_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='service_disabled_veteran_o',
            new_name='service_disabled_veteran_o_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='contracts',
            new_name='contracts_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='grants',
            new_name='grants_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='receives_contracts_and_gra',
            new_name='receives_contracts_and_gra_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='airport_authority',
            new_name='airport_authority_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='council_of_governments',
            new_name='council_of_governments_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='housing_authorities_public',
            new_name='housing_authorities_public_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='interstate_entity',
            new_name='interstate_entity_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='planning_commission',
            new_name='planning_commission_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='port_authority',
            new_name='port_authority_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='transit_authority',
            new_name='transit_authority_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='subchapter_s_corporation',
            new_name='subchapter_s_corporation_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='limited_liability_corporat',
            new_name='limited_liability_corporat_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='foreign_owned_and_located',
            new_name='foreign_owned_and_located_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='american_indian_owned_busi',
            new_name='american_indian_owned_busi_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='alaskan_native_owned_corpo',
            new_name='alaskan_native_owned_corpo_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='indian_tribe_federally_rec',
            new_name='indian_tribe_federally_rec_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='native_hawaiian_owned_busi',
            new_name='native_hawaiian_owned_busi_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='tribally_owned_business',
            new_name='tribally_owned_business_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='asian_pacific_american_own',
            new_name='asian_pacific_american_own_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='black_american_owned_busin',
            new_name='black_american_owned_busin_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='hispanic_american_owned_bu',
            new_name='hispanic_american_owned_bu_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='native_american_owned_busi',
            new_name='native_american_owned_busi_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='subcontinent_asian_asian_i',
            new_name='subcontinent_asian_asian_i_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='other_minority_owned_busin',
            new_name='other_minority_owned_busin_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='for_profit_organization',
            new_name='for_profit_organization_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='nonprofit_organization',
            new_name='nonprofit_organization_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='other_not_for_profit_organ',
            new_name='other_not_for_profit_organ_old'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_local_government',
            new_name='us_local_government_old'
        ),

        # Change the name of the new BooleanField
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='small_business_competitive_temp',
            new_name='small_business_competitive'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='city_local_government_temp',
            new_name='city_local_government'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='county_local_government_temp',
            new_name='county_local_government'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='inter_municipal_local_gove_temp',
            new_name='inter_municipal_local_gove'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='local_government_owned_temp',
            new_name='local_government_owned'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='municipality_local_governm_temp',
            new_name='municipality_local_governm'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='school_district_local_gove_temp',
            new_name='school_district_local_gove'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='township_local_government_temp',
            new_name='township_local_government'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_state_government_temp',
            new_name='us_state_government'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_federal_government_temp',
            new_name='us_federal_government'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='federal_agency_temp',
            new_name='federal_agency'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='federally_funded_research_temp',
            new_name='federally_funded_research'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_tribal_government_temp',
            new_name='us_tribal_government'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='foreign_government_temp',
            new_name='foreign_government'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='community_developed_corpor_temp',
            new_name='community_developed_corpor'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='labor_surplus_area_firm_temp',
            new_name='labor_surplus_area_firm'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='corporate_entity_not_tax_e_temp',
            new_name='corporate_entity_not_tax_e'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='corporate_entity_tax_exemp_temp',
            new_name='corporate_entity_tax_exemp'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='partnership_or_limited_lia_temp',
            new_name='partnership_or_limited_lia'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='sole_proprietorship_temp',
            new_name='sole_proprietorship'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='small_agricultural_coopera_temp',
            new_name='small_agricultural_coopera'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='international_organization_temp',
            new_name='international_organization'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_government_entity_temp',
            new_name='us_government_entity'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='emerging_small_business_temp',
            new_name='emerging_small_business'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='c8a_program_participant_temp',
            new_name='c8a_program_participant'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='sba_certified_8_a_joint_ve_temp',
            new_name='sba_certified_8_a_joint_ve'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='dot_certified_disadvantage_temp',
            new_name='dot_certified_disadvantage'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='self_certified_small_disad_temp',
            new_name='self_certified_small_disad'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='historically_underutilized_temp',
            new_name='historically_underutilized'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='small_disadvantaged_busine_temp',
            new_name='small_disadvantaged_busine'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='the_ability_one_program_temp',
            new_name='the_ability_one_program'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='historically_black_college_temp',
            new_name='historically_black_college'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='c1862_land_grant_college_temp',
            new_name='c1862_land_grant_college'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='c1890_land_grant_college_temp',
            new_name='c1890_land_grant_college'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='c1994_land_grant_college_temp',
            new_name='c1994_land_grant_college'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='minority_institution_temp',
            new_name='minority_institution'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='private_university_or_coll_temp',
            new_name='private_university_or_coll'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='school_of_forestry_temp',
            new_name='school_of_forestry'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='state_controlled_instituti_temp',
            new_name='state_controlled_instituti'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='tribal_college_temp',
            new_name='tribal_college'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='veterinary_college_temp',
            new_name='veterinary_college'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='educational_institution_temp',
            new_name='educational_institution'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='alaskan_native_servicing_i_temp',
            new_name='alaskan_native_servicing_i'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='community_development_corp_temp',
            new_name='community_development_corp'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='native_hawaiian_servicing_temp',
            new_name='native_hawaiian_servicing'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='domestic_shelter_temp',
            new_name='domestic_shelter'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='manufacturer_of_goods_temp',
            new_name='manufacturer_of_goods'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='hospital_flag_temp',
            new_name='hospital_flag'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='veterinary_hospital_temp',
            new_name='veterinary_hospital'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='hispanic_servicing_institu_temp',
            new_name='hispanic_servicing_institu'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='foundation_temp',
            new_name='foundation'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='woman_owned_business_temp',
            new_name='woman_owned_business'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='minority_owned_business_temp',
            new_name='minority_owned_business'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='women_owned_small_business_temp',
            new_name='women_owned_small_business'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='economically_disadvantaged_temp',
            new_name='economically_disadvantaged'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='joint_venture_women_owned_temp',
            new_name='joint_venture_women_owned'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='joint_venture_economically_temp',
            new_name='joint_venture_economically'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='veteran_owned_business_temp',
            new_name='veteran_owned_business'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='service_disabled_veteran_o_temp',
            new_name='service_disabled_veteran_o'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='contracts_temp',
            new_name='contracts'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='grants_temp',
            new_name='grants'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='receives_contracts_and_gra_temp',
            new_name='receives_contracts_and_gra'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='airport_authority_temp',
            new_name='airport_authority'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='council_of_governments_temp',
            new_name='council_of_governments'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='housing_authorities_public_temp',
            new_name='housing_authorities_public'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='interstate_entity_temp',
            new_name='interstate_entity'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='planning_commission_temp',
            new_name='planning_commission'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='port_authority_temp',
            new_name='port_authority'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='transit_authority_temp',
            new_name='transit_authority'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='subchapter_s_corporation_temp',
            new_name='subchapter_s_corporation'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='limited_liability_corporat_temp',
            new_name='limited_liability_corporat'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='foreign_owned_and_located_temp',
            new_name='foreign_owned_and_located'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='american_indian_owned_busi_temp',
            new_name='american_indian_owned_busi'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='alaskan_native_owned_corpo_temp',
            new_name='alaskan_native_owned_corpo'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='indian_tribe_federally_rec_temp',
            new_name='indian_tribe_federally_rec'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='native_hawaiian_owned_busi_temp',
            new_name='native_hawaiian_owned_busi'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='tribally_owned_business_temp',
            new_name='tribally_owned_business'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='asian_pacific_american_own_temp',
            new_name='asian_pacific_american_own'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='black_american_owned_busin_temp',
            new_name='black_american_owned_busin'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='hispanic_american_owned_bu_temp',
            new_name='hispanic_american_owned_bu'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='native_american_owned_busi_temp',
            new_name='native_american_owned_busi'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='subcontinent_asian_asian_i_temp',
            new_name='subcontinent_asian_asian_i'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='other_minority_owned_busin_temp',
            new_name='other_minority_owned_busin'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='for_profit_organization_temp',
            new_name='for_profit_organization'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='nonprofit_organization_temp',
            new_name='nonprofit_organization'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='other_not_for_profit_organ_temp',
            new_name='other_not_for_profit_organ'
        ),
        migrations.RenameField(
            model_name='sourceprocurementtransaction',
            old_name='us_local_government_temp',
            new_name='us_local_government'
        ),

        # Remove all of the old TextFields
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='small_business_competitive_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='city_local_government_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='county_local_government_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='inter_municipal_local_gove_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='local_government_owned_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='municipality_local_governm_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='school_district_local_gove_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='township_local_government_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='us_state_government_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='us_federal_government_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='federal_agency_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='federally_funded_research_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='us_tribal_government_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='foreign_government_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='community_developed_corpor_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='labor_surplus_area_firm_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='corporate_entity_not_tax_e_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='corporate_entity_tax_exemp_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='partnership_or_limited_lia_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='sole_proprietorship_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='small_agricultural_coopera_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='international_organization_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='us_government_entity_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='emerging_small_business_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='c8a_program_participant_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='sba_certified_8_a_joint_ve_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='dot_certified_disadvantage_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='self_certified_small_disad_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='historically_underutilized_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='small_disadvantaged_busine_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='the_ability_one_program_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='historically_black_college_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='c1862_land_grant_college_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='c1890_land_grant_college_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='c1994_land_grant_college_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='minority_institution_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='private_university_or_coll_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='school_of_forestry_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='state_controlled_instituti_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='tribal_college_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='veterinary_college_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='educational_institution_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='alaskan_native_servicing_i_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='community_development_corp_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='native_hawaiian_servicing_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='domestic_shelter_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='manufacturer_of_goods_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='hospital_flag_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='veterinary_hospital_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='hispanic_servicing_institu_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='foundation_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='woman_owned_business_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='minority_owned_business_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='women_owned_small_business_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='economically_disadvantaged_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='joint_venture_women_owned_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='joint_venture_economically_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='veteran_owned_business_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='service_disabled_veteran_o_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='contracts_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='grants_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='receives_contracts_and_gra_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='airport_authority_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='council_of_governments_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='housing_authorities_public_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='interstate_entity_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='planning_commission_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='port_authority_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='transit_authority_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='subchapter_s_corporation_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='limited_liability_corporat_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='foreign_owned_and_located_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='american_indian_owned_busi_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='alaskan_native_owned_corpo_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='indian_tribe_federally_rec_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='native_hawaiian_owned_busi_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='tribally_owned_business_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='asian_pacific_american_own_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='black_american_owned_busin_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='hispanic_american_owned_bu_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='native_american_owned_busi_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='subcontinent_asian_asian_i_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='other_minority_owned_busin_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='for_profit_organization_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='nonprofit_organization_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='other_not_for_profit_organ_old'
        ),
        migrations.RemoveField(
            model_name='sourceprocurementtransaction',
            name='us_local_government_old'
        ),
    ]
