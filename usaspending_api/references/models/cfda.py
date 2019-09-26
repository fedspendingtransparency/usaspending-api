from django.db import models
from usaspending_api.common.models import DataSourceTrackedModel


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
