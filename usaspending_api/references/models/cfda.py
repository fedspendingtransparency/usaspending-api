from django.db import models
from pgvector.django import VectorField

from usaspending_api.common.mixins import EmbeddingMixin
from usaspending_api.common.models import DataSourceTrackedModel


class Cfda(EmbeddingMixin, DataSourceTrackedModel):
    embedding_dimensions = 512

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
    embedding = VectorField(dimensions=512, null=True, blank=True)

    class Meta:
        managed = True

    def __str__(self):
        return "%s" % self.program_title

    def get_embedding_text(self) -> str | None:
        parts = []

        if self.program_title:
            parts.append(self.program_title.strip())

        if self.popular_name:
            parts.append(f"({self.popular_name.strip()})")

        if self.objectives:
            objectives = self._truncate_field(self.objectives, 2000)
            parts.append(objectives)

        if self.types_of_assistance:
            parts.append(f"Assistance: {self.types_of_assistance.strip()}")

        if self.applicant_eligibility:
            eligibility = self._truncate_field(self.applicant_eligibility, 500)
            parts.append(f"Eligible: {eligibility}")

        if self.uses_and_use_restrictions:
            uses = self._truncate_field(self.uses_and_use_restrictions, 500)
            parts.append(f"Uses: {uses}")

        if self.examples_of_funded_projects:
            examples = self._truncate_field(self.examples_of_funded_projects, 500)
            parts.append(f"Examples: {examples}")

        return " | ".join(parts) if parts else None

    @staticmethod
    def _truncate_field(text: str, max_length: int) -> str:
        if not text:
            return ""
        text = text.strip()
        if len(text) > max_length:
            return text[:max_length] + "..."
        return text
