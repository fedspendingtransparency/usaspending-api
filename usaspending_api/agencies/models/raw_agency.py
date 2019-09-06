from distutils.util import strtobool
from django.db import transaction
from django.db.models import BooleanField, IntegerField, Manager, Model, TextField
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.helpers.text_helpers import standardize_nullable_whitespace as prep


class RawAgencyManager(Manager):

    def perform_import(self, file_path, force=False):
        """
        Accepts a path to the raw CSV file and imports it into the database.  Performs a modest
        amount of pre-processing in the form of whitespace standardization, nulls for blanks, and
        boolean conversion.  Returns the number of rows imported.

        If the force parameter is not True, performs a quick comparison against existing data.  If
        nothing has changed, nothing is imported and 0 is returned which allows the caller to
        circumvent loading downstream data sets.
        """
        agencies = read_csv_file_as_list_of_dictionaries(file_path)
        if len(agencies) < 1:
            raise RuntimeError("Agency file '{}' appears to be empty".format(file_path))

        # Convert the agency data into something the database will be happy with.
        agencies = [
            {
                "row_number": row_number,
                "cgac_agency_code": prep(agency["CGAC AGENCY CODE"]),
                "agency_name": prep(agency["AGENCY NAME"]),
                "agency_abbreviation": prep(agency["AGENCY ABBREVIATION"]),
                "frec": prep(agency["FREC"]),
                "frec_entity_description": prep(agency["FREC Entity Description"]),
                "frec_abbreviation": prep(agency["FREC ABBREVIATION"]),
                "subtier_code": prep(agency["SUBTIER CODE"]),
                "subtier_name": prep(agency["SUBTIER NAME"]),
                "subtier_abbreviation": prep(agency["SUBTIER ABBREVIATION"]),
                "admin_org_name": prep(agency["Admin Org Name"]),
                "admin_org": prep(agency["ADMIN_ORG"]),
                "toptier_flag": strtobool(prep(agency["TOPTIER_FLAG"])),
                "is_frec": strtobool(prep(agency["IS_FREC"])),
                "frec_cgac_association": strtobool(prep(agency["FREC CGAC ASSOCIATION"])),
                "user_selectable_on_usaspending_gov": strtobool(prep(agency["USER SELECTABLE ON USASPENDING.GOV"])),
                "mission": prep(agency["MISSION"]),
                "website": prep(agency["WEBSITE"]),
                "congressional_justification": prep(agency["CONGRESSIONAL JUSTIFICATION"]),
                "icon_filename": prep(agency["ICON FILENAME"]),
                "comment": prep(agency["COMMENT"]),
            }
            for row_number, agency in enumerate(agencies)
        ]

        if force is not True:
            existing_agencies = list(RawAgency.objects.all().order_by("row_number").values())
            if agencies == existing_agencies:
                return 0

        with transaction.atomic():
            self.get_queryset().all().delete()
            self.bulk_create([RawAgency(**agency) for agency in agencies])

        return len(agencies)


class RawAgency(Model):
    """
    A copy of the raw agency_codes.csv file as maintained by the product owner.

    I would recommend never joining directly to this table from production code and never pointing
    hard foreign keys at it.  Always carve off what you need into independent tables.  I would even
    steer away from views of any sort based on this table as raw tables are more likely to change
    and views prevent structural changes to their underlying data sources.  Independent tables are
    your most flexible option.
    """
    row_number = IntegerField(primary_key=True)
    cgac_agency_code = TextField(blank=True, null=True)
    agency_name = TextField(blank=True, null=True)
    agency_abbreviation = TextField(blank=True, null=True)
    frec = TextField(blank=True, null=True)
    frec_entity_description = TextField(blank=True, null=True)
    frec_abbreviation = TextField(blank=True, null=True)
    subtier_code = TextField(blank=True, null=True)
    subtier_name = TextField(blank=True, null=True)
    subtier_abbreviation = TextField(blank=True, null=True)
    admin_org_name = TextField(blank=True, null=True)
    admin_org = TextField(blank=True, null=True)
    toptier_flag = BooleanField()
    is_frec = BooleanField()
    frec_cgac_association = BooleanField()
    user_selectable_on_usaspending_gov = BooleanField()
    mission = TextField(blank=True, null=True)
    website = TextField(blank=True, null=True)
    congressional_justification = TextField(blank=True, null=True)
    icon_filename = TextField(blank=True, null=True)
    comment = TextField(blank=True, null=True)

    objects = RawAgencyManager()

    class Meta:
        db_table = "raw_agency"

    def __repr__(self):
        return "[{}] []/[{}] {}/[{}] {}/[{}] {}".format(
            self.cgac_agency_code, self.agency_name,
            self.frec, self.frec_entity_description,
            self.subtier_code, self.subtier_name,
            self.admin_org, self.admin_org_name,
        )
