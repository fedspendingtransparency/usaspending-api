import logging
import re

from django.db import models
from django.db.models import Q
from usaspending_api.common.models import DataSourceTrackedModel, DeleteIfChildlessMixin
from usaspending_api.references.abbreviations import code_to_state, state_to_code
from usaspending_api.references.models import RefCityCountyCode


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
        blank=True, null=True, verbose_name="Primary Place Of Performance Location Code"
    )
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
    transaction_unique_id = models.TextField(
        blank=False, null=False, default="NONE", verbose_name="Transaction Unique ID"
    )

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
        if self.country_name == "UNITED STATES":
            if not self.state_code:
                self.state_code = state_to_code.get(self.state_name)
            elif not self.state_name:
                state_obj = code_to_state.get(self.state_code)

                if state_obj:
                    self.state_name = state_obj["name"]

    zip_code_pattern = re.compile(r"^(\d{5})\-?(\d{4})?$")

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
                "county_name": temp_county_name,
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
                logging.getLogger("debug").info(
                    "Could not find single matching city/county for following arguments:"
                    + str(q_kwargs)
                    + "; got "
                    + str(matched_reference.count())
                )
