FORMAT: 1A
HOST: https://api.usaspending.gov

# Agencies' Unlinked Awards [/api/v2/reporting/agencies/{toptier_code}/{fiscal_year}/{fiscal_period}/unlinked_awards/{type}/]

This endpoint is used to power USAspending.gov's About the Data \| Agencies unlinked data modals.

## GET

This endpoint returns the number of unlinked and linked awards for the agency in the provided fiscal year and period.

+ Parameters
    + `toptier_code`: `020` (required, string)
        The specific agency code.
    + `fiscal_year`: 2020 (required, number)
        The fiscal year of the submission
    + `fiscal_period`: 10 (required, number)
        The fiscal period of the submission. valid values: 2-12 (2 = November ... 12 = September)
        For retrieving quarterly submissions, provide the period which equals 'quarter * 3' (e.g. Q2 = P6)
    + `type`: `assistance` (required, enum[string])
        + Members
            + `assistance`
            + `procurement`

+ Response 200 (application/json)

    + Attributes (object)
        + `unlinked_file_c_award_count` (required, number)
        + `unlinked_file_d_award_count` (required, number)
        + `total_linked_award_count` (required, number)
    + Body

            {
                "unlinked_file_c_award_count": 123213,
                "unlinked_file_d_award_count": 43543,
                "total_linked_award_count": 12321312
            }