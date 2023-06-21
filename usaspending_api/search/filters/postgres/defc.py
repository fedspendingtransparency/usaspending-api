from django.db import connection
from django.db.models import Q


class DefCodes:
    underscore_name = "def_codes"

    @classmethod
    def build_def_codes_filter(cls, filter_values):
        """Build a SQL filter to only include Subawards that are associated with any
        DEF codes included in the API request.

        Which subawards should be returned currently is currently
        decided by:

            1. JOINing the award_search table to the subaward_search table on the
                `award_id` fields WHERE there is any overlap between the disaster
                code(s) provided in the request and the award_search row's
                `disaster_emergency_fund_codes` array.
            2. We then SELECT the `broker_subaward_id` WHERE the UNNESTed disaster
                code equals one of the disaster codes provided in the API request
                and WHERE the subaward's `sub_action_date` is on or after the
                disaster code's `earliest_public_law_enactment_date`

        Args:
            filter_values (List[str]): List of DEF codes to use in filtering. These come
                from the API request.

        Returns:
            Q(): Subquery filter containing Award IDs that are associated with the
            provided DEF code filter(s).
        """

        with connection.cursor() as cursor:
            cursor.execute(
                """
                with unnested_sub as (
                    select
                        ss.broker_subaward_id,
                        ss.award_id,
                        unnest(aws.disaster_emergency_fund_codes) as "singular_defc",
                        ss.sub_action_date
                    from
                        rpt.subaward_search as ss
                    join rpt.award_search as aws
                        on ss.award_id = aws.award_id
                    where
                         array[{def_codes}] && aws.disaster_emergency_fund_codes
                    )
                    select
                        us.broker_subaward_id
                    from
                        unnested_sub as us
                    join disaster_emergency_fund_code as defc
                        on defc.code = us.singular_defc
                    where
                        us.singular_defc in ({def_codes})
                        and (
                            defc.earliest_public_law_enactment_date is null
                            or defc.earliest_public_law_enactment_date <= sub_action_date
                        );
                """.format(
                    def_codes=",".join([f"'{code}'" for code in filter_values])
                )
            )

            results = cursor.fetchall()

        # The above SQL will return 1 result (row) for each DEF code it's associated with IF that DEF code
        #   is also in the API request ({def_codes}). This means the same `broker_subaward_id` can be returned
        #   multiple times because each row matches a different DEF code from the API request, using a set
        #   removes these duplicate `broker_subaward_id` values.
        results = {result[0] for result in results}
        q = Q(broker_subaward_id__in=results)

        return q
