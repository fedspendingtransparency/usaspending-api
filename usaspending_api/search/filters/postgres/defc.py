from django.db import connection
from django.db.models import Q

from usaspending_api.awards.models import FinancialAccountsByAwards


class DefCodes:
    underscore_name = "def_codes"

    @classmethod
    def build_def_codes_filter(cls, queryset, filter_values):
        """Build a SQL filter to only include Subawards that are associated with any
        DEF codes included in the API request

        Args:
            queryset (Queryset()): The existing queryset that will be added to.
            filter_values (List[str]): List of DEF codes to use in filtering.

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
                            on
                        ss.award_id = aws.award_id
                    where
                            array[{def_codes}] && aws.disaster_emergency_fund_codes
                    )
                    select
                        us.broker_subaward_id
                    from
                        unnested_sub as us
                    join disaster_emergency_fund_code as defc
                        on
                        defc.code = us.singular_defc
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
            results = [result[0] for result in results]

        q = Q(broker_subaward_id__in=results)
        return q
