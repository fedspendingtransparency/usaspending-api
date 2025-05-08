import pytest

from datetime import datetime, timezone, date
from decimal import Decimal

from django.conf import settings
from django.core.management import call_command, CommandError
from django.db import connections, DEFAULT_DB_ALIAS
from django.test import TransactionTestCase
from model_bakery import baker
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.helpers.sql_helpers import ordered_dictionary_fetcher
from usaspending_api.etl.submission_loader_helpers.object_class import reset_object_class_cache
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.mark.usefixtures("broker_db_setup", "broker_server_dblink_setup")
class TestWithMultipleDatabases(TransactionTestCase):
    """
    Super unfortunate, but because we're using a dblink these data will need to actually be committed to
    the database so we use TransactionTestCase instead of TestCase.  This slows down tests so use sparingly.
    """

    databases = "__all__"

    def setUp(self):
        """
        Because we are adding fields and tables to the database and we want to keep track of that, we're
        using setUp instead of setUpClass so that we can retain some state on the object.  Another
        unfortunate side effect of using dblink and having to modify the database.  We can refactor this
        set of tests once either of those situations is alleviated.
        """

        reset_object_class_cache()

        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=1,
            agency_id="111",
            availability_type_code="X",
            main_account_code="1111",
            sub_account_code="111",
            tas_rendering_label="111-X-1111-111",
        )

        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=2,
            agency_id="222",
            availability_type_code="X",
            main_account_code="2222",
            sub_account_code="222",
            tas_rendering_label="222-X-2222-222",
        )

        baker.make("references.ObjectClass", major_object_class="10", object_class="10.1", direct_reimbursable="D")
        baker.make("references.ObjectClass", major_object_class="01", object_class="01.0", direct_reimbursable="D")

        baker.make("references.DisasterEmergencyFundCode", code="B", title="BB")
        baker.make("references.DisasterEmergencyFundCode", code="L", title="LL")
        baker.make("references.DisasterEmergencyFundCode", code="N", title="NN")

        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            id="2000041",
            submission_fiscal_year=2000,
            submission_fiscal_month=4,
            is_quarter=True,
        )
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            id="2000040",
            submission_fiscal_year=2000,
            submission_fiscal_month=4,
            is_quarter=False,
        )
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            id="2000041",
            submission_fiscal_year=2000,
            submission_fiscal_month=4,
            is_quarter=True,
        )
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            id="2000050",
            submission_fiscal_year=2000,
            submission_fiscal_month=5,
            is_quarter=False,
        )
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            id="2000060",
            submission_fiscal_year=2000,
            submission_fiscal_month=6,
            is_quarter=False,
        )
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            id="2000091",
            submission_fiscal_year=2000,
            submission_fiscal_month=9,
            is_quarter=True,
        )

        connection = connections[settings.DATA_BROKER_DB_ALIAS]
        with connection.cursor() as cursor:

            self._nuke_broker_data()

            cursor.execute(
                """
                insert into tas_lookup (
                    tas_id,
                    account_num,
                    agency_identifier,
                    availability_type_code,
                    main_account_code,
                    sub_account_code,
                    internal_start_date,
                    tas,
                    display_tas
                ) (values
                    (1, 1, '111', 'X', '1111', '111', '1900-01-01', '00011100000000X1111111', '000-111-X-1111-111'),
                    (2, 2, '222', 'X', '2222', '222', '1900-01-01', '00022200000000X2222222', '000-222-X-2222-222')
                )
                """
            )

            cursor.execute(
                """
                insert into submission (
                    submission_id,
                    cgac_code,
                    frec_code,
                    reporting_start_date,
                    reporting_end_date,
                    reporting_fiscal_year,
                    reporting_fiscal_period,
                    is_quarter_format,
                    is_fabs,
                    publish_status_id,
                    updated_at
                ) (values
                    -- bunch of good records with a mix of all kinds of settings
                    (1, '001', null, '2000-01-01', '2000-03-31', 2000, 4, true, false, 2, now()),
                    (2, null, '0002', '2000-01-01', '2000-01-31', 2000, 4, false, false, 3, now()),
                    (3, '003', '0003', '2000-02-01', '2000-02-29', 2000, 5, false, false, 2, now()),
                    (4, '004', null, '2000-03-01', '2000-03-31', 2000, 6, false, false, 3, now()),
                    (5, null, '005', '2000-04-01', '2000-06-30', 2000, 9, true, false, 2, now()),
                    -- submissions that should never return for various reasons
                    (6, '006', null, '2000-01-01', '2000-03-31', 2000, 4, true, false, 1, now()), -- not publish type 2 or 3
                    (7, '007', null, '2000-01-01', '2000-03-31', 2000, 4, true, true, 2, now()) -- D2
                )
                """
            )

            cursor.execute(
                """
                insert into publish_history (
                    publish_history_id,
                    submission_id,
                    updated_at
                ) (values
                    (1, 1, '1999-01-01'), (2, 2, '2000-01-02'), (3, 3, '2000-01-03'), (4, 4, '2000-01-04'),
                    (5, 5, '2000-01-05'), (6, 6, '2000-01-06'), (7, 7, '2000-01-07'), (8, 1, '2000-01-01')
                )
                """
            )

            cursor.execute(
                """
                insert into certify_history (
                    certify_history_id,
                    submission_id,
                    updated_at
                ) (values
                    (1, 1, '2000-02-01'), (3, 3, '2000-02-03'), (5, 5, '2000-02-05'), (7, 7, '2000-02-07')
                )
                """
            )

            cursor.execute(
                """
                insert into published_files_history (
                    published_files_history_id,
                    submission_id,
                    publish_history_id,
                    certify_history_id,
                    updated_at
                ) (values
                    (1, 1, 1, NULL, '1999-01-01'), (2, 2, 2, NULL, '2000-01-02'), (3, 3, 3, 3, '2000-01-03'),
                    (4, 4, 4, NULL, '2000-01-04'), (5, 5, 5, 5, '2000-01-05'), (6, 6, 6, NULL, '2000-01-06'),
                    (7, 7, 7, 7, '2000-01-07'), (8, 1, 8, 1, '2000-01-01')
                )
                """
            )

            cursor.execute(
                """
                insert into published_appropriation (
                    published_appropriation_id,
                    submission_id,
                    account_num,
                    total_budgetary_resources_cpe
                ) (values
                    (1, 1, 1, 11),
                    (2, 2, 1, 22),
                    (3, 3, 2, 33),
                    (4, 4, 2, 44),
                    (5, 5, 2, 55),
                    (6, 6, 2, 66),
                    (7, 7, 2, 77)
                )
                """
            )

            cursor.execute(
                """
                insert into published_object_class_program_activity (
                    published_object_class_program_activity_id,
                    submission_id,
                    account_num,
                    object_class,
                    by_direct_reimbursable_fun,
                    gross_outlay_amount_by_pro_cpe,
                    disaster_emergency_fund_code,
                    prior_year_adjustment
                ) (values
                    (1, 1, 1, '1010', 'D', 1111, null, 'x'),
                    (2, 1, 1, '1010', 'D', 2222, 'b', 'b'),
                    (3, 1, 1, '1010', 'D', 3333, 'L', 'p'),
                    (4, 2, 1, '1010', 'D', 4444, null, 'X'),
                    (5, 2, 1, '1010', 'D', 5555, null, 'B'),
                    (6, 2, 1, '1010', 'D', 6666, null, 'P'),
                    (7, 3, 2, '1010', 'D', 7777, 'L', 'X'),
                    (8, 3, 2, '1010', 'D', 8888, 'l', 'X'),
                    (9, 3, 2, '1010', 'D', 9999, 'L', 'X'),
                    (10, 4, 2, '1010', 'D', 1010, null, 'X'),
                    (11, 5, 2, '1010', 'D', 1111, 'B', 'X'),
                    (12, 6, 2, '1010', 'D', 1212, 'L', 'X'),
                    (13, 7, 2, '1010', 'D', 1313, 'n', 'X')
                )
                """
            )

            cursor.execute(
                """
                insert into published_award_financial (
                    published_award_financial_id,
                    submission_id,
                    account_num,
                    object_class,
                    by_direct_reimbursable_fun,
                    gross_outlay_amount_by_awa_cpe,
                    transaction_obligated_amou,
                    ussgl487200_downward_adjus_cpe,
                    ussgl497200_downward_adjus_cpe,
                    disaster_emergency_fund_code,
                    prior_year_adjustment
                ) (values
                    (1, 1, 1, '1010', 'D', 11111, 111110, -11, -111, null, 'x'),
                    (2, 1, 1, '1010', 'D', 22222, 222220, -22, -222, 'B', 'b'),
                    (3, 1, 1, '1010', 'D', 33333, 333330, -33, -333, 'l', 'p'),
                    (4, 2, 1, '1010', 'D', 44444, 444440, -44, -444, null, 'X'),
                    (5, 2, 1, '1010', 'D', 55555, 555550, -55, -555, null, 'B'),
                    (6, 2, 1, '1010', 'D', 66666, 666660, -66, -666, null, 'P'),
                    (7, 3, 2, '1010', 'D', 77777, 777770, -77, -777, 'L', 'X'),
                    (8, 3, 2, '1010', 'D', 88888, 888880, -88, -888, 'L', 'X'),
                    (9, 3, 2, '1010', 'D', 99999, 999990, -99, -999, 'L', 'X'),
                    (10, 4, 2, '1010', 'D', 10101, 101010, -10, -101, null, 'X'),
                    (11, 5, 2, '1010', 'D', 11111, 111110, 0, 0, 'B', 'X'),
                    (12, 5, 2, '1010', 'D', null, null, 0, 0, 'M', 'X'), -- this should not load because of 0/null values
                    (13, 5, 2, '1010', 'D', 0, 0, null, 0, 'M', 'X'), -- this should not load because of 0/null values
                    (14, 5, 2, '1010', 'D', null, 0, null, 0, 'm', 'X'), -- this should not load because of 0/null values
                    (15, 5, 2, '1010', 'D', 0, null, 0, null, 'M', 'X'), -- this should not load because of 0/null values
                    (16, 6, 2, '1010', 'D', 12121, 121210, -12, -121, 'L', 'X'),
                    (17, 7, 2, '1010', 'D', 13131, 131310, -13, -131, 'n', 'X'),
                    (18, 5, 2, '1010', 'D', 0, 0, 0, -1010, 'N', 'X')
                )
                """
            )

            # This is an extremely brute force tactic, but there are many non-nullable fields in USAspending
            # that are nullable in Broker.  To keep from throwing not-null errors, we are going to provide
            # zero values for a whole mess of fields known to be numeric.  This will also prevent me having
            # to mock a whole mess of additional data.
            cursor.execute(
                """
                    select  table_name, column_name
                    from    information_schema.columns
                    where   table_schema = 'public' and
                            table_name in (
                                'published_appropriation',
                                'published_object_class_program_activity',
                                'published_award_financial'
                            ) and
                            (column_name like '%cpe' or column_name like '%fyb')
                """
            )
            sqls = " ".join([f"update {r[0]} set {r[1]} = 0 where {r[1]} is null;" for r in cursor.fetchall()])
            cursor.execute(sqls)

    @staticmethod
    def _nuke_broker_data():
        """
        For reasons unbeknownst to me, I am having a very hard time getting TransactionTestCase to roll
        back Broker changes.  I spent entirely too much time trying to figure out a more graceful
        way, sooooo, brute force it is.
        """
        connection = connections[settings.DATA_BROKER_DB_ALIAS]
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    truncate table certify_history restart identity cascade;
                    truncate table publish_history restart identity cascade;
                    truncate table published_appropriation restart identity cascade;
                    truncate table published_object_class_program_activity restart identity cascade;
                    truncate table published_award_financial restart identity cascade;
                    truncate table tas_lookup restart identity cascade;
                    truncate table submission restart identity cascade;
                """
            )

    def tearDown(self):
        self._nuke_broker_data()

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_all_the_things(self):
        """
        Because we are using TransactionTestCase we're going to run all of our tests in one method to
        prevent repeated set ups and tear downs which are expensive.  This is less than ideal, but we'll
        probably be fine.
        """

        # Cue firey explosions.
        with self.assertRaises(CommandError):
            call_command("load_multiple_submissions")
        with self.assertRaises(CommandError):
            call_command("load_multiple_submissions", "--report-queue-status-only", "--submission_ids")
        with self.assertRaises(CommandError):
            call_command("load_multiple_submissions", "--submission_ids", "--incremental")

        # Load specific submissions.
        call_command("load_multiple_submissions", "--submission-ids", 1, 2, 3)
        assert SubmissionAttributes.objects.count() == 3
        assert AppropriationAccountBalances.objects.count() == 3
        assert FinancialAccountsByProgramActivityObjectClass.objects.count() == 7
        assert FinancialAccountsByAwards.objects.count() == 9

        # We'll need these later.
        update_date_sub_2 = SubmissionAttributes.objects.get(submission_id=2).update_date
        create_date_sub_3 = SubmissionAttributes.objects.get(submission_id=3).create_date

        # Load remaining submissions.
        call_command("load_multiple_submissions", "--incremental")
        assert SubmissionAttributes.objects.count() == 5
        assert AppropriationAccountBalances.objects.count() == 5
        assert FinancialAccountsByProgramActivityObjectClass.objects.count() == 9
        assert FinancialAccountsByAwards.objects.count() == 12

        # Now that we have everything loaded, let's make sure our data make sense.
        with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            fields = [
                "submission_id",
                "certified_date",
                "toptier_code",
                "reporting_period_start",
                "reporting_period_end",
                "reporting_fiscal_year",
                "reporting_fiscal_quarter",
                "reporting_fiscal_period",
                "quarter_format_flag",
                "reporting_agency_name",
                "is_final_balances_for_fy",
                "published_date",
                "submission_window_id",
                "history::JSON",
            ]
            cursor.execute(f"select {', '.join(fields)} from submission_attributes where submission_id = 1")
            d = dict(ordered_dictionary_fetcher(cursor)[0])
            assert d == {
                "submission_id": 1,
                "certified_date": datetime(2000, 2, 1, 0, 0, tzinfo=timezone.utc),
                "toptier_code": "001",
                "reporting_period_start": date(2000, 1, 1),
                "reporting_period_end": date(2000, 3, 31),
                "reporting_fiscal_year": 2000,
                "reporting_fiscal_quarter": 2,
                "reporting_fiscal_period": 4,
                "quarter_format_flag": True,
                "reporting_agency_name": None,
                "is_final_balances_for_fy": False,
                "published_date": datetime(2000, 1, 1, 0, 0, tzinfo=timezone.utc),
                "submission_window_id": 2000041,
                "history": [
                    {"certified_date": None, "published_date": "1999-01-01T00:00:00+00:00"},
                    {"certified_date": "2000-02-01T00:00:00+00:00", "published_date": "2000-01-01T00:00:00+00:00"},
                ],
            }

            cursor.execute(
                """
                    select  sum(total_budgetary_resources_amount_cpe)
                    from    appropriation_account_balances
                """
            )
            assert cursor.fetchone()[0] == Decimal("165.00")

            cursor.execute(
                """
                    select  sum(gross_outlay_amount_by_program_object_class_cpe),
                            string_agg(disaster_emergency_fund_code, ',' order by disaster_emergency_fund_code),
                            string_agg(prior_year_adjustment, ',' order by prior_year_adjustment)
                    from    financial_accounts_by_program_activity_object_class
                """
            )
            assert cursor.fetchone() == (Decimal("-52116.00"), "B,B,L,L", "B,B,P,P,X,X,X,X,X")

            cursor.execute(
                """
                    select  sum(gross_outlay_amount_by_award_cpe),
                            sum(transaction_obligated_amount),
                            sum(ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe),
                            sum(ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe),
                            string_agg(disaster_emergency_fund_code, ',' order by disaster_emergency_fund_code),
                            string_agg(prior_year_adjustment, ',' order by prior_year_adjustment)
                    from    financial_accounts_by_awards
                """
            )
            assert cursor.fetchone() == (
                Decimal("-521207.00"),
                Decimal("-5212070.00"),
                Decimal("505.00"),
                Decimal("6106.00"),
                "B,B,L,L,L,L,N",
                "B,B,P,P,X,X,X,X,X,X,X,X",
            )

        # Nuke a submission.
        SubmissionAttributes.objects.filter(submission_id=1).delete()
        assert SubmissionAttributes.objects.count() == 4
        assert AppropriationAccountBalances.objects.count() == 4
        assert FinancialAccountsByProgramActivityObjectClass.objects.count() == 6
        assert FinancialAccountsByAwards.objects.count() == 9

        # Make sure it reloads.
        call_command("load_multiple_submissions", "--incremental")
        assert SubmissionAttributes.objects.count() == 5
        assert AppropriationAccountBalances.objects.count() == 5
        assert FinancialAccountsByProgramActivityObjectClass.objects.count() == 9
        assert FinancialAccountsByAwards.objects.count() == 12

        # Make a change to a submission.
        SubmissionAttributes.objects.filter(submission_id=1).update(reporting_fiscal_year=1999)
        assert SubmissionAttributes.objects.get(submission_id=1).reporting_fiscal_year == 1999

        # Make sure it reloads.
        call_command("load_multiple_submissions", "--incremental")
        assert SubmissionAttributes.objects.get(submission_id=1).reporting_fiscal_year == 2000

        # Nuke a submission.
        SubmissionAttributes.objects.filter(submission_id=1).delete()

        # Make it really old.
        with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
            cursor.execute("update submission set updated_at = '1999-01-01' where submission_id = 1")

        # Make sure it DOESN'T reload.
        call_command("load_multiple_submissions", "--incremental")
        assert SubmissionAttributes.objects.count() == 4
        assert AppropriationAccountBalances.objects.count() == 4
        assert FinancialAccountsByProgramActivityObjectClass.objects.count() == 6
        assert FinancialAccountsByAwards.objects.count() == 9

        # Ok, after all the stuff we just did, let's make sure submissions 2 and 3 never got touched.
        assert SubmissionAttributes.objects.get(submission_id=2).update_date == update_date_sub_2
        assert SubmissionAttributes.objects.get(submission_id=3).create_date == create_date_sub_3

        # Now let's make sure submission 2 gets touched.
        call_command("load_multiple_submissions", "--submission-ids", 2)
        assert SubmissionAttributes.objects.get(submission_id=2).update_date > update_date_sub_2

        # Let's test the new certified_date change detection code.  But first, bring submission 1 back to the present.
        with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
            cursor.execute("update submission set updated_at = now() where submission_id = 1")
        call_command("load_multiple_submissions", "--submission-ids", 1)

        # Confirm that submission 3 only received a certified_date change, not a reload.
        assert SubmissionAttributes.objects.get(submission_id=3).create_date == create_date_sub_3

        # Ok.  That's probably good enough for now.  Thanks for bearing with me.
