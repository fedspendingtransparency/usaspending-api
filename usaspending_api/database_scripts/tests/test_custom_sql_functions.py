import pytest
import urllib

from django.db import connection
from model_mommy import mommy


URLENCODE_FUNCTION_NAME = "urlencode"


@pytest.fixture()
def add_fun_awards(db):
    mommy.make("awards.award", generated_unique_award_id="CONT_IDV_ABCDEFG_0123456")
    mommy.make("awards.award", generated_unique_award_id="CONT_IDV_abcdefg_9876543")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_._.._..._....")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_-_--_---_----")
    mommy.make("awards.award", generated_unique_award_id="ASST_AGG_1008DRCATTHP  01^~@01906470531201403_7022")
    mommy.make("awards.award", generated_unique_award_id="ASST_AGG_12C30000000000006122970000  121/21000_12C3")
    mommy.make("awards.award", generated_unique_award_id="ASST_AGG_17.302-MARYLAND-PRINCE GEORGE'S-20081231-10_1635")
    mommy.make("awards.award", generated_unique_award_id="ASST_NON_30180J015 MOD#2_1448")
    mommy.make("awards.award", generated_unique_award_id="ASST_NON_5% RECAP_8630")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_GS30FY0027QP0019405Â_4732_GS30FY0027_4732")
    mommy.make("awards.award", generated_unique_award_id="ASST_NON_R!D1102A37    10_12E2")
    mommy.make("awards.award", generated_unique_award_id="CONT_IDV_[_]_test")
    mommy.make("awards.award", generated_unique_award_id="CONT_IDV_(_)_test")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_(())_[[]]_test")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_==_++_test")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_?_??_test")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_^_^^_^^^")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_::_;;_:::;;;")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_,_,,_,,,")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_$_$$_$$$")
    mommy.make("awards.award", generated_unique_award_id="CONT_AWD_%_%%_%%%%")
    mommy.make("awards.award", generated_unique_award_id="☰☱☳☲☶☴ ൠൠൠ ☴☶☲☳☱☰")
    mommy.make("awards.award", generated_unique_award_id="❋❋❋ ALL YOUR BASE ARE BELONG TO US ❋❋❋")
    mommy.make("awards.award", generated_unique_award_id="⎺╲_❪ツ❫_╱⎺")
    mommy.make("awards.award", generated_unique_award_id="питон е јазик на компјутер и змија")
    mommy.make("awards.award", generated_unique_award_id="如果科羅拉多被鋪平會比得克薩斯州大")
    mommy.make("awards.award", generated_unique_award_id="епстеин се није убио")
    mommy.make("awards.award", generated_unique_award_id="kjo frazë nuk bën mirëkuptim")
    mommy.make("awards.award", generated_unique_award_id="何者なにものかによって、爆発物ばくはつぶつが仕掛しかけられたようです。")


def test_urlencoding_no_change(add_fun_awards):
    test_sql = f"SELECT generated_unique_award_id, {URLENCODE_FUNCTION_NAME}(generated_unique_award_id) from awards"
    with connection.cursor() as cursor:
        cursor.execute(test_sql)
        results = cursor.fetchall()

    for i in range(4):
        print(results[i][0], results[i][1])

    for i in range(4):
        assert results[i][0] == results[i][1], "Safe ASCII characters were incorrectly modified!"


def test_urlencoding_with_urllib(add_fun_awards):
    test_sql = f"SELECT generated_unique_award_id, {URLENCODE_FUNCTION_NAME}(generated_unique_award_id) from awards"
    with connection.cursor() as cursor:
        cursor.execute(test_sql)
        results = cursor.fetchall()

    for result in results:
        urlib_ver = urllib.parse.quote(result[0], safe="")
        msg = f"Custom SQL result '{result[1]}' doesn't match urllib function's '{urlib_ver}'"
        assert urlib_ver == result[1], msg


def test_reverse_urlencoding_with_urllib(add_fun_awards):
    test_sql = f"SELECT generated_unique_award_id, {URLENCODE_FUNCTION_NAME}(generated_unique_award_id) from awards"
    with connection.cursor() as cursor:
        cursor.execute(test_sql)
        results = cursor.fetchall()

    for result in results:
        msg = f"Original '{result[0]}' doesn't match reverse quote '{urllib.parse.unquote(result[1])}'"
        assert urllib.parse.unquote(result[1]) == result[0], msg
