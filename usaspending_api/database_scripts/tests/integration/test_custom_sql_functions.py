import pytest
import urllib

from django.db import connection
from model_bakery import baker


URLENCODE_FUNCTION_NAME = "urlencode"


@pytest.fixture()
def add_fun_awards(db):
    generated_unique_award_ids = [
        "CONT_IDV_ABCDEFG_0123456",
        "CONT_IDV_abcdefg_9876543",
        "CONT_AWD_._.._..._....",
        "CONT_AWD_-_--_---_----",
        "ASST_AGG_1008DRCATTHP  01^~@01906470531201403_7022",
        "ASST_AGG_12C30000000000006122970000  121/21000_12C3",
        "ASST_AGG_17.302-MARYLAND-PRINCE GEORGE'S-20081231-10_1635",
        "ASST_NON_30180J015 MOD#2_1448",
        "ASST_NON_5% RECAP_8630",
        "CONT_AWD_GS30FY0027QP0019405Â_4732_GS30FY0027_4732",
        "ASST_NON_R!D1102A37    10_12E2",
        "CONT_IDV_[_]_test",
        "CONT_IDV_(_)_test",
        "CONT_AWD_(())_[[]]_test",
        "CONT_AWD_==_++_test",
        "CONT_AWD_?_??_test",
        "CONT_AWD_^_^^_^^^",
        "CONT_AWD_::_;;_:::;;;",
        "CONT_AWD_,_,,_,,,",
        "CONT_AWD_$_$$_$$$",
        "CONT_AWD_%_%%_%%%%",
        "☰☱☳☲☶☴ ൠൠൠ ☴☶☲☳☱☰",
        "❋❋❋ ALL YOUR BASE ARE BELONG TO US ❋❋❋",
        "⎺╲_❪ツ❫_╱⎺",
        "питон е јазик на компјутер и змија",
        "如果科羅拉多被鋪平會比得克薩斯州大",
        "епстеин се није убио",
        "kjo frazë nuk bën mirëkuptim",
        "何者なにものかによって、爆発物ばくはつぶつが仕掛しかけられたようです。",
    ]
    for id_, generated_unique_award_id in enumerate(generated_unique_award_ids):
        baker.make("search.AwardSearch", award_id=id_, generated_unique_award_id=generated_unique_award_id)


@pytest.mark.django_db
def test_urlencoding_no_change(add_fun_awards):
    test_sql = f"""
        SELECT      generated_unique_award_id,
                    {URLENCODE_FUNCTION_NAME}(generated_unique_award_id)
        from        vw_awards
        order by    id
    """
    with connection.cursor() as cursor:
        cursor.execute(test_sql)
        results = cursor.fetchall()

    for i in range(4):
        assert results[i][0] == results[i][1], "Safe ASCII characters were incorrectly modified!"


@pytest.mark.django_db
def test_urlencoding_with_urllib(add_fun_awards):
    test_sql = f"SELECT generated_unique_award_id, {URLENCODE_FUNCTION_NAME}(generated_unique_award_id) from vw_awards"
    with connection.cursor() as cursor:
        cursor.execute(test_sql)
        results = cursor.fetchall()

    for result in results:
        urlib_ver = urllib.parse.quote(result[0], safe="")
        msg = f"Custom SQL result '{result[1]}' doesn't match urllib function's '{urlib_ver}'"
        assert urlib_ver == result[1], msg


@pytest.mark.django_db
def test_reverse_urlencoding_with_urllib(add_fun_awards):
    test_sql = f"SELECT generated_unique_award_id, {URLENCODE_FUNCTION_NAME}(generated_unique_award_id) from vw_awards"
    with connection.cursor() as cursor:
        cursor.execute(test_sql)
        results = cursor.fetchall()

    for result in results:
        msg = f"Original '{result[0]}' doesn't match reverse quote '{urllib.parse.unquote(result[1])}'"
        assert urllib.parse.unquote(result[1]) == result[0], msg
