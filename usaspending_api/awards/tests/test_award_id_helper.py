from usaspending_api.awards.award_id_helper import AwardIdType, detect_award_id_type, MAX_INT


def test_acceptable_internal_award_ids():

    assert detect_award_id_type(12345) == (12345, AwardIdType.internal)
    assert detect_award_id_type(-12345) == (-12345, AwardIdType.internal)
    assert detect_award_id_type(+12345) == (12345, AwardIdType.internal)
    assert detect_award_id_type('12345') == (12345, AwardIdType.internal)
    assert detect_award_id_type('-12345') == (-12345, AwardIdType.internal)
    assert detect_award_id_type('+12345') == (12345, AwardIdType.internal)

    assert detect_award_id_type(0) == (0, AwardIdType.internal)
    assert detect_award_id_type(-0) == (0, AwardIdType.internal)
    assert detect_award_id_type(+0) == (0, AwardIdType.internal)
    assert detect_award_id_type('0') == (0, AwardIdType.internal)
    assert detect_award_id_type('-0') == (0, AwardIdType.internal)
    assert detect_award_id_type('+0') == (0, AwardIdType.internal)

    # A unicode Arabic-Indic zero.
    assert detect_award_id_type('\u0660') == (0, AwardIdType.internal)


def test_acceptable_generated_award_ids():

    assert detect_award_id_type('1a') == ('1A', AwardIdType.generated)
    assert detect_award_id_type('1.1') == ('1.1', AwardIdType.generated)
    assert detect_award_id_type('CONT_AW_4732_-NONE-_47QRAA18D0081_-NONE-') == \
        ('CONT_AW_4732_-NONE-_47QRAA18D0081_-NONE-', AwardIdType.generated)
    assert detect_award_id_type('') == ('', AwardIdType.generated)  # Unfortunately
    assert detect_award_id_type('   ') == ('   ', AwardIdType.generated)  # Also unfortunately

    # An integer that's too big for the database will be cast to a generated
    # type.  Is this right?  Who's to say.
    assert detect_award_id_type(MAX_INT + 1) == (str(MAX_INT + 1), AwardIdType.generated)


def test_invalid_award_ids():

    assert detect_award_id_type(None) == (None, AwardIdType.unknown)
    assert detect_award_id_type(1.0) == (1.0, AwardIdType.unknown)
    assert detect_award_id_type([1, 2, 3]) == ([1, 2, 3], AwardIdType.unknown)
    assert detect_award_id_type(('1', '2', '3')) == (('1', '2', '3'), AwardIdType.unknown)
    assert detect_award_id_type({'a': 1, 'b': 2}) == ({'a': 1, 'b': 2}, AwardIdType.unknown)
