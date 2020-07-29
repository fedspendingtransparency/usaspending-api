from usaspending_api.references.models import DisasterEmergencyFundCode


DISASTER_EMERGENCY_FUND_CODES = None


def get_disaster_emergency_fund(row):
    global DISASTER_EMERGENCY_FUND_CODES
    if DISASTER_EMERGENCY_FUND_CODES is None:
        DISASTER_EMERGENCY_FUND_CODES = {defc.code: defc for defc in DisasterEmergencyFundCode.objects.all()}
    disaster_emergency_fund_code = row["disaster_emergency_fund_code"]
    if not disaster_emergency_fund_code:
        return None
    try:
        return DISASTER_EMERGENCY_FUND_CODES[disaster_emergency_fund_code]
    except KeyError:
        raise DisasterEmergencyFundCode.DoesNotExist(
            f"Unable to find disaster emergency fund code for '{disaster_emergency_fund_code}'."
        )
