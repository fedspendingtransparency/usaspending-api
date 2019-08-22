from usaspending_api.common.helpers.generic_helper import fy

def calculate_fiscal_year(broker_input):
    return fy(broker_input["action_date"])
