from usaspending_api.llm.models.py_models import AITool, AIToolDescription

DEFC_CODES: dict[str, list[dict[str, str]]] = {
    "COVID-19": [
        {"code": "L", "label": "Coronavirus Preparedness"},
        {"code": "M", "label": "Families First Act"},
        {"code": "N", "label": "CARES Act"},
        {"code": "O", "label": "Non-emergency COVID"},
        {"code": "P", "label": "Paycheck Protection Program"},
        {"code": "U", "label": "Consolidated Appropriations 2021 COVID"},
        {"code": "V", "label": "American Rescue Plan"},
    ],
    "Infrastructure (IIJA)": [
        {"code": "Z", "label": "Emergency IIJA funding"},
        {"code": "1", "label": "Non-emergency IIJA funding"},
    ],
    "Ukraine Aid": [
        {"code": "6", "label": "Additional Ukraine Supplemental"},
        {"code": "AAA", "label": "Ukraine Continuing Appropriations"},
    ],
    "2017-2020 Natural Disasters": [
        {"code": "A", "label": "Hurricanes Harvey, Irma, Maria relief (2017)"},
        {"code": "B", "label": "Additional disaster relief (2017)"},
        {"code": "C", "label": "Bipartisan Budget Act 2018"},
        {"code": "D", "label": "FAA Reauthorization 2018"},
        {"code": "E", "label": "Additional Disaster Relief 2019"},
        {"code": "F", "label": "Southern Border Assistance 2019"},
        {"code": "G", "label": "Emergency Consolidated Appropriations 2020"},
        {"code": "H", "label": "Disaster Consolidated Appropriations 2020"},
        {"code": "I", "label": "Further Consolidated Appropriations 2020"},
        {"code": "J", "label": "Wildfire Suppression 2020"},
        {"code": "K", "label": "USMCA Implementation"},
    ],
    "2021-2024 Appropriations": [
        {"code": "R", "label": "Consolidated Appropriations 2021 (non-COVID)"},
        {"code": "S", "label": "Consolidated Appropriations 2021 (non-COVID)"},
        {"code": "T", "label": "Consolidated Appropriations 2021 (non-COVID)"},
        {"code": "W", "label": "Emergency Security Supplemental 2021"},
        {"code": "X", "label": "Emergency Extending Government Funding 2021"},
        {"code": "Y", "label": "Disaster Extending Government Funding 2021"},
        {"code": "2", "label": "Further Extending Government Funding 2021"},
        {"code": "3", "label": "Emergency Consolidated Appropriations 2022"},
        {"code": "4", "label": "Disaster Consolidated Appropriations 2022"},
        {"code": "5", "label": "Wildfire Suppression 2022"},
        {"code": "7", "label": "Bipartisan Safer Communities Act 2022"},
        {"code": "8", "label": "Legislative Branch Appropriations 2022"},
        {"code": "AAB", "label": "Emergency Consolidated Appropriations 2023"},
        {"code": "AAC", "label": "Wildfire Suppression 2023"},
        {"code": "AAD", "label": "Disaster Consolidated Appropriations 2023"},
        {"code": "AAE", "label": "Continuing Appropriations 2024"},
        {"code": "AAF", "label": "Emergency Consolidated Appropriations 2024"},
        {"code": "AAG", "label": "Disaster Consolidated Appropriations 2024"},
        {"code": "AAH", "label": "Emergency Appropriations P.L. 118-47 (2024)"},
        {"code": "AAI", "label": "Disaster Appropriations P.L. 118-47 (2024)"},
        {"code": "AAJ", "label": "Emergency Appropriations P.L. 118-50 (2024)"},
    ],
    "Special / Other": [
        {"code": "Q", "label": "Not Designated (non-emergency/non-disaster)"},
        {"code": "9", "label": "Unspecified non-COVID (discontinued Jul 2021)"},
        {"code": "QQQ", "label": "Excluded from tracking"},
    ],
}


def list_defc_codes() -> dict[str, list[dict[str, str]]]:
    return DEFC_CODES


list_defc_codes_tool = AITool(
    function=list_defc_codes,
    logging=lambda _: "Listing DEFC codes.",
    description=AIToolDescription(
        name="list_defc_codes",
        description=(
            "List every valid Disaster/Emergency Fund Code (DEFC), grouped by the appropriations event "
            "users refer to (COVID-19, Infrastructure (IIJA), Ukraine Aid, 2017-2020 Natural Disasters, "
            "2021-2024 Appropriations, Special / Other). Takes no input. Each entry has a 'code' (the "
            "exact string to put in the execute_filter 'defCodes' require/exclude lists) and a short "
            "'label'. Call this before setting 'defCodes' to choose the correct code(s) — e.g. all of "
            "COVID-19 for comprehensive pandemic spending."
        ),
        input_schema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
)
