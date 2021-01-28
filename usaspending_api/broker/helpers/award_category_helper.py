def award_types(row):
    """
    "Award Type" for FPDS transactions
        if award <> IDV (`pulled_from` <> 'IDV'): use `contract_award_type`
        elif `idv_type` == B &`type_of_idc` is present: use "IDV_B_" + `type_of_idc`
        elif `idv_type` == B & ("case" for type_of_idc_description for specific IDC type): use IDV_B_*
        else use "IDV_" + `idv_type`

    "Award Type Description" for FPDS transactions
        if award <> IDV (`pulled_from` <> 'IDV'): use `contract_award_type_desc`
        elif `idv_type` == B & `type_of_idc_description` <> null/NAN: use `type_of_idc_description`
        elif `idv_type` == B: use "INDEFINITE DELIVERY CONTRACT"
        else: use `idv_type_description`
    """
    pulled_from = row.get("pulled_from", None)
    idv_type = row.get("idv_type", None)
    type_of_idc = row.get("type_of_idc", None)
    type_of_idc_description = row.get("type_of_idc_description", None)

    if pulled_from != "IDV":
        award_type = row.get("contract_award_type")
    elif idv_type == "B" and type_of_idc is not None:
        award_type = "IDV_B_{}".format(type_of_idc)
    elif idv_type == "B" and type_of_idc_description == "INDEFINITE DELIVERY / REQUIREMENTS":
        award_type = "IDV_B_A"
    elif idv_type == "B" and type_of_idc_description == "INDEFINITE DELIVERY / INDEFINITE QUANTITY":
        award_type = "IDV_B_B"
    elif idv_type == "B" and type_of_idc_description == "INDEFINITE DELIVERY / DEFINITE QUANTITY":
        award_type = "IDV_B_C"
    else:
        award_type = "IDV_{}".format(idv_type)

    if pulled_from != "IDV":
        award_type_desc = row.get("contract_award_type_desc")
    elif idv_type == "B" and type_of_idc_description not in (None, "NAN"):
        award_type_desc = type_of_idc_description
    elif idv_type == "B":
        award_type_desc = "INDEFINITE DELIVERY CONTRACT"
    else:
        award_type_desc = row.get("idv_type_description")

    return award_type, award_type_desc
