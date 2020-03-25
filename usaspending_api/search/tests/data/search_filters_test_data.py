def non_legacy_filters():
    return {
        "agencies": [
            {"type": "funding", "tier": "toptier", "name": "Office of Pizza"},
            {"type": "awarding", "tier": "subtier", "name": "Personal Pizza"},
        ],
        "award_amounts": [
            {"lower_bound": 1000000.00, "upper_bound": 25000000.00},
            {"upper_bound": 1000000.00},
            {"lower_bound": 500000000.00},
        ],
        "award_ids": ["D0G0EL1", "A2D9D0C", "3DAB3021"],
        "award_type_codes": ["A", "B"],
        "contract_pricing_type_codes": ["SAMPLECODE_CPTC"],
        "extent_competed_type_codes": ["SAMPLECODE_ECTC"],
        "keywords": ["test", "testing"],
        "legal_entities": [1, 2, 3],
        "naics_codes": {"require": [336411]},
        "place_of_performance_locations": [{"country": "USA"}, {"country": "PQR"}],
        "place_of_performance_scope": "domestic",
        "program_numbers": ["10.553"],
        "psc_codes": ["1510"],
        "recipient_locations": [{"country": "XYZ"}, {"country": "USA"}, {"country": "ABC"}],
        "recipient_scope": "domestic",
        "recipient_type_names": ["Small Business", "Alaskan Native Corporation Owned Firm"],
        "set_aside_type_codes": ["SAMPLECODE123"],
        "time_period": [{"start_date": "2016-10-01", "end_date": "2017-09-30"}],
        "tas_codes": [
            {"ata": "012", "aid": "016", "bpoa": "2013", "epoa": "2016", "main": "0181", "sub": "000"},
            {"ata": "012", "aid": "069", "a": "X", "main": "0500", "sub": "011"},
        ],
    }


def legacy_filters():
    return {"award_type_codes": ["A", "B"], "object_class": ["111"], "program_activity": [222]}
