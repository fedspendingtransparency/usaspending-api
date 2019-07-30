def all_filters():
    return {
        "keywords": ["test", "testing"],
        "time_period": [{"start_date": "2016-10-01", "end_date": "2017-09-30"}],
        "agencies": [
            {"type": "funding", "tier": "toptier", "name": "Office of Pizza"},
            {"type": "awarding", "tier": "subtier", "name": "Personal Pizza"},
        ],
        "legal_entities": [1, 2, 3],
        "recipient_scope": "domestic",
        "recipient_locations": [{"country": "XYZ"}, {"country": "USA"}, {"country": "ABC"}],
        "recipient_type_names": ["Small Business", "Alaskan Native Owned Business"],
        "place_of_performance_scope": "domestic",
        "place_of_performance_locations": [{"country": "USA"}, {"country": "PQR"}],
        "award_type_codes": ["A", "B"],
        "award_ids": ["D0G0EL1", "A2D9D0C", "3DAB3021"],
        "award_amounts": [
            {"lower_bound": 1000000.00, "upper_bound": 25000000.00},
            {"upper_bound": 1000000.00},
            {"lower_bound": 500000000.00},
        ],
        "program_numbers": ["10.553"],
        "naics_codes": ["336411"],
        "psc_codes": ["1510"],
        "contract_pricing_type_codes": ["SAMPLECODE_CPTC"],
        "set_aside_type_codes": ["SAMPLECODE123"],
        "extent_competed_type_codes": ["SAMPLECODE_ECTC"],
    }
