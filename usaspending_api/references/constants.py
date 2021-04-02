WEBSITE_AWARD_BINS = {
    "<1M": {"lower": None, "upper": 1000000, "enums": ["<1M", "1M"]},
    "1M": {"lower": 1000000, "upper": 1000000, "enums": ["1M"]},
    "1M..25M": {"lower": 1000000, "upper": 25000000, "enums": ["1M", "1M..25M", "25M"]},
    "25M": {"lower": 25000000, "upper": 25000000, "enums": ["25M"]},
    "25M..100M": {"lower": 25000000, "upper": 100000000, "enums": ["25M", "25M..100M", "100M"]},
    "100M": {"lower": 100000000, "upper": 100000000, "enums": ["100M"]},
    "100M..500M": {"lower": 100000000, "upper": 500000000, "enums": ["100M", "100M..500M", "500M"]},
    "500M": {"lower": 500000000, "upper": 500000000, "enums": ["500M"]},
    ">500M": {"lower": 500000000, "upper": None, "enums": ["500M", ">500M"]},
}
