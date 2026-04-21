DIMENSIONS = {
    1: "Industry",
    2: "Type",
    3: "Size",
}

TAGS = {
    # Industry
    101: {"dimension_id": 1, "tag_name": "Government"},
    102: {"dimension_id": 1, "tag_name": "Finance"},
    103: {"dimension_id": 1, "tag_name": "Consulting"},
    104: {"dimension_id": 1, "tag_name": "Retail"},
    105: {"dimension_id": 1, "tag_name": "Technology"},
    106: {"dimension_id": 1, "tag_name": "Utilities"},
    107: {"dimension_id": 1, "tag_name": "Healthcare"},
    108: {"dimension_id": 1, "tag_name": "Education"},
    109: {"dimension_id": 1, "tag_name": "Other"},
    110: {"dimension_id": 1, "tag_name": "Manufacturing"},
    111: {"dimension_id": 1, "tag_name": "Construction"},
    112: {"dimension_id": 1, "tag_name": "Logistics & Supply Chain"},
    113: {"dimension_id": 1, "tag_name": "Agriculture & Farming"},
    115: {"dimension_id": 1, "tag_name": "Energy"},
    117: {"dimension_id": 1, "tag_name": "Media & Communications"},
    118: {"dimension_id": 1, "tag_name": "Hospitality & Tourism"},
    119: {"dimension_id": 1, "tag_name": "Professional Services"},
    120: {"dimension_id": 1, "tag_name": "Non-profit / NGO"},

    # Type
    201: {"dimension_id": 2, "tag_name": "Public Sector"},
    202: {"dimension_id": 2, "tag_name": "Private Sector"},
    203: {"dimension_id": 2, "tag_name": "Non-profit"},
    204: {"dimension_id": 2, "tag_name": "Recruiter"},
    205: {"dimension_id": 2, "tag_name": "Other"},

    # Size
    301: {"dimension_id": 3, "tag_name": "Enterprise (1000+)"},
    302: {"dimension_id": 3, "tag_name": "Mid-size (200-999)"},
    303: {"dimension_id": 3, "tag_name": "Small (50-199)"},
    304: {"dimension_id": 3, "tag_name": "SME (<50)"},
}

TAG_NAME_TO_ID = {v["tag_name"]: k for k, v in TAGS.items()}